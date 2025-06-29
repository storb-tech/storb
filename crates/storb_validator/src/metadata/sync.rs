use std::sync::Arc;

use base::utils::multiaddr_to_socketaddr;
use libp2p::Multiaddr;
use thiserror::Error;
use tracing::{debug, info, warn};

use crate::{
    constants::TAO_IN_RAO,
    metadata::{db::MetadataDB, models::CrSqliteChanges},
    validator::Validator,
};

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Failed to get db changes from validator: {0}")]
    GetDBChangesError(String),
}

async fn get_crsql_changes(
    address: Multiaddr,
    site_id: Vec<u8>,
    min_db_version: u64,
) -> Result<Vec<CrSqliteChanges>, SyncError> {
    // // We need to specify the site_id to exclude our own changes
    // get ipv4 address and port from the multiaddr
    let socket_addr = multiaddr_to_socketaddr(&address)
        .ok_or_else(|| SyncError::GetDBChangesError("Invalid multiaddr".to_string()))?;
    let ip = socket_addr.ip();
    let port = socket_addr.port();
    let addr = format!("{}:{}", ip, port);

    let endpoint = format!(
        "http://{}/db_changes?site_id_exclude={}&min_db_version={}",
        addr,
        hex::encode(&site_id),
        min_db_version
    );
    let url = reqwest::Url::parse(&endpoint).map_err(|e| {
        warn!("Failed to parse URL: {:?}", e);
        SyncError::GetDBChangesError(format!("Invalid URL: {:?}", e))
    })?;
    let response = reqwest::get(url).await.map_err(|e| {
        warn!("Failed to get changes from {}: {:?}", addr, e);
        SyncError::GetDBChangesError(format!("Request failed: {:?}", e))
    })?;
    if !response.status().is_success() {
        warn!(
            "Failed to get changes from {}: {:?}",
            endpoint,
            response.status()
        );
        return Err(SyncError::GetDBChangesError(format!(
            "Request failed with status: {:?}",
            response.status()
        )));
    }

    // deserialize the response into Vec<CrSqliteChanges>
    let changes: Vec<CrSqliteChanges> =
        bincode::deserialize(&response.bytes().await.map_err(|e| {
            warn!("Failed to read response bytes: {:?}", e);
            SyncError::GetDBChangesError(format!("Could not read response bytes: {:?}", e))
        })?)
        .map_err(|e| {
            warn!("Failed to deserialize CrSqliteChanges: {:?}", e);
            SyncError::GetDBChangesError(format!("Failed to deserialize response: {:?}", e))
        })?;
    info!(
        "Successfully retrieved {} changes from {}",
        changes.len(),
        addr
    );
    Ok(changes)
}

pub async fn sync_metadata_db(validator: Arc<Validator>) -> Result<(), SyncError> {
    info!("Syncing MetadataDB...");
    let neuron = validator.neuron.read().await;

    let validator_uid = neuron
        .local_node_info
        .uid
        .ok_or_else(|| SyncError::GetDBChangesError("Local node UID is not set".to_string()))?;
    let neurons = neuron.neurons.read().await;
    let address_book = neuron.address_book.clone();
    let sync_stake_threshold = validator.config.sync_stake_threshold;
    // get the site id of our validator from the metadatadb
    let site_id = match MetadataDB::get_site_id(&validator.metadatadb_sender).await {
        Ok(id) => id,
        Err(e) => {
            warn!("Failed to get site ID: {:?}", e);
            return Err(SyncError::GetDBChangesError(format!(
                "Failed to get site ID: {:?}",
                e
            )));
        }
    };

    // get the db version of our validator
    let min_db_version = match MetadataDB::get_db_version(&validator.metadatadb_sender).await {
        Ok(version) => version,
        Err(e) => {
            warn!("Failed to get DB version: {:?}", e);
            return Err(SyncError::GetDBChangesError(format!(
                "Failed to get DB version: {:?}",
                e
            )));
        }
    };

    // Iterate through the neurons
    for neuron_info in neurons.iter() {
        // Skip self
        if neuron_info.uid == validator_uid {
            continue;
        }

        // Check to see if the neuron stake is above the stake streshold
        let stake = neuron_info
            .stake
            .iter()
            .map(|(_, stake)| stake.0)
            .sum::<u64>() as f64
            / TAO_IN_RAO;
        debug!(
            "Neuron {} has stake {}, threshold is {}",
            neuron_info.uid, stake, sync_stake_threshold
        );
        if stake < sync_stake_threshold as f64 {
            debug!("skipping neuron {}", neuron_info.uid);
            continue;
        }
        // If it does have enough stake, we can sync the metadata
        // We first get the http address for the neuron's api
        let node_info = match address_book.get(&neuron_info.uid) {
            Some(addr) => addr,
            None => {
                warn!("No node info found for neuron {}", neuron_info.uid);
                continue;
            }
        };

        let address = match &node_info.http_address {
            Some(addr) => addr.clone(),
            None => {
                warn!("No HTTP address found for neuron {}", neuron_info.uid);
                continue;
            }
        };

        // Use the address to ask the neuron for changes in its db from its /db_changes endpoint
        let db_changes = match get_crsql_changes(address, site_id.clone(), min_db_version).await {
            Ok(changes) => changes,
            Err(e) => {
                warn!(
                    "Failed to get changes for neuron {}: {:?}",
                    neuron_info.uid, e
                );
                continue;
            }
        };

        // Apply the changes to the local metadata database with MetadataDB::insert_crsqlite_changes
        if let Err(e) =
            MetadataDB::insert_crsqlite_changes(&validator.metadatadb_sender, db_changes).await
        {
            warn!(
                "Failed to insert changes for neuron {}: {:?}",
                neuron_info.uid, e
            );
        } else {
            info!(
                "Successfully synced metadata for neuron {}",
                neuron_info.uid
            );
        }
    }
    Ok(())
}
