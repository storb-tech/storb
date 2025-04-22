use std::error::Error as StdError;
use std::net::Ipv4Addr;

use crabtensor::api::runtime_apis::neuron_info_runtime_api::NeuronInfoRuntimeApi;
use crabtensor::api::runtime_types::pallet_subtensor::rpc_info::neuron_info::NeuronInfoLite;
use crabtensor::AccountId;
use futures::stream::{self, StreamExt};
use thiserror::Error;
use tracing::{debug, error, info, warn};

use crate::constants::{CLIENT_TIMEOUT, SYNC_BUFFER_SIZE};
use crate::{BaseNeuron, LocalNodeInfo, NodeInfo};

#[derive(Debug, Error)]
pub enum SyncError {
    #[error("Failed to get node info: {0}")]
    GetNodeInfoError(String),
}
pub trait Synchronizable {
    fn sync_metagraph(
        &mut self,
    ) -> impl std::future::Future<Output = Result<Vec<u16>, Box<dyn StdError + Send + Sync>>> + Send;
    fn get_remote_node_info(
        addr: reqwest::Url,
    ) -> impl std::future::Future<Output = Result<LocalNodeInfo, SyncError>> + Send;
}

// Implementation for BaseNeuron
impl Synchronizable for BaseNeuron {
    async fn get_remote_node_info(addr: reqwest::Url) -> Result<LocalNodeInfo, SyncError> {
        debug!("Requesting node info from: {}", addr.to_string());
        let req_client = reqwest::Client::builder()
            .timeout(CLIENT_TIMEOUT)
            .build()
            .map_err(|e| {
                error!("Failed to build HTTP client: {:?}", e);
                SyncError::GetNodeInfoError(format!("Could not build HTTP client: {e}"))
            })?;

        let response = tokio::time::timeout(CLIENT_TIMEOUT, req_client.get(addr.clone()).send())
            .await
            .map_err(|e| {
                error!("Request timed out for {}: {:?}", addr, e);
                SyncError::GetNodeInfoError(format!("Request timed out: {e}"))
            })?
            .map_err(|e| {
                error!("Failed to send request to {}: {:?}", addr, e);
                SyncError::GetNodeInfoError(format!("Could not send request: {:?}", e))
            })?;

        // Read the response bytes.
        let data = response.bytes().await.map_err(|e| {
            error!("Failed to read response bytes: {:?}", e);
            SyncError::GetNodeInfoError(format!("Could not read response bytes: {:?}", e))
        })?;

        debug!("get_remote_node_info - raw data: {:?}", data);

        // Deserialize the response using bincode.
        let local_node_info: LocalNodeInfo =
            bincode::deserialize::<LocalNodeInfo>(&data).map_err(|e| {
                error!("Failed to deserialize LocalNodeInfo: {:?}", e);
                SyncError::GetNodeInfoError(format!("Failed to deserialize LocalNodeInfo: {:?}", e))
            })?;

        Ok(local_node_info)
    }

    /// Synchronise the local metagraph state with chain.
    async fn sync_metagraph(
        &mut self,
    ) -> Result<Vec<u16>, Box<dyn StdError + Send + Sync + 'static>> {
        info!("Starting sync_metagraph");
        let start = std::time::Instant::now();

        info!("Getting latest block...");
        let current_block = self
            .subtensor
            .blocks()
            .at_latest()
            .await
            .map_err(|e| format!("Failed to get latest block: {:?}", e))?;
        info!("Got latest block in {:?}", start.elapsed());

        info!("Getting runtime API...");
        let runtime_api = self.subtensor.runtime_api().at(current_block.reference());
        let mut local_node_info = self.local_node_info.clone();
        // TODO: error out if cant access chain when calling above functions

        // TODO: is there a nicer way to pass self for NeuronInfoRuntimeApi?
        let neurons_payload =
            NeuronInfoRuntimeApi::get_neurons_lite(&NeuronInfoRuntimeApi {}, self.config.netuid);

        // TODO: error out if cant access chain when calling above function
        let neurons: Vec<NeuronInfoLite<AccountId>> = runtime_api.call(neurons_payload).await?;

        let original_neurons = self.neurons.read().await.clone();

        info!("Got neurons from metagraph");

        // Check if neurons have changed or not
        let mut changed_neurons: Vec<u16> = neurons.iter().map(|neuron| neuron.uid).collect();

        if original_neurons.len() < neurons.len() {
            changed_neurons = neurons
                .iter()
                .zip(original_neurons.iter())
                .filter(|(curr, og)| curr.hotkey != og.hotkey)
                .map(|(curr, _)| curr.uid)
                .collect();

            let mut new_neurons: Vec<u16> = neurons[original_neurons.len()..]
                .iter()
                .map(|neuron| neuron.uid)
                .collect();

            changed_neurons.append(&mut new_neurons);

            let mut neurons_guard = self.neurons.write().await;
            *neurons_guard = neurons.clone();
        } else if !original_neurons.is_empty() {
            changed_neurons = neurons
                .iter()
                .zip(original_neurons.iter())
                .filter(|(curr, og)| curr.hotkey != og.hotkey)
                .map(|(curr, _)| curr.uid)
                .collect();

            let mut neurons_guard = self.neurons.write().await;
            *neurons_guard = neurons.clone();
        }

        drop(original_neurons);

        info!("Updated local neurons state");

        // After updating neurons state, save to disk
        info!("Saving neurons state to disk");
        Self::save_neurons_state_to_disk(&self.config.neurons_dir, &self.neurons.read().await)?;
        info!("Saved neurons state to disk");

        if local_node_info.uid.is_none() {
            // Find our local node uid or crash the program
            let my_neuron = Self::find_neuron_info(&neurons, self.signer.account_id())
                .expect("Local node not found in neuron list");
            local_node_info.uid = Some(my_neuron.uid);
        }

        self.local_node_info = local_node_info;

        let mut futures = Vec::new();

        // Create futures for all node info requests
        for (neuron_uid, _) in neurons.iter().enumerate() {
            let neuron_info: NeuronInfoLite<AccountId> = neurons[neuron_uid].clone();

            // Skip self to avoid deadlock
            if neuron_uid == self.local_node_info.uid.ok_or("Node UID did not exist")? as usize {
                continue;
            }

            let neuron_ip = neuron_info.axon_info.ip;
            let neuron_port = neuron_info.axon_info.port;

            // Skip invalid IPs/ports
            if neuron_ip == 0 {
                warn!(
                    "Invalid IP for neuron {:?}. Node has never been started",
                    neuron_info.uid
                );
                continue;
            }

            if neuron_port == 0 {
                error!("Invalid port for neuron: {:?}", neuron_info.uid);
                continue;
            }

            let ip = Ipv4Addr::from((neuron_ip & 0xffff_ffff) as u32);
            let url_raw = format!("http://{}:{}/info", ip, neuron_port);
            let url = reqwest::Url::parse(&url_raw).map_err(|e| {
                error!("Failed to parse URL: {:?}", e);
                SyncError::GetNodeInfoError(format!("Failed to parse URL: {:?}", e))
            })?;

            // Create future for this node's info request
            let future = async move {
                match Self::get_remote_node_info(url).await {
                    Ok(remote_node_info) => Some((neuron_info, remote_node_info)),
                    Err(err) => {
                        warn!(
                            "Failed to get remote node info (it may be offline): {:?}",
                            err
                        );
                        None
                    }
                }
            };

            futures.push(future);
        }

        // Execute all futures concurrently and collect results
        let stream = stream::iter(futures)
            .buffer_unordered(SYNC_BUFFER_SIZE)
            .collect::<Vec<_>>();
        let results = stream.await;

        // Process results and update address book
        for result in results.into_iter().flatten() {
            let (neuron_info, remote_node_info) = result;

            let node_info = NodeInfo {
                neuron_info: neuron_info.clone(),
                peer_id: remote_node_info.peer_id,
                http_address: remote_node_info.http_address,
                quic_address: remote_node_info.quic_address,
                version: remote_node_info.version,
            };

            if let Some(peer_id) = remote_node_info.peer_id {
                self.address_book.clone().insert(peer_id, node_info);
                self.peer_node_uid.insert(peer_id, neuron_info.uid);
            }
        }

        info!("Completed sync_metagraph in {:?}", start.elapsed());
        info!("Done syncing metagraph");

        Ok(changed_neurons)
    }
}
