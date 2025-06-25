use std::{collections::HashMap, sync::Arc};

use anyhow::Context;
use base::{constants::MIN_BANDWIDTH, NodeUID};
use quinn::Connection;
use tokio::sync::mpsc;
use tracing::{error, info};

use crate::{download, metadata, upload, utils::get_id_quic_uids, validator::Validator};

// Function which called to check for what pieces to repair from the MetadataDB
// and distributes the pieces to different miners
pub async fn repair_pieces(validator: Arc<Validator>) -> Result<(), Box<dyn std::error::Error>> {
    // query the database to get the pieces that need repair
    let metadatadb_sender = validator.clone().metadatadb_sender.clone();
    let signer = Arc::new(validator.neuron.read().await.signer.clone());
    let pieces_to_repair =
        match metadata::db::MetadataDB::get_pieces_for_repair(&metadatadb_sender.clone()).await {
            Ok(pieces) => pieces,
            Err(e) => {
                error!("Failed to get pieces for repair: {}", e);
                // convert the error into a Box<dyn std::error::Error>
                // and return it
                return Err(Box::new(e));
            }
        };

    if pieces_to_repair.is_empty() {
        info!("No pieces to repair.");
        return Ok(());
    }

    info!("Found {} pieces to repair.", pieces_to_repair.len());

    let vali_clone = validator.clone();
    let base_neuron_guard = vali_clone.neuron.read().await;
    let vali_uid = base_neuron_guard
        .local_node_info
        .uid
        .context("Failed to get UID for validator")?;
    drop(base_neuron_guard);

    // Download, and redistribute the pieces to miners
    for (piece_hash, miners) in pieces_to_repair {
        // get piece from metadatadb
        let piece =
            match metadata::db::MetadataDB::get_piece(&metadatadb_sender.clone(), piece_hash).await
            {
                Ok(piece) => piece,
                Err(e) => {
                    error!(
                        "Failed to get piece {} from MetadataDB: {}",
                        hex::encode(piece_hash),
                        e
                    );
                    continue;
                }
            };

        let piece_size: f64 = piece.piece_size as f64;
        let timeout_duration =
            std::time::Duration::from_secs_f64(piece_size / MIN_BANDWIDTH as f64);

        let req_client = match reqwest::Client::builder().timeout(timeout_duration).build() {
            Ok(client) => client,
            Err(e) => {
                error!("Failed to create HTTP client: {}", e);
                continue;
            }
        };

        // Try to download the piece from the miners
        // Once one of the miners responds, and it's
        // hash is valid, we can stop trying to download
        let mut download_piece_futures: Vec<tokio::task::JoinHandle<Result<(), anyhow::Error>>> =
            Vec::new();
        let (download_task_tx, mut download_task_rx) =
            mpsc::channel::<Result<Vec<u8>, anyhow::Error>>(miners.len());

        let validator_for_download = validator.clone();
        for miner in &miners {
            let miner_uid = *miner;
            // get node info for the miner
            let miner_node_info = match validator
                .clone()
                .neuron
                .read()
                .await
                .address_book
                .get(&miner_uid)
            {
                Some(node_info) => node_info.clone(),
                None => {
                    error!("Miner UID {} not found in address book.", miner_uid);
                    continue; // Skip this miner if not found
                }
            };

            let piece_hash_clone = piece_hash;
            let req_client_clone = req_client.clone();
            let download_task_tx_clone = download_task_tx.clone();
            let signer_clone = signer.clone();
            let validator_clone = validator_for_download.clone();

            let download_process_piece = tokio::spawn(async move {
                // download the piece with get_piece_from_miner and process if with process_piece_response
                // if the piece is valid, send it to the download_task_tx channel
                let scoring_system = validator_clone.scoring_system.clone();
                let download_response = match download::get_piece_from_miner(
                    req_client_clone,
                    &miner_node_info,
                    piece_hash_clone,
                    signer_clone.clone(),
                    vali_uid,
                    scoring_system.clone(),
                )
                .await
                {
                    Ok(response) => response,
                    Err(e) => {
                        error!(
                            "Failed to download piece {} from miner {}: {}",
                            hex::encode(piece_hash_clone),
                            miner_uid,
                            e
                        );
                        return Err(e);
                    }
                };

                match download::process_piece_response(
                    download_response,
                    piece_hash_clone,
                    miner_uid,
                )
                .await
                {
                    Ok(valid_piece) => {
                        // Send the valid piece to the channel
                        if let Err(e) = download_task_tx_clone.send(Ok(valid_piece)).await {
                            error!("Failed to send valid piece to channel: {}", e);
                        }
                        Ok(())
                    }
                    Err(e) => {
                        error!(
                            "Failed to process piece response from miner {}: {}",
                            miner_uid, e
                        );
                        Err(e)
                    }
                }
            });
            download_piece_futures.push(download_process_piece);
        }

        // Process the results of the downloads, we monitor the download_task_rx channel
        // and wait for the first valid piece to be received
        // If we receive a valid piece, we can store it in a variable,
        // and abort() the futures in download_piece_futures
        let mut valid_piece: Option<Vec<u8>> = None;
        for _ in 0..miners.len() {
            match download_task_rx.recv().await {
                Some(Ok(piece_data)) => {
                    valid_piece = Some(piece_data);
                    break; // We found a valid piece, we can stop waiting
                }
                Some(Err(e)) => {
                    error!("Error receiving piece data: {}", e);
                }
                None => {
                    error!("Download task channel closed unexpectedly.");
                    break;
                }
            }
        }

        // abort all download futures if we found a valid piece
        if valid_piece.is_some() {
            for download_future in download_piece_futures {
                download_future.abort();
            }
        }

        let valid_piece =
            valid_piece.ok_or_else(|| anyhow::anyhow!("No valid piece found after downloading"))?;

        // TODO(repair): exclude the miners that got deregistered?
        // Distribute the pieces to the top miners

        // First, we get the scores of the miners from the scoring system
        let scoring_system = validator.clone().scoring_system.clone();
        let miner_scores = scoring_system.read().await.state.ema_scores.clone();

        // The indices of miner_scores are the uids of the miners
        // We want to sort them by their scores, and *try* distribute the each piece with upload_piece_data
        // to n_i of them, where n_i=2 * m_i where m_i
        // is the number of miners that lost piece i
        // once we have successfully uploaded m_i pieces to m_i unique miners, we can stop
        // distributing the piece
        let mut sorted_miners: Vec<usize> = (0..miner_scores.len())
            .filter(|&i| miner_scores[i] > 0.0) // Filter out miners with zero scores
            .collect();
        sorted_miners.sort_by(|&a, &b| {
            miner_scores[b]
                .partial_cmp(&miner_scores[a])
                .unwrap_or(std::cmp::Ordering::Equal)
        });

        let num_miners_to_distribute = 2 * miners.len();
        let mut distributed_count = 0;

        // Get quic addresses and miner uids, and establish connections with the miners
        let (_, quic_addresses, miner_uids) = get_id_quic_uids(validator.clone()).await;

        upload::log_connection_attempt(&quic_addresses, &miner_uids);

        let miner_connections = upload::establish_and_validate_connections(quic_addresses)
            .await
            .context("Failed to establish connections with miners")?;

        // create hashmap of miner_uids->miner_connection.
        // each miner_uid coresponds to the miner connection in miner_connections
        let uid_to_connection: HashMap<NodeUID, Connection> = miner_uids
            .iter()
            .zip(miner_connections.iter())
            .map(|(&uid, conn)| (uid, conn.1.clone()))
            .collect();

        upload::log_connection_success(&miner_connections);

        // Distribute to miners, if there ar not enough miners, then we automatically loop back around the sorted_miners
        // TODP(repair): we should probably have a better way to handle this
        for &miner_uid in sorted_miners.iter().cycle().take(num_miners_to_distribute) {
            // Get the miner's node info
            let miner_node_info = match validator
                .clone()
                .neuron
                .read()
                .await
                .address_book
                .get(&(miner_uid as u16))
            {
                Some(node_info) => node_info.clone(),
                None => {
                    error!("Miner UID {} not found in address book.", miner_uid);
                    continue; // Skip this miner if not found
                }
            };

            let conn = uid_to_connection[&(miner_uid as u16)].clone();
            let valid_piece_clone = valid_piece.clone(); // TODO(repair): is there a better way to handle this instead of cloning the piece?

            // Upload the piece to the miner
            if let Err(e) = upload::upload_piece_data(
                validator.clone().neuron.clone(),
                miner_node_info,
                &conn,
                valid_piece_clone,
            )
            .await
            {
                error!(
                    "Failed to upload piece {} to miner {}: {}",
                    hex::encode(piece_hash),
                    miner_uid,
                    e
                );
                continue; // Skip this miner if upload fails
            }

            distributed_count += 1;
            if distributed_count >= miners.len() {
                break; // We have successfully distributed to enough miners
            }
        }
    }
    Ok(())
}
