use std::collections::HashSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use anyhow::{anyhow, bail, Context, Result};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use base::constants::MIN_BANDWIDTH;
use base::verification::{HandshakePayload, KeyRegistrationInfo, VerificationMessage};
use base::{AddressBook, BaseNeuron};
use crabtensor::sign::sign_message;
use futures::stream::FuturesUnordered;
use tokio::sync::{mpsc, Mutex, RwLock};
use tokio_stream::StreamExt;
use tracing::{debug, error, trace};

use crate::metadata;
use crate::metadata::db::MetadataDBCommand;
use crate::scoring::ScoringSystem;
use crate::ValidatorState;

const MPSC_BUFFER_SIZE: usize = 100;
// TODO: Put this in config or just base it on hardware threads
const THREAD_COUNT: usize = 10;

/// Processes download streams and retrieves file pieces from available miners.
pub(crate) struct DownloadProcessor {
    pub metadatadb_sender: mpsc::Sender<MetadataDBCommand>,
    pub state: Arc<ValidatorState>,
}

impl DownloadProcessor {
    /// Create a new instance of the DownloadProcessor.
    pub(crate) async fn new(state: &ValidatorState) -> Result<Self> {
        let metadatadb_sender = state.validator.metadatadb_sender.clone();
        Ok(Self {
            metadatadb_sender,
            state: Arc::new(state.clone()),
        })
    }

    /// Producer for pieces. Queries the miners to get piece data.
    async fn produce_piece(
        validator_base_neuron: Arc<RwLock<BaseNeuron>>,
        scoring_system: Arc<RwLock<ScoringSystem>>,
        local_address_book: AddressBook,
        piece_value: metadata::models::PieceValue,
        chunk_idx: u64,
        piece_idx: u64,
    ) -> Result<base::piece::Piece> {
        // Retrieve the piece entry.
        let piece_hash = piece_value.piece_hash;
        debug!("Piece hash: {}", hex::encode(piece_hash));

        let piece_miners = piece_value.miners;

        if piece_miners.is_empty() {
            bail!("No providers found for piece {}", hex::encode(piece_hash));
        }

        let mut requests = FuturesUnordered::new();

        let base_neuron_guard = validator_base_neuron.read().await;
        let signer = base_neuron_guard.signer.clone();

        let vali_uid = base_neuron_guard
            .local_node_info
            .uid
            .context("Failed to get UID for validator")?;
        drop(base_neuron_guard);

        let size: f64 = piece_value.piece_size as f64;
        let min_bandwidth = MIN_BANDWIDTH as f64;

        for miner_uid in piece_miners {
            // Look up node info from the address book.
            let node_info = local_address_book
                .clone()
                .get(&miner_uid.0)
                .ok_or_else(|| anyhow!("Miner {:?} not found in local address book", miner_uid))?
                .clone();

            let scoring_system_clone = scoring_system.clone();
            let db = scoring_system_clone.write().await.db.clone();
            drop(scoring_system_clone);

            let scoring_system = Arc::clone(&scoring_system);

            let signer = Arc::new(signer.clone());

            // Each provider query is executed in its own async block.
            // The provider variable is moved into the block for logging purposes.
            let timeout_duration = Duration::from_secs_f64(size / min_bandwidth);
            let fut = async move {
                let miner_uid = node_info.neuron_info.uid;
                db.conn.lock().await.execute("UPDATE miner_stats SET retrieval_attempts = retrieval_attempts + 1 WHERE miner_uid = $1", [&miner_uid])?;
                let req_client = match reqwest::Client::builder().timeout(timeout_duration).build()
                {
                    Ok(client) => client,
                    Err(e) => {
                        let mut scoring_system_rw = scoring_system.write().await;
                        scoring_system_rw
                            .update_alpha_beta_db(miner_uid, 1.0, false)
                            .await
                            .map_err(|e| anyhow!("Failed to update scoring system: {}", e))?;
                        drop(scoring_system_rw);
                        error!(
                            "Failed to create HTTP client for miner {:?}: {}",
                            miner_uid, e
                        );
                        return Err(anyhow!("Failed to create HTTP client: {}", e));
                    }
                };

                let message = VerificationMessage {
                    netuid: node_info.neuron_info.netuid,
                    miner: KeyRegistrationInfo {
                        uid: miner_uid,
                        account_id: node_info.neuron_info.hotkey.clone(),
                    },
                    validator: KeyRegistrationInfo {
                        uid: vali_uid,
                        account_id: signer.clone().account_id().clone(),
                    },
                };
                let signature = sign_message(&signer.clone(), &message);
                let payload = HandshakePayload { signature, message };
                let payload_bytes = bincode::serialize(&payload)?;

                // log timeout
                debug!(
                    "Timeout duration for download acknowledgement: {} milliseconds",
                    timeout_duration.as_millis()
                );

                let url = match node_info
                    .http_address
                    .as_ref()
                    .and_then(base::utils::multiaddr_to_socketaddr)
                    .map(|socket_addr| {
                        format!(
                            "http://{}:{}/piece?piecehash={}&handshake={}",
                            socket_addr.ip(),
                            socket_addr.port(),
                            hex::encode(piece_hash),
                            hex::encode(payload_bytes)
                        )
                    }) {
                    Some(url) => url,
                    None => {
                        let mut scoring_system_rw = scoring_system.write().await;
                        scoring_system_rw
                            .update_alpha_beta_db(miner_uid, 1.0, false)
                            .await
                            .map_err(|e| anyhow!("Failed to update scoring system: {}", e))?;
                        drop(scoring_system_rw);
                        error!("Miner {:?} has no valid HTTP address", miner_uid);
                        return Err(anyhow!("Miner has no valid HTTP address"));
                    }
                };

                let node_response = match req_client.get(&url).send().await {
                    Ok(response) => response,
                    Err(e) => {
                        let mut scoring_system_rw = scoring_system.write().await;
                        scoring_system_rw
                            .update_alpha_beta_db(miner_uid, 1.0, false)
                            .await
                            .map_err(|e| anyhow!("Failed to update scoring system: {}", e))?;
                        drop(scoring_system_rw);
                        error!("Failed to send request to miner {:?}: {}", miner_uid, e);
                        return Err(anyhow!("Failed to send request to miner: {}", e));
                    }
                };

                trace!("Node response from {:?}: {:?}", miner_uid, node_response);

                let response_status = node_response.status();
                let body_bytes = node_response
                    .bytes()
                    .await
                    .context("Failed to read response body")?;
                debug!(
                    "Raw body preview from miner {:?}: {:?}",
                    miner_uid,
                    // show first few as readable bytes
                    &body_bytes[..std::cmp::min(100, body_bytes.len())]
                );
                debug!(
                    "Utf8 body preview from miner {:?}: {:?}",
                    miner_uid,
                    // show first few as readable bytes
                    String::from_utf8_lossy(&body_bytes[..std::cmp::min(100, body_bytes.len())])
                );

                let piece_data =
                    match base::piece::deserialise_piece_response(&body_bytes, &piece_hash) {
                        Ok(data) => data,
                        Err(e) => {
                            let mut scoring_system_rw = scoring_system.write().await;
                            scoring_system_rw
                                .update_alpha_beta_db(miner_uid, 1.0, false)
                                .await
                                .map_err(|e| anyhow!("Failed to update scoring system: {}", e))?;
                            drop(scoring_system_rw);
                            bail!("Failed to deserialize piece response: {}", e);
                        }
                    };

                // Check status of response
                if response_status != StatusCode::OK {
                    let err_msg = bincode::deserialize::<String>(&piece_data[..])?;
                    let mut scoring_system_rw = scoring_system.write().await;
                    scoring_system_rw
                        .update_alpha_beta_db(miner_uid, 1.0, false)
                        .await
                        .map_err(|e| anyhow!("Failed to update scoring system: {}", e))?;
                    drop(scoring_system_rw);
                    bail!(
                        "Response returned with status code {}: {}",
                        response_status,
                        err_msg
                    );
                }

                // Verify the integrity of the piece_data using Blake3.
                let computed_hash = blake3::hash(&piece_data);
                if computed_hash.as_bytes() != piece_hash.as_ref() {
                    let mut scoring_system_rw = scoring_system.write().await;
                    scoring_system_rw
                        .update_alpha_beta_db(miner_uid, 1.0, false)
                        .await
                        .map_err(|e| anyhow!("Failed to update scoring system: {}", e))?;
                    drop(scoring_system_rw);
                    bail!("Hash mismatch for miner {:?}", miner_uid);
                }

                // Update the scoring system with the successful retrieval.
                db.conn.lock().await.execute("UPDATE miner_stats SET retrieval_successes = retrieval_successes + 1 WHERE miner_uid = $1", [&miner_uid])?;
                db.conn.lock().await.execute("UPDATE miner_stats SET total_successes = total_successes + 1 WHERE miner_uid = $1", [&miner_uid])?;
                let mut scoring_system_rw = scoring_system.write().await;
                scoring_system_rw
                    .update_alpha_beta_db(miner_uid, 1.0, true)
                    .await
                    .map_err(|e| anyhow!("Failed to update scoring system: {}", e))?;
                drop(scoring_system_rw);

                Ok(piece_data)
            };

            requests.push(fut);
        }

        // Process the futures as they complete.
        while let Some(result) = requests.next().await {
            match result {
                Ok(piece_data) => {
                    // Return immediately on the first valid response.
                    let piece = base::piece::Piece {
                        chunk_idx,
                        piece_size: piece_value.piece_size,
                        piece_idx,
                        piece_type: piece_value.piece_type.clone(),
                        data: piece_data,
                    };
                    return Ok(piece);
                }
                Err(e) => {
                    // Log errors from individual providers and continue waiting.
                    error!("Provider error: {:?}", e);
                }
            }
        }

        bail!("No valid piece data received from any provider")
    }

    /// Producer for chunks. It consumes the pieces produced to achieve this.
    /// The chunks are sent via the MPSC channel back to the HTTP stream processor.
    async fn produce_chunk(
        validator_base_neuron: Arc<RwLock<BaseNeuron>>,
        scoring_system: Arc<RwLock<ScoringSystem>>,
        metadatadb_sender: mpsc::Sender<MetadataDBCommand>,
        address_book: AddressBook,
        chunk_tx: mpsc::Sender<Vec<u8>>,
        chunk_info: metadata::models::ChunkValue,
        chunk_idx: u64,
    ) -> Result<()> {
        // let piece_hashes = chunk_info;
        let piece_values = metadata::db::MetadataDB::get_pieces_by_chunk(
            &metadatadb_sender,
            chunk_info.chunk_hash,
        )
        .await
        .map_err(|e| {
            error!("Failed to get pieces by chunk: {}", e);
            anyhow!("Internal server error while getting pieces by chunk")
        })?;

        let total_pieces = piece_values.len();

        let (piece_task_tx, piece_task_rx) =
            mpsc::channel::<(u64, metadata::models::PieceValue)>(total_pieces);
        let piece_task_rx = Arc::new(Mutex::new(piece_task_rx));
        let (piece_result_tx, mut piece_result_rx) =
            mpsc::channel::<base::piece::Piece>(total_pieces);

        let mut join_handles = Vec::with_capacity(THREAD_COUNT);
        let local_address_book = address_book.clone();

        let (completion_tx, _) = tokio::sync::broadcast::channel::<()>(1);
        let completion_tx = Arc::new(completion_tx);

        let progress = Arc::new(AtomicUsize::new(0));

        // Spawn threads
        for _ in 0..THREAD_COUNT {
            let piece_task_rx = Arc::clone(&piece_task_rx);
            let piece_result_tx = piece_result_tx.clone();
            let address_book_clone = local_address_book.clone();
            let scoring_system_clone = scoring_system.clone();
            let validator_base_clone = validator_base_neuron.clone();

            let completion_tx = completion_tx.clone();
            let mut completion_rx = completion_tx.subscribe();

            let handle = thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new()
                    .expect("Failed to create Tokio runtime to process pieces");

                rt.block_on(async move {
                    loop {
                        let result = {
                            let mut rx_lock = piece_task_rx.lock().await;
                            rx_lock.recv().await
                        };

                        match result {
                            Some((piece_idx, piece_value)) => {
                                let p_hash = piece_value.piece_hash;
                                tokio::select! {
                                    byte_prod_res = Self::produce_piece(
                                        validator_base_clone.clone(),
                                        scoring_system_clone.clone(),
                                        address_book_clone.clone(),
                                        piece_value,
                                        chunk_idx,
                                        piece_idx,
                                    ) => {
                                        match byte_prod_res {
                                            Ok(piece) => {
                                                piece_result_tx
                                                    .send(piece)
                                                    .await
                                                    .expect("Failed to send piece");
                                            }
                                            Err(err) => {
                                                error!("Failed to process piece: {}", err);
                                            }
                                        }
                                    }
                                    _ = completion_rx.recv() => {
                                        // Download was cancelled because we have enough successful pieces
                                        debug!("Download cancelled for piece hash: {}", hex::encode(p_hash));
                                        break;
                                    }
                                }
                            }
                            None => break,
                        }
                    }
                });
            });
            join_handles.push(handle);
        }
        drop(piece_result_tx);

        for (piece_idx, piece_value) in piece_values.iter().enumerate() {
            piece_task_tx
                .send((piece_idx as u64, piece_value.clone()))
                .await?;
        }
        drop(piece_task_tx);

        let mut collected_pieces = Vec::new();
        let unique_pieces = Arc::new(RwLock::new(HashSet::new()));

        while let Some(piece) = piece_result_rx.recv().await {
            let current = progress.fetch_add(1, Ordering::SeqCst) + 1;
            debug!(
                "Download progress for chunk {}: {}/{}",
                chunk_idx, current, total_pieces
            );

            // If we have the minimum k pieces necessary for reconstructing
            // the chunk then we can exit early
            if unique_pieces.read().await.len() as u64 > chunk_info.k {
                let _ = completion_tx.send(());
                debug!("Received enough pieces for chunk reconstruction - signalling cancellation");
                break;
            }
            let mut pieces = unique_pieces.write().await;
            pieces.insert(piece.piece_idx);
            collected_pieces.push(piece);
        }

        let encoded_chunk = base::piece::EncodedChunk {
            pieces: collected_pieces,
            chunk_idx,
            k: chunk_info.k,
            m: chunk_info.m,
            chunk_size: chunk_info.chunk_size,
            padlen: chunk_info.padlen,
            original_chunk_size: chunk_info.original_chunk_size,
        };

        // Reconstruct chunk from pieces
        let reconstructed_chunk = base::piece::reconstruct_chunk(&encoded_chunk)
            .context("Failed to reconstruct chunk")?;
        chunk_tx
            .send(reconstructed_chunk)
            .await
            .context("Failed to send chunk")?;
        Ok(())
    }

    /// Process the file download request.
    pub(crate) async fn process_download(
        &self,
        tracker: metadata::models::InfohashValue,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let infohash = tracker.infohash;
        debug!("Downloading file with infohash: {}", hex::encode(infohash));

        let metadatadb_sender = self.metadatadb_sender.clone();

        let chunk_values =
            metadata::db::MetadataDB::get_chunks_by_infohash(&metadatadb_sender, infohash.to_vec())
                .await
                .map_err(|e| {
                    error!("Failed to get chunks by infohash: {}", e);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "An internal server error occurred".to_string(),
                    )
                })?;

        let state = self.state.validator.clone();

        let neuron_guard = state.neuron.read().await;
        let address_book = neuron_guard.address_book.clone();
        drop(neuron_guard);

        let (chunk_tx, chunk_rx) = mpsc::channel::<Vec<u8>>(MPSC_BUFFER_SIZE);

        let scoring_system = state.scoring_system.clone();
        let validator_base_neuron = state.neuron.clone();

        for (chunk_idx, chunk_value) in chunk_values.iter().enumerate() {
            // convert chunk_hash to a string
            let chunk_hash = chunk_value.chunk_hash;
            debug!(
                "Chunk hash: {}, idx: {}",
                hex::encode(chunk_hash),
                chunk_idx
            );

            if let Err(e) = Self::produce_chunk(
                validator_base_neuron.clone(),
                scoring_system.clone(),
                metadatadb_sender.clone(),
                address_book.clone(),
                chunk_tx.clone(),
                chunk_value.clone(),
                chunk_idx as u64,
            )
            .await
            {
                error!("Error producing chunk: {}", e);
            }

            debug!("Produced chunk {:?}", chunk_idx);
        }

        let stream =
            tokio_stream::wrappers::ReceiverStream::new(chunk_rx).map(Ok::<_, std::io::Error>);
        let body = axum::body::Body::from_stream(stream);

        Ok(body)
    }
}
