use anyhow::Result;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use base::constants::CLIENT_TIMEOUT;
use base::swarm;
use libp2p::kad::RecordKey;
use tokio::sync::mpsc;
use tokio_stream::StreamExt;
use tracing::{debug, error, info};

use crate::ValidatorState;

const MPSC_BUFFER_SIZE: usize = 10;

/// Processes download streams and retrieves file pieces from available miners.
pub(crate) struct DownloadProcessor<'a> {
    // pub miner_connections: Vec<(SocketAddr, Connection)>,
    // pub validator_id: Compact<u16>,
    pub state: &'a ValidatorState,
}

impl<'a> DownloadProcessor<'a> {
    /// Create a new instance of the DownloadProcessor.
    pub(crate) async fn new(state: &'a ValidatorState) -> Result<Self> {
        Ok(Self {
            // miner_connections,
            // validator_id,
            state,
        })
    }

    // TODO
    pub(crate) async fn process_download(
        &self,
        infohash: String,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let (tx, rx) = mpsc::channel::<Vec<u8>>(MPSC_BUFFER_SIZE);

        let key = RecordKey::new(&infohash.as_bytes().to_vec());
        info!("Downloading file with infohash: {:?}", &key);
        let dht_sender = self
            .state
            .validator
            .read()
            .await
            .neuron
            .command_sender
            .clone();
        let tracker_res = swarm::dht::StorbDHT::get_tracker_entry(dht_sender.clone(), key)
            .await
            .map_err(|e| {
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    format!("Error getting tracker entry: {}", e),
                )
            })?;
        let tracker = tracker_res.ok_or((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Tracker entry not found".to_string(),
        ))?;
        info!("Tracker hash: {:?}", tracker.infohash);

        let chunk_hashes = tracker.chunk_hashes;
        let mut piece_infos: Vec<swarm::models::PieceDHTValue> = Vec::new();

        let state = self.state.validator.read().await.clone();

        tokio::spawn(async move {
            for chunk_hash in chunk_hashes.clone() {
                // convert chunk_hash to a string
                let chunk_key = RecordKey::new(&chunk_hash);
                info!("RecordKey of chunk hash: {:?}", chunk_key);

                let chunk_res = match swarm::dht::StorbDHT::get_chunk_entry(
                    dht_sender.clone(),
                    chunk_key,
                )
                .await
                {
                    Ok(chunk) => Some(chunk),
                    Err(e) => {
                        error!("Error getting chunk entry: {}", e);
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An internal server error occurred".to_string(),
                        ));
                    }
                };

                let chunk = match chunk_res {
                    Some(Some(chunk)) => chunk,
                    Some(None) | None => {
                        error!("Chunk entry not found");
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An internal server error occurred".to_string(),
                        ));
                    }
                };
                info!(
                    "Found chunk hash: {:?} with {:?} pieces",
                    &chunk.chunk_hash,
                    &chunk.piece_hashes.len()
                );

                let piece_hashes = chunk.piece_hashes;
                let mut piece_data: Vec<base::piece::Piece> =
                    Vec::with_capacity(piece_hashes.len());

                for piece_hash in piece_hashes {
                    let piece_key = RecordKey::new(&piece_hash);
                    info!("RecordKey of piece hash: {:?}", &piece_key);
                    let piece_res = swarm::dht::StorbDHT::get_piece_entry(
                        dht_sender.clone(),
                        piece_key.clone(),
                    )
                    .await
                    .map_err(|e| {
                        error!("Error getting piece entry: {}", e);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An internal server error occurred".to_string(),
                        )
                    })?;
                    let piece_info = match piece_res {
                        Some(piece_entry) => piece_entry,
                        None => {
                            error!("Piece entry not found");
                            return Err((
                                StatusCode::INTERNAL_SERVER_ERROR,
                                "An internal server error occurred".to_string(),
                            ));
                        }
                    };
                    piece_infos.push(piece_info.clone());
                    info!(
                        "Looking for piece providers for {:?}",
                        &piece_info.piece_hash
                    );

                    let piece_providers = swarm::dht::StorbDHT::get_piece_providers(
                        dht_sender.clone(),
                        piece_info.piece_hash.clone(),
                    )
                    .await
                    .map_err(|_| {
                        error!("No miner found for piece distribution");
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An internal server error occurred".to_string(),
                        )
                    })
                    .unwrap(); // TODO: proper error handling

                    if piece_providers.is_empty() {
                        error!("No providers found for piece");
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An internal server error occurred".to_string(),
                        ));
                    }
                    let local_address_book = state.neuron.address_book.read().await;
                    for provider in piece_providers {
                        let node_info = local_address_book.get(&provider);
                        if let Some(node_info) = node_info {
                            info!(
                                "Found Provider: {:?} for piece {:?}",
                                node_info, piece_info.piece_hash
                            );

                            let req_client = reqwest::Client::builder()
                                .timeout(CLIENT_TIMEOUT)
                                .build()
                                .map_err(|e| {
                                    error!("Failed to build client: {}", e);
                                    (
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "An internal server error occurred".to_string(),
                                    )
                                })?;

                            let url = node_info
                                .http_address
                                .as_ref()
                                .and_then(base::utils::multiaddr_to_socketaddr)
                                .map(|socket_addr| {
                                    format!(
                                        "http://{}:{}/piece?piecehash={}",
                                        socket_addr.ip(),
                                        socket_addr.port(),
                                        hex::encode(piece_hash)
                                    )
                                })
                                .ok_or_else(|| {
                                    error!("Invalid HTTP address in node_info");
                                    (
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "An internal server error occurred".to_string(),
                                    )
                                })?;

                            let node_response = req_client.get(&url).send().await.map_err(|e| {
                                error!("Failed to query node: {}", e);
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "An internal server error occurred".to_string(),
                                )
                            })?;

                            debug!("Node response: {:?}", node_response);
                            let body_bytes = node_response.bytes().await.map_err(|e| {
                                error!("Failed to read response body: {}", e);
                                (
                                    StatusCode::INTERNAL_SERVER_ERROR,
                                    "An internal server error occurred".to_string(),
                                )
                            })?;

                            let deserialised_data = match base::piece::deserialise_piece_response(
                                &body_bytes,
                                &piece_hash,
                            ) {
                                Ok(data) => {
                                    debug!("Deserialised piece response");
                                    data
                                }
                                Err(e) => {
                                    error!("Failed to deserialise piece response: {}", e);
                                    return Err((
                                        StatusCode::INTERNAL_SERVER_ERROR,
                                        "An internal server error occurred".to_string(),
                                    ));
                                }
                            };

                            let piece = base::piece::Piece {
                                chunk_idx: piece_info.chunk_idx,
                                piece_idx: piece_info.piece_idx,
                                piece_type: piece_info.piece_type.clone(),
                                data: deserialised_data,
                            };

                            piece_data.push(piece);
                        }
                    }
                }

                let chunk_info = base::piece::EncodedChunk {
                    pieces: piece_data,
                    chunk_idx: chunk.chunk_idx,
                    k: chunk.k,
                    m: chunk.m,
                    chunk_size: chunk.chunk_size,
                    padlen: chunk.padlen,
                    original_chunk_size: chunk.original_chunk_size,
                };

                // Reconstruct chunk from pieces
                let reconstructed_chunk = match base::piece::reconstruct_chunk(&chunk_info) {
                    Ok(reconstructed_chunk) => reconstructed_chunk,
                    Err(e) => {
                        error!("Failed to reconstruct chunk: {}", e);
                        return Err((
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "An internal server error occurred".to_string(),
                        ));
                    }
                };

                if tx.send(reconstructed_chunk).await.is_err() {
                    info!("Receiver dropped, stopping stream.");
                    break;
                }
            }
            Ok(())
        });

        // Reconstruct file from chunks
        let stream = tokio_stream::wrappers::ReceiverStream::new(rx).map(Ok::<_, std::io::Error>);
        let body = axum::body::Body::from_stream(stream);

        Ok(body)
    }
}
