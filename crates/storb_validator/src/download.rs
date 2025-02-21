use std::sync::Arc;
use std::thread;

use anyhow::{anyhow, bail, Context, Result};
use axum::http::StatusCode;
use axum::response::IntoResponse;
use base::constants::CLIENT_TIMEOUT;
use base::swarm::dht::DhtCommand;
use base::{swarm, AddressBook};
use futures::stream::FuturesUnordered;
use libp2p::kad::RecordKey;
use tokio::sync::{mpsc, Mutex};
use tokio_stream::StreamExt;
use tracing::{debug, error, info, trace};

use crate::ValidatorState;

const MPSC_BUFFER_SIZE: usize = 10;
// TODO: Put this in config or just base it on hardware threads
const THREAD_COUNT: usize = 10;

/// Processes download streams and retrieves file pieces from available miners.
pub(crate) struct DownloadProcessor {
    pub dht_sender: mpsc::Sender<DhtCommand>,
    pub state: Arc<ValidatorState>,
}

impl DownloadProcessor {
    /// Create a new instance of the DownloadProcessor.
    pub(crate) async fn new(state: &ValidatorState) -> Result<Self> {
        let dht_sender = state.validator.read().await.neuron.command_sender.clone();
        Ok(Self {
            dht_sender,
            state: Arc::new(state.clone()),
        })
    }

    /// Producer for pieces. Queries the miners to get piece data.
    async fn produce_piece(
        dht_sender: mpsc::Sender<DhtCommand>,
        local_address_book: AddressBook,
        piece_hash: [u8; 32],
    ) -> Result<base::piece::Piece> {
        // Retrieve the piece entry.
        let piece_key = RecordKey::new(&piece_hash);
        debug!("RecordKey of piece hash: {:?}", &piece_key);
        let piece_entry = swarm::dht::StorbDHT::get_piece_entry(&dht_sender, piece_key.clone())
            .await
            .map_err(|err| anyhow!("Failed to get piece entry: {err}"))
            .context("Failed to get piece entry from DHT")?
            .ok_or_else(|| anyhow!("Piece entry not found"))?;

        debug!(
            "Looking for piece providers for {:?}",
            &piece_entry.piece_hash
        );

        let piece_providers =
            swarm::dht::StorbDHT::get_piece_providers(&dht_sender, piece_entry.piece_hash.clone())
                .await
                .map_err(|err| anyhow!("Failed to get piece providers: {err}"))
                .context("Error getting piece providers")?;

        if piece_providers.is_empty() {
            bail!("No providers found for piece {:?}", &piece_entry.piece_hash);
        }

        let mut requests = FuturesUnordered::new();

        for provider in piece_providers {
            // Look up node info from the address book.
            let node_info = local_address_book
                .read()
                .await
                .get(&provider)
                .ok_or_else(|| anyhow!("Provider {:?} not found in local address book", provider))?
                .clone();

            // Each provider query is executed in its own async block.
            // The provider variable is moved into the block for logging purposes.
            let fut = async move {
                let req_client = reqwest::Client::builder()
                    .timeout(CLIENT_TIMEOUT)
                    .build()
                    .context("Failed to build reqwest client")?;

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
                    .ok_or_else(|| anyhow!("Invalid HTTP address in node_info"))?;

                let node_response = req_client
                    .get(&url)
                    .send()
                    .await
                    .context("Failed to query node")?;

                trace!("Node response from {:?}: {:?}", provider, node_response);

                let body_bytes = node_response
                    .bytes()
                    .await
                    .context("Failed to read response body")?;

                let piece_data = base::piece::deserialise_piece_response(&body_bytes, &piece_hash)
                    .context("Failed to deserialise piece response")?;

                // Verify the integrity of the piece_data using Blake3.
                let computed_hash = blake3::hash(&piece_data);
                if computed_hash.as_bytes() != &piece_hash {
                    bail!("Hash mismatch for provider {:?}", provider);
                }

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
                        chunk_idx: piece_entry.chunk_idx,
                        piece_idx: piece_entry.piece_idx,
                        piece_type: piece_entry.piece_type.clone(),
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
    /// The chunks are sent via the MSPC channel back to the HTTP stream processor.
    async fn produce_chunk(
        dht_sender: mpsc::Sender<DhtCommand>,
        address_book: AddressBook,
        chunk_tx: mpsc::Sender<Vec<u8>>,
        chunk_info: base::swarm::models::ChunkDHTValue,
    ) -> Result<()> {
        let piece_hashes = chunk_info.piece_hashes;
        let total_pieces = piece_hashes.len();

        let (piece_task_tx, piece_task_rx) = mpsc::channel::<[u8; 32]>(total_pieces);
        let piece_task_rx = Arc::new(Mutex::new(piece_task_rx));
        let (piece_result_tx, mut piece_result_rx) =
            mpsc::channel::<base::piece::Piece>(total_pieces);

        let mut join_handles = Vec::with_capacity(10);
        let dht_sender = dht_sender.clone();
        let local_address_book = address_book.clone();

        // Spawn threads
        for _ in 0..THREAD_COUNT {
            let piece_task_rx = Arc::clone(&piece_task_rx);
            let piece_result_tx = piece_result_tx.clone();
            let dht_sender_clone = dht_sender.clone();
            let address_book_clone = local_address_book.clone();

            let handle = thread::spawn(move || {
                let rt = tokio::runtime::Runtime::new()
                    .expect("Failed to create Tokio runtime to process pieces");

                rt.block_on(async move {
                    loop {
                        let task = {
                            let mut rx_lock = piece_task_rx.lock().await;
                            rx_lock.recv().await
                        };

                        match task {
                            Some(task) => {
                                match Self::produce_piece(
                                    dht_sender_clone.clone(),
                                    address_book_clone.clone(),
                                    task,
                                )
                                .await
                                {
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
                            None => break,
                        }
                    }
                });
            });
            join_handles.push(handle);
        }
        drop(piece_result_tx);

        for piece_hash in piece_hashes {
            piece_task_tx.send(piece_hash).await?;
        }
        drop(piece_task_tx);

        let mut collected_pieces = Vec::new();
        while let Some(piece) = piece_result_rx.recv().await {
            collected_pieces.push(piece);
        }

        let encoded_chunk = base::piece::EncodedChunk {
            pieces: collected_pieces,
            chunk_idx: chunk_info.chunk_idx,
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
        infohash: String,
    ) -> Result<impl IntoResponse, (StatusCode, String)> {
        let key = RecordKey::new(&infohash.as_bytes().to_vec());
        debug!("Downloading file with infohash: {:?}", &key);

        let tracker_res = swarm::dht::StorbDHT::get_tracker_entry(self.dht_sender.clone(), key)
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

        let state = self.state.validator.read().await.clone();
        let address_book = state.neuron.address_book;
        let dht_sender = self.dht_sender.clone();
        let (chunk_tx, chunk_rx) = mpsc::channel::<Vec<u8>>(MPSC_BUFFER_SIZE);

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

                if let Err(e) = Self::produce_chunk(
                    dht_sender.clone(),
                    address_book.clone(),
                    chunk_tx.clone(),
                    chunk,
                )
                .await
                {
                    error!("Error producing chunk: {}", e);
                }
            }
            Ok(())
        });

        let stream =
            tokio_stream::wrappers::ReceiverStream::new(chunk_rx).map(Ok::<_, std::io::Error>);
        let body = axum::body::Body::from_stream(stream);

        Ok(body)
    }
}
