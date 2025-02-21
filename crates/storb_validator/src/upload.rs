use std::net::SocketAddr;

use anyhow::{bail, Result};
use axum::body::Bytes;
use base::piece::{encode_chunk, get_infohash, piece_length};
use base::swarm::{self, dht::DhtCommand, models};
use chrono::Utc;
use crabtensor::sign::sign_message;
use crabtensor::wallet::Signer;
use futures::{Stream, TryStreamExt};
use libp2p::Multiaddr;
use quinn::Connection;
use subxt::ext::codec::Compact;
use tokio::sync::mpsc;
use tracing::{debug, error, info, warn};

use crate::{quic::establish_miner_connections, ValidatorState};

const MIN_REQUIRED_MINERS: usize = 1;
const LOGGING_INTERVAL_MB: u64 = 100;
const BYTES_PER_MB: u64 = 1024 * 1024;

/// Processes upload streams and distributes file pieces to available miners.
pub(crate) struct UploadProcessor<'a> {
    pub miner_connections: Vec<(SocketAddr, Connection)>,
    pub validator_id: Compact<u16>,
    pub state: &'a ValidatorState,
}

impl<'a> UploadProcessor<'a> {
    /// Create a new instance of the UploadProcessor.
    pub(crate) async fn new(state: &'a ValidatorState) -> Result<Self> {
        let (validator_id, quic_addresses) = {
            let address_book_arc = state.validator.read().await.neuron.address_book.clone();
            let validator_id = Compact(
                state
                    .validator
                    .read()
                    .await
                    .neuron
                    .local_node_info
                    .uid
                    .expect("Could not read node UID"),
            );
            let address_book = address_book_arc.read().await;
            let quic_addresses: Vec<Multiaddr> = address_book
                .iter()
                .filter_map(|(_, node_info)| node_info.quic_address.clone())
                .collect();

            (validator_id, quic_addresses)
        };

        // Establish QUIC connections with miners
        let miner_connections = match establish_miner_connections(quic_addresses).await {
            Ok(connections) => {
                if connections.is_empty() {
                    bail!("Failed to establish connections with any miners");
                }
                if connections.len() < MIN_REQUIRED_MINERS {
                    warn!(
                        "Connected to fewer miners than recommended: {}",
                        connections.len()
                    );
                }
                connections
            }
            Err(e) => {
                bail!("Failed to establish miner connections: {e}");
            }
        };

        info!(
            "Successfully connected to {} miners",
            miner_connections.len()
        );
        Ok(Self {
            miner_connections,
            validator_id,
            state,
        })
    }

    /// Process the upload of a file and its distribution to miners.
    pub(crate) async fn process_upload<S, E>(
        &self,
        stream: S,
        total_size: u64,
        validator_id: Compact<u16>,
    ) -> Result<String>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    {
        info!("Processing upload of size {} bytes...", total_size);

        if self.miner_connections.is_empty() {
            return Err(anyhow::anyhow!("No active miner connections available"));
        }

        let chunk_size = piece_length(total_size, None, None);
        let (tx, rx) = mpsc::channel(chunk_size as usize);

        // Spawn producer task
        let producer_handle = {
            let stream = Box::pin(stream);
            tokio::spawn(async move {
                if let Err(e) = produce_bytes(stream, tx, total_size, chunk_size as usize).await {
                    error!("Producer error: {}", e);
                    return Err(e);
                }
                Ok(())
            })
        };

        // Spawn consumer task
        let miner_conns = self.miner_connections.clone();
        // TODO: all these clones are not the most ideal, perhaps fix?
        let dht_sender = self
            .state
            .validator
            .read()
            .await
            .neuron
            .command_sender
            .clone();
        let dht_sender_clone = dht_sender.clone();

        let signer = self.state.validator.read().await.neuron.signer.clone();
        let signer_clone = signer.clone();
        let consumer_handle = tokio::spawn(async move {
            consume_bytes(
                rx,
                miner_conns,
                validator_id,
                dht_sender_clone,
                signer_clone,
            )
            .await
        });

        let now = Utc::now();
        let timestamp = now.timestamp();
        let filename: String = "filename".to_string(); // TODO: get filename from params

        // Wait for both tasks to complete
        let (_producer_result, consumer_result) = tokio::join!(producer_handle, consumer_handle);

        let (piece_hashes, chunk_hashes) = consumer_result??;

        // log the chunk hashes as strings
        info!("Lubed up Chunk hashes: {:?}", chunk_hashes);
        let chunk_hashes_str: Vec<String> = chunk_hashes.iter().map(hex::encode).collect();
        info!("Chunk hashes: {:?}", chunk_hashes_str);

        let chunk_count = chunk_hashes.len() as u64;

        let infohash = get_infohash(
            filename.clone(),
            timestamp,
            chunk_size,
            total_size,
            piece_hashes,
        );

        let tracker_key = libp2p::kad::RecordKey::new(&infohash.as_bytes().to_vec());

        let tracker_msg = (
            tracker_key.clone(),
            validator_id,
            filename.clone(),
            total_size,
            chunk_size,
            chunk_count,
            chunk_hashes.clone(),
            timestamp,
        );

        let tracker_msg_bytes = match bincode::serialize(&tracker_msg) {
            Ok(bytes) => bytes,
            Err(err) => {
                error!("Failed to serialize msg: {:?}", err);
                bail!("Failed to serialize tracker message");
            }
        };

        let tracker_sig = sign_message(&signer, tracker_msg_bytes);

        let tracker_dht_value = models::TrackerDHTValue {
            infohash: tracker_key.clone(),
            validator_id,
            filename: filename.clone(),
            length: total_size,
            chunk_size,
            chunk_count,
            chunk_hashes: chunk_hashes.clone(),
            creation_timestamp: now,
            signature: tracker_sig,
        };

        swarm::dht::StorbDHT::put_tracker_entry(dht_sender, tracker_key, tracker_dht_value)
            .await
            .expect("Failed to put tracker entry into DHT"); // TODO: handle error
        debug!("Inserted tracker entry with infohash {infohash}");

        Ok(infohash)
    }
}

async fn produce_bytes<S, E>(
    stream: S,
    tx: mpsc::Sender<Vec<u8>>,
    total_size: u64,
    chunk_size: usize,
) -> Result<()>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut total_processed: u64 = 0;
    let mut buffer = Vec::with_capacity(chunk_size);

    let mut stream = stream.map_err(|e| anyhow::anyhow!("Stream error: {}", e.into()));

    while let Some(chunk_result) = stream.try_next().await? {
        buffer.extend_from_slice(&chunk_result);

        // Process full chunks
        while buffer.len() >= chunk_size {
            let chunk_data = buffer.drain(..chunk_size).collect::<Vec<u8>>();
            total_processed += chunk_data.len() as u64;

            if total_processed > total_size {
                return Err(anyhow::anyhow!("Upload size exceeds expected size"));
            }

            if total_processed % (LOGGING_INTERVAL_MB * BYTES_PER_MB) == 0 {
                info!(
                    "Processing: {} MB / {} MB",
                    total_processed / (1024 * 1024),
                    total_size / (1024 * 1024)
                );
            }

            tx.send(chunk_data).await?;
        }
    }

    // Send any remaining data
    if !buffer.is_empty() {
        total_processed += buffer.len() as u64;
        tx.send(buffer).await?;
    }

    info!("Total bytes processed: {}", total_processed);
    Ok(())
}

async fn consume_bytes(
    mut rx: mpsc::Receiver<Vec<u8>>,
    miner_connections: Vec<(SocketAddr, Connection)>,
    validator_id: Compact<u16>,
    dht_sender: mpsc::Sender<DhtCommand>,
    signer: Signer,
) -> Result<(Vec<[u8; 32]>, Vec<[u8; 32]>)> {
    let mut chunk_idx = 0;
    let mut chunk_hashes: Vec<[u8; 32]> = Vec::new();
    let mut piece_hashes: Vec<[u8; 32]> = Vec::new();

    while let Some(chunk) = rx.recv().await {
        // Encode the chunk using FEC
        let encoded = encode_chunk(&chunk, chunk_idx);
        let mut chunk_piece_hashes: Vec<[u8; 32]> = Vec::new();
        let mut chunk_hash_raw = blake3::Hasher::new();
        // Distribute pieces to miners
        for piece in encoded.pieces {
            let piece_idx = piece.piece_idx;
            // Round-robin distribution to miners
            let (addr, conn) = &miner_connections[(piece_idx as usize) % miner_connections.len()];

            // get blake3 hash of piece
            let piece_hash_raw = blake3::hash(&piece.data);
            let piece_hash = piece_hash_raw.as_bytes();

            info!("Sending piece {piece_idx} to miner {addr}");

            // Send piece to miner via QUIC connection
            let (mut send_stream, mut recv_stream) = conn.open_bi().await?;

            send_stream
                .write_all(&(piece.data.len() as u64).to_be_bytes())
                .await?;
            send_stream.write_all(&piece.data).await?;
            send_stream.finish()?;

            // Read hash response
            let mut _hash = Vec::new();
            let mut buf = [0u8; 1];
            while let Ok(Some(n)) = recv_stream.read(&mut buf).await {
                if n == 0 || buf[0] == b'\n' {
                    break;
                }
                _hash.push(buf[0]);
            }

            // TODO: verification + scoring

            // TODO: maybe do this in a background thread instead of doing it here?
            // Add piece metadata to DHT
            // TODO: some of this stuff can be moved into its own helper function
            let piece_key = libp2p::kad::RecordKey::new(&piece_hash);
            info!("UPLOAD PIECE KEY: {:?}", &piece_key);

            let msg = (
                piece_key.clone(),
                validator_id,
                chunk_idx,
                piece_idx,
                piece.piece_type.clone(),
            );
            let msg_bytes = match bincode::serialize(&msg) {
                Ok(bytes) => bytes,
                Err(err) => {
                    error!("Failed to serialize msg: {:?}", err);
                    continue; // Skip to the next iteration on error
                }
            };
            let signature = sign_message(&signer, msg_bytes);
            // Get netuid from validator state
            let value = models::PieceDHTValue {
                piece_hash: piece_key.clone(),
                validator_id,
                chunk_idx,
                piece_idx,
                piece_type: piece.piece_type,
                signature,
            };

            swarm::dht::StorbDHT::put_piece_entry(dht_sender.clone(), piece_key, value)
                .await
                .expect("Failed to put piece entry in DHT"); // TODO: handle error

            info!("Received hash for piece {piece_idx} from miner {addr}");
            chunk_hash_raw.update(piece_hash);
            chunk_piece_hashes.push(*piece_hash);
            piece_hashes.extend(chunk_piece_hashes.iter().cloned());
        }

        // TODO: some of this stuff can be moved into its own helper function
        let chunk_size = encoded.chunk_size;
        let chunk_hash = chunk_hash_raw.finalize();

        let chunk_key = libp2p::kad::RecordKey::new(&chunk_hash.as_bytes());
        info!("UPLOAD CHUNK KEY: {:?}", &chunk_key);

        let chunk_msg = (
            chunk_key.clone(),
            validator_id,
            chunk_piece_hashes.clone(),
            chunk_idx,
            encoded.k,
            encoded.m,
            chunk_size,
            encoded.padlen,
            encoded.original_chunk_size,
        );

        let chunk_msg_bytes = match bincode::serialize(&chunk_msg) {
            Ok(bytes) => bytes,
            Err(err) => {
                error!("Failed to serialize msg: {:?}", err);
                continue; // Skip to the next iteration on error
            }
        };

        let chunk_sig = sign_message(&signer, chunk_msg_bytes);

        let chunk_dht_value = models::ChunkDHTValue {
            chunk_hash: chunk_key.clone(),
            validator_id,
            piece_hashes: chunk_piece_hashes,
            chunk_idx,
            k: encoded.k,
            m: encoded.m,
            chunk_size,
            padlen: encoded.padlen,
            original_chunk_size: encoded.original_chunk_size,
            signature: chunk_sig,
        };

        swarm::dht::StorbDHT::put_chunk_entry(
            dht_sender.clone(),
            chunk_key.clone(),
            chunk_dht_value,
        )
        .await
        .expect("Failed to put chunk entry into DHT"); // TODO: handle error
        debug!("Distributed pieces for chunk with hash {:?}", chunk_key);

        chunk_hashes.push(*chunk_hash.as_bytes());

        chunk_idx += 1;
    }

    Ok((piece_hashes, chunk_hashes))
}
