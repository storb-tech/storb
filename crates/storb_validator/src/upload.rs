use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use base::constants::MIN_BANDWIDTH;
use base::piece::{encode_chunk, get_infohash, piece_length};
use base::swarm::db::MetadataDBCommand;
use base::verification::{HandshakePayload, KeyRegistrationInfo, VerificationMessage};
use base::{swarm, BaseNeuron, NodeInfo};
use chrono::Utc;
use crabtensor::sign::{sign_message, KeypairSignature};
use crabtensor::wallet::Signer;
use futures::{Stream, TryStreamExt};
use quinn::Connection;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use subxt::ext::codec::Compact;
use subxt::ext::sp_core::hexdisplay::AsBytesRef;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, trace, warn};

use crate::constants::MIN_REQUIRED_MINERS;
use crate::quic::establish_miner_connections;
use crate::scoring::ScoringSystem;
use crate::utils::get_id_quic_uids;
use crate::ValidatorState;

const LOGGING_INTERVAL_MB: u64 = 100;
const BYTES_PER_MB: u64 = 1024 * 1024;

/// Processes upload streams and distributes file pieces to available miners.
pub(crate) struct UploadProcessor<'a> {
    pub miner_uids: Vec<u16>,
    pub miner_connections: Vec<(SocketAddr, Connection)>,
    pub validator_id: Compact<u16>,
    pub state: &'a ValidatorState,
}

/// Upload piece data to a miner
pub async fn upload_piece_data(
    validator_base_neuron: Arc<RwLock<BaseNeuron>>,
    miner_info: NodeInfo,
    conn: &Connection,
    data: Vec<u8>,
) -> Result<Vec<u8>> {
    // Send piece to miner via QUIC connection
    let (mut send_stream, mut recv_stream) = conn.open_bi().await?;

    let base_neuron_guard = validator_base_neuron.read().await;
    let signer = base_neuron_guard.signer.clone();
    drop(base_neuron_guard);

    let message = VerificationMessage {
        netuid: miner_info.neuron_info.netuid,
        miner: KeyRegistrationInfo {
            uid: miner_info.neuron_info.uid,
            account_id: miner_info.neuron_info.hotkey.clone(),
        },
        validator: KeyRegistrationInfo {
            uid: validator_base_neuron
                .read()
                .await
                .local_node_info
                .uid
                .context("Failed to get UID for validator")?,
            account_id: signer.account_id().clone(),
        },
    };
    let signature = sign_message(&signer, &message);
    let payload = HandshakePayload { signature, message };
    let payload_bytes = bincode::serialize(&payload)?;
    debug!("payload_bytes: {:?}", payload_bytes);

    let piece_size = data.len();
    let size: f64 = piece_size as f64;
    let min_bandwidth = MIN_BANDWIDTH as f64;
    let timeout_duration = std::time::Duration::from_secs_f64(size / min_bandwidth); // this should scale with piece size

    // Send payload length
    send_stream
        .write_all(&(payload_bytes.len() as u64).to_be_bytes())
        .await?;

    // Send signature to the miner for it to verify
    send_stream.write_all(&payload_bytes).await?;

    // Send data length and data itself
    send_stream
        .write_all(&(piece_size as u64).to_be_bytes())
        .await?;
    send_stream.write_all(&data).await?;
    send_stream.finish()?;

    debug!(
        "Timeout duration for upload acknowledgement: {} milliseconds",
        timeout_duration.as_millis()
    );

    // Wrap the upload response operation in a timeout
    let hash = tokio::time::timeout(timeout_duration, async {
        // Read hash response
        let mut hash = Vec::new();
        let mut buf = [0u8; 1];
        let mut count = 0;
        while let Ok(Some(n)) = recv_stream.read(&mut buf).await {
            if n == 0 || count > 31 {
                break;
            }
            hash.push(buf[0]);
            count += 1;
        }
        Ok::<Vec<u8>, anyhow::Error>(hash)
    })
    .await
    .context("Timed out waiting for upload acknowledgement")??;

    Ok(hash)
}

impl<'a> UploadProcessor<'a> {
    /// Create a new instance of the UploadProcessor.
    pub(crate) async fn new(state: &'a ValidatorState) -> Result<Self> {
        let (validator_id, quic_addresses, miner_uids) =
            get_id_quic_uids(state.validator.clone()).await;

        info!(
            "Attempting to establish connections with {} miners",
            quic_addresses.len()
        );
        info!("Miner UIDs: {:?}", miner_uids);
        info!("Quic addresses: {:?}", quic_addresses);

        // Establish QUIC connections with miners
        // TODO: do we want to establish connections with all miners?
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
            miner_uids,
            miner_connections,
            validator_id,
            state,
        })
    }

    // TODO(restore)
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
        let miner_uids = self.miner_uids.clone();
        let miner_connections = self.miner_connections.clone();
        let validator = self.state.validator.clone();
        let validator_read_guard = validator.neuron.read().await;
        let metadatdb_sender = validator.metadatadb_sender.clone();

        // Get MemoryDB from state
        let scoring_system = validator.scoring_system.clone();

        let signer = validator_read_guard.signer.clone();
        // Clone the signer to avoid borrowing issues
        let signer_clone = signer.clone();
        // Cloning again to avoid borrowing issues
        let validator_clone = validator.clone();
        let consumer_handle = tokio::spawn(async move {
            consume_bytes(
                validator_clone.neuron.clone(),
                scoring_system,
                rx,
                miner_uids,
                miner_connections,
                signer_clone,
            )
            .await
        });

        drop(validator_read_guard);

        let now = Utc::now();
        let timestamp = now.timestamp();
        let filename: String = "filename".to_string(); // TODO: get filename from params

        // Wait for both tasks to complete
        let (_producer_result, consumer_result) = tokio::join!(producer_handle, consumer_handle);

        let (chunks_with_pieces, piece_hashes) = consumer_result??;

        let infohash: [u8; 32] = get_infohash(piece_hashes);
        // convert the infohash to a hex string for logging
        let infohash_str = hex::encode(infohash);

        // Put tracker entry into local database
        let infohash_value = swarm::models::InfohashValue {
            infohash,
            length: total_size,
            chunk_size,
            chunk_count: (total_size + chunk_size - 1) / chunk_size,
            creation_timestamp: now,
            // TODO: use the signature of the data blob owner
            // empty signature for now
            signature: KeypairSignature::from_raw([0; 64]),
        };

        swarm::db::MetadataDB::insert_object(&metadatdb_sender, infohash_value, chunks_with_pieces)
            .await
            .expect("Failed to insert object entry into local database"); // TOOD: handle this error properly
        debug!("Inserted object entry with infohash {infohash_str} into local database");

        Ok(infohash_str)
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

// TODO(restore)
async fn consume_bytes(
    validator_base_neuron: Arc<RwLock<BaseNeuron>>,
    scoring_system: Arc<RwLock<ScoringSystem>>,
    mut rx: mpsc::Receiver<Vec<u8>>,
    miner_uids: Vec<u16>,
    miner_connections: Vec<(SocketAddr, Connection)>,
    signer: Signer,
) -> Result<(
    Vec<(swarm::models::ChunkValue, Vec<swarm::models::PieceValue>)>,
    Vec<[u8; 32]>,
)> {
    let mut chunk_idx = 0;
    let mut piece_hashes: Vec<[u8; 32]> = Vec::new();
    let mut chunks_with_pieces: Vec<(swarm::models::ChunkValue, Vec<swarm::models::PieceValue>)> =
        Vec::new();

    let validator_guard = validator_base_neuron.read().await;
    let vali_uid = validator_guard
        .local_node_info
        .uid
        .context("Failed to get UID for validator")?;
    let validator_id = Compact(vali_uid);
    drop(validator_guard);

    // TODO: the miners connections that will be used here should be determined by the scoring system
    // and hence determine which miners to use for each piece. we do the following for the time being:
    // randomly shuffle the miner connections and uids, but do so so that they are shuffled in the same order
    // good way to do this is to first create a vector of indices and then shuffle that
    // and then use that to index into the miner connections and uids
    let mut indices: Vec<usize> = (0..miner_connections.len()).collect();
    let mut rng: StdRng = SeedableRng::from_entropy();
    indices.shuffle(&mut rng);
    let miner_connections: Vec<(SocketAddr, Connection)> = indices
        .iter()
        .map(|&i| miner_connections[i].clone())
        .collect();
    let miner_uids: Vec<u16> = indices.iter().map(|&i| miner_uids[i]).collect();

    while let Some(chunk) = rx.recv().await {
        // Encode the chunk using FEC
        let encoded = encode_chunk(&chunk, chunk_idx);
        let mut chunk_piece_hashes: Vec<[u8; 32]> = Vec::new();
        let mut chunk_hash_raw = blake3::Hasher::new();

        // Distribute pieces to miners
        let mut futures = Vec::new();
        let successful_pieces = Arc::new(RwLock::new(HashSet::new()));
        let upload_count = Arc::new(AtomicUsize::new(0));
        let target_piece_count = encoded.pieces.len();

        // Create a channel to signal when we have enough successful uploads
        let (completion_tx, _) = tokio::sync::broadcast::channel(1);
        let completion_tx = Arc::new(completion_tx);

        // Calculate how many times we'll try to distribute each piece
        // set the number of miner connections to use for this chunk
        // TODO: this may become a constant in the future: https://github.com/storb-tech/storb/issues/66
        let num_connections = 2 * encoded.pieces.len();
        let attempts_per_piece = num_connections / encoded.pieces.len();

        // For each attempt at distributing pieces
        for attempt in 0..attempts_per_piece {
            // For each piece
            for (piece_idx, piece) in encoded.pieces.iter().enumerate() {
                // Calculate which miner connection to use for this piece in this attempt
                let connection_idx = attempt * encoded.pieces.len() + piece_idx;
                // NOTE: This will wrap around if there are more attempts than connections
                // This is not ideal, but it will work for now
                // Rishi: A round-robin approach might be suitable here
                let miner_idx = connection_idx % miner_connections.len();

                let piece = piece.clone();
                let miner_uid = miner_uids[miner_idx];
                let (_, conn) = miner_connections[miner_idx].clone();

                let vali_clone = validator_base_neuron.clone();
                let scoring_clone = scoring_system.clone();
                let successful_pieces = successful_pieces.clone();
                let upload_count = upload_count.clone();
                let completion_tx = completion_tx.clone();
                let mut completion_rx = completion_tx.subscribe();

                let piece_data = piece.clone();
                let future = tokio::spawn(async move {
                    // Check if this piece was already successfully uploaded
                    let suc_pieces_read = successful_pieces.read().await;
                    if suc_pieces_read.contains(&piece.piece_idx) {
                        return Ok(None);
                    }
                    drop(suc_pieces_read);

                    let vali_guard = vali_clone.read().await;

                    let miner_info = vali_guard
                        .address_book
                        .clone()
                        .get(&miner_uid.clone())
                        .ok_or_else(|| {
                            anyhow::anyhow!(
                                "Miner info not found for miner with UID: {}",
                                &miner_uid
                            )
                        })?
                        .clone();
                    drop(vali_guard);
                    tokio::select! {
                        upload_result = upload_piece_to_miner(
                            vali_clone.clone(),
                            miner_info,
                            &conn,
                            piece_data.clone(),
                            scoring_clone,
                        ) => {
                            match upload_result {
                                Ok((piece_hash, miner_uid)) => {
                                    let mut pieces = successful_pieces.write().await;
                                    if !pieces.contains(&piece.piece_idx) {
                                        pieces.insert(piece.piece_idx);
                                        let count = upload_count.fetch_add(1, Ordering::SeqCst) + 1;

                                        // If we've uploaded all pieces, signal completion
                                        if count >= target_piece_count {
                                            let _ = completion_tx.send(());
                                            debug!("Required number of pieces uploaded - signalling completion");
                                        }

                                        return Ok(Some((piece.piece_idx, piece_data, piece_hash, miner_uid)));
                                    }
                                    Ok(None)
                                }
                                Err(e) => Err(e)
                            }
                        }
                        _ = completion_rx.recv() => {
                            // Upload was cancelled because we have enough successful pieces
                            trace!("Upload cancelled for piece {}", piece.piece_idx);
                            Ok(None)
                        }
                    }
                });

                futures.push(future);
            }
        }

        // Wait for all uploads to complete or be cancelled
        let results = futures::future::join_all(futures).await;

        // a map to track miners for each piece hash (piece_hash -> Vec<miner_uid>)
        let mut miners_for_piece: HashMap<[u8; 32], Vec<u16>> = HashMap::new();
        // Process successful uploads
        for result in results {
            match result {
                // TODO: remove unused variables from the match arm?
                Ok(Ok(Some((piece_idx, piece, piece_hash, miner_uid)))) => {
                    // Process successful upload (DHT updates etc)
                    chunk_hash_raw.update(&piece_hash);
                    chunk_piece_hashes.push(piece_hash);
                    piece_hashes.push(piece_hash);

                    debug!("UPLOAD PIECE HASH: {:?}", &piece_hash);

                    // insert miner uid into miners_for_piece map
                    miners_for_piece
                        .entry(piece_hash)
                        .or_default()
                        .push(miner_uid);
                }
                Ok(Ok(None)) => {
                    // Upload was skipped or cancelled - no action needed
                }
                Ok(Err(e)) => {
                    error!("Failed to upload piece: {}", e);
                }
                Err(e) => {
                    error!("Failed to join piece upload task: {}", e);
                }
            }
        }

        // TODO: some of this stuff can be moved into its own helper function
        let chunk_size = encoded.chunk_size;
        let chunk_hash_finalized = chunk_hash_raw.finalize();
        let chunk_hash = chunk_hash_finalized.as_bytes();

        debug!("UPLOAD CHUNK HASH: {:?}", &chunk_hash);

        let chunk_dht_value = swarm::models::ChunkValue {
            chunk_hash: *chunk_hash,
            k: encoded.k,
            m: encoded.m,
            chunk_size,
            padlen: encoded.padlen,
            original_chunk_size: encoded.original_chunk_size,
        };

        // create vector of the piecevalues for the current chunk
        // the piece_type for each piece can be retrieved by getting the piece type from the encoded pieces
        let chunk_piece_values = encoded
            .pieces
            .iter()
            .enumerate()
            .map(|(piece_idx, piece)| swarm::models::PieceValue {
                piece_hash: chunk_piece_hashes[piece_idx],
                piece_size: piece.data.len() as u64,
                piece_type: piece.piece_type.clone(),
                miners: miners_for_piece
                    .get(&chunk_piece_hashes[piece_idx])
                    .cloned()
                    .unwrap_or_default()
                    .into_iter()
                    .map(Compact)
                    .collect(),
            })
            .collect::<Vec<_>>();

        // Put chunk entry into local database
        debug!("Distributed pieces for chunk with hash {:?}", chunk_hash);

        let chunk_with_pieces = (chunk_dht_value.clone(), chunk_piece_values.clone());
        chunks_with_pieces.push(chunk_with_pieces);

        chunk_idx += 1;
    }

    Ok((chunks_with_pieces, piece_hashes))
}

/// Upload a piece to a miner and verify the upload by checking the hash, then update the miner's stats
pub async fn upload_piece_to_miner(
    validator_base_neuron: Arc<RwLock<BaseNeuron>>,
    miner_info: NodeInfo,
    conn: &Connection,
    piece: base::piece::Piece,
    scoring_system: Arc<RwLock<ScoringSystem>>,
) -> Result<([u8; 32], u16)> {
    let miner_uid = miner_info.neuron_info.uid;
    // Track request
    if let Err(e) = scoring_system
        .write()
        .await
        .increment_request_counter(miner_uid as usize)
        .await
    {
        error!(
            "Failed to increment request counter for miner {}: {}",
            miner_uid, e
        );
    }

    let piece_hash = *blake3::hash(&piece.data).as_bytes();
    let upload_result =
        upload_piece_data(validator_base_neuron.clone(), miner_info, conn, piece.data).await?;

    // Verification: Check if the hash received from the miner is the same
    // as the hash of the piece and update the stats of the miner in the database accordingly

    // log hash that we get vs. hash that we expect
    info!(
        "Hash from miner: {:?}, Expected hash: {:?}",
        upload_result.as_bytes_ref(),
        piece_hash
    );

    // Update stats
    let db = scoring_system.write().await.db.clone();
    db.conn.lock().await.execute(
        "UPDATE miner_stats SET store_attempts = store_attempts + 1 WHERE miner_uid = ?",
        [miner_uid],
    )?;

    if upload_result.as_bytes_ref() == piece_hash {
        db.conn.lock().await.execute(
            "UPDATE miner_stats SET store_successes = store_successes + 1, total_successes = total_successes + 1 WHERE miner_uid = ?",
            [miner_uid],
        )?;
    }

    Ok((piece_hash, miner_uid))
}
