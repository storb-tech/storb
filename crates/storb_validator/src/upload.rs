use std::collections::{HashMap, HashSet};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use anyhow::{bail, Context, Result};
use axum::body::Bytes;
use base::constants::MIN_BANDWIDTH;
use base::piece::{
    encode_chunk, get_infohash_by_identity, piece_length, ChunkHash, InfoHash, PieceHash,
};
use base::verification::{HandshakePayload, KeyRegistrationInfo, VerificationMessage};
use base::{BaseNeuron, NodeInfo, NodeUID};
use chrono::Utc;
use crabtensor::sign::{sign_message, KeypairSignature};
use crabtensor::AccountId;
use futures::{Stream, TryStreamExt};
use libp2p::Multiaddr;
use opentelemetry::metrics::{Counter, Histogram};
use opentelemetry::KeyValue;
use quinn::Connection;
use rand::rngs::StdRng;
use rand::seq::SliceRandom;
use rand::SeedableRng;
use subxt::ext::codec::Compact;
use subxt::ext::sp_core::hexdisplay::AsBytesRef;
use tokio::sync::{mpsc, RwLock};
use tracing::{debug, error, info, instrument, trace, warn};

use crate::constants::MIN_REQUIRED_MINERS;
use crate::metadata;
use crate::metadata::models::SqlAccountId;
use crate::quic::establish_miner_connections;
use crate::scoring::ScoringSystem;
use crate::utils::get_id_quic_uids;
use crate::validator::UPLOAD_METER;
use crate::ValidatorState;

const LOGGING_INTERVAL_MB: u64 = 100;
const BYTES_PER_MB: u64 = 1024 * 1024;

#[derive(Debug)]
pub struct UploadMetrics {
    // Total Upload Requests
    pub upload_requests: Counter<u64>,
    /// Total bytes received from client
    pub bytes_received: Counter<u64>,
    /// Number of chunks processed
    pub chunks_processed: Counter<u64>,
    /// Time taken to process each chunk
    pub chunk_processing_time: Histogram<f64>,
    /// Time taken to process each piece
    pub piece_processing_time: Histogram<f64>,
    /// Number of pieces attempted per miner
    pub pieces_uploaded: Counter<u64>,
    /// Number of successful piece uploads
    pub pieces_succeeded: Counter<u64>,
    /// Number of failed piece uploads
    pub pieces_failed: Counter<u64>,
}

impl UploadMetrics {
    pub fn new() -> Self {
        let m = &*UPLOAD_METER;
        Self {
            upload_requests: m.u64_counter("upload.requests").build(),
            bytes_received: m.u64_counter("upload.bytes_received").build(),
            chunks_processed: m.u64_counter("upload.chunks_processed").build(),
            chunk_processing_time: m.f64_histogram("upload.chunk_processing_ms").build(),
            piece_processing_time: m.f64_histogram("upload.piece_processing_ms").build(),
            pieces_uploaded: m.u64_counter("upload.pieces_uploaded").build(),
            pieces_succeeded: m.u64_counter("upload.pieces_succeeded").build(),
            pieces_failed: m.u64_counter("upload.pieces_failed").build(),
        }
    }
}

/// Processes upload streams and distributes file pieces to available miners.
pub(crate) struct UploadProcessor<'a> {
    pub miner_uids: Vec<NodeUID>,
    pub miner_connections: Vec<(SocketAddr, Connection)>,
    pub state: &'a ValidatorState,
    pub metrics: Arc<UploadMetrics>,
}

#[instrument(
    name = "upload.upload_piece_data",
    skip(validator_base_neuron, conn, data),
    fields(
        miner_uid = ?miner_info.neuron_info.uid,
        piece_size = data.len(),
    )
)]
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
    trace!("payload_bytes: {:?}", payload_bytes);

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

/// Log the initial connection attempt details
pub fn log_connection_attempt(quic_addresses: &[Multiaddr], miner_uids: &[NodeUID]) {
    info!(
        "Attempting to establish connections with {} miners",
        quic_addresses.len()
    );
    info!("Miner UIDs: {:?}", miner_uids);
    info!("Quic addresses: {:?}", quic_addresses);
}

#[instrument(
    name = "upload.establish_and_validate_connections",
    skip(quic_addresses),
    fields(requested = quic_addresses.len())
)]
/// Establish connections and validate minimum requirements
pub async fn establish_and_validate_connections(
    quic_addresses: Vec<Multiaddr>,
) -> Result<Vec<(SocketAddr, Connection)>> {
    // TODO: do we want to establish connections with all miners?
    let miner_connections = establish_miner_connections(quic_addresses)
        .await
        .context("Failed to establish miner connections")?;

    validate_connection_count(&miner_connections)?;

    Ok(miner_connections)
}

#[instrument(
    name = "upload.validate_connection_count",
    skip(connections),
    fields(actual = connections.len())
)]
/// Validate that we have sufficient connections
pub fn validate_connection_count(connections: &[(SocketAddr, Connection)]) -> Result<()> {
    if connections.is_empty() {
        bail!("Failed to establish connections with any miners");
    }

    if connections.len() < MIN_REQUIRED_MINERS {
        warn!(
            "Connected to fewer miners than recommended: {}",
            connections.len()
        );
    }

    Ok(())
}

/// Log successful connection establishment
pub fn log_connection_success(miner_connections: &[(SocketAddr, Connection)]) {
    info!(
        "Successfully connected to {} miners",
        miner_connections.len()
    );
}

impl<'a> UploadProcessor<'a> {
    /// Create a new instance of the UploadProcessor.
    pub(crate) async fn new(state: &'a ValidatorState) -> Result<Self> {
        let (_, quic_addresses, miner_uids) = get_id_quic_uids(state.validator.clone()).await;

        log_connection_attempt(&quic_addresses, &miner_uids);

        let miner_connections = establish_and_validate_connections(quic_addresses).await?;

        log_connection_success(&miner_connections);

        Ok(Self {
            miner_uids,
            miner_connections,
            state,
            metrics: Arc::new(UploadMetrics::new()),
        })
    }

    #[instrument(
        name = "upload.process_upload",
        skip(self, stream),
        fields(
            total_size,
            filename,
            account_id = ?account_id,
        )
    )]
    /// Process the upload of a file and its distribution to miners.
    pub(crate) async fn process_upload<S, E>(
        &self,
        stream: S,
        total_size: u64,
        filename: String,
        account_id: AccountId,
        signature: KeypairSignature,
    ) -> Result<String>
    where
        S: Stream<Item = Result<Bytes, E>> + Send + Unpin + 'static,
        E: Into<Box<dyn std::error::Error + Send + Sync>> + 'static,
    {
        self.metrics.upload_requests.add(1, &[]);

        info!("Processing upload of size {} bytes...", total_size);

        self.metrics.bytes_received.add(total_size, &[]);
        if self.miner_connections.is_empty() {
            return Err(anyhow::anyhow!("No active miner connections available"));
        }

        let chunk_size = piece_length(total_size, None, None);
        let (tx, rx) = mpsc::channel::<(u64, Vec<u8>)>(chunk_size as usize);
        let metrics_cloned = self.metrics.clone();
        // Spawn producer task
        let producer_handle = {
            let stream = Box::pin(stream);
            tokio::spawn(async move {
                if let Err(e) =
                    produce_bytes(stream, tx, total_size, chunk_size as usize, metrics_cloned).await
                {
                    error!("Producer error: {}", e);
                    return Err(e);
                }
                Ok(())
            })
        };

        // get nonce from MetadatDB
        let nonce = match metadata::db::MetadataDB::get_nonce(
            &self.state.validator.metadatadb_sender,
            account_id.clone(),
        )
        .await
        .map_err(|e| anyhow::anyhow!(e.to_string()))
        {
            Ok(nonce) => nonce,
            Err(e) => {
                error!("Failed to get nonce: {}", e);
                return Err(e);
            }
        };

        // Spawn consumer task
        let miner_uids = self.miner_uids.clone();
        let miner_connections = self.miner_connections.clone();
        let validator = self.state.validator.clone();
        let validator_read_guard = validator.neuron.read().await;
        let metadatadb_sender = validator.metadatadb_sender.clone();
        let metadatadb_sender_clone = metadatadb_sender.clone();

        // Get MemoryDB from state
        let scoring_system = validator.scoring_system.clone();
        let metrics_cloned = self.metrics.clone();
        // Cloning again to avoid borrowing issues
        let validator_clone = validator.clone();
        let consumer_handle = tokio::spawn(async move {
            consume_bytes(
                validator_clone.neuron.clone(),
                scoring_system,
                metadatadb_sender_clone,
                rx,
                miner_uids,
                miner_connections,
                metrics_cloned,
            )
            .await
        });

        drop(validator_read_guard);

        let now = Utc::now();

        // Wait for both tasks to complete
        let (_producer_result, consumer_result) = tokio::join!(producer_handle, consumer_handle);

        let (chunks_with_pieces, piece_hashes) = consumer_result??;

        let infohash: InfoHash = get_infohash_by_identity(piece_hashes, &account_id);
        // convert the infohash to a hex string for logging
        let infohash_str = hex::encode(infohash);

        // Create signature for the infohash value with nonce
        let infohash_value = metadata::models::InfohashValue {
            infohash,
            name: filename.clone(),
            length: total_size,
            chunk_size,
            chunk_count: chunks_with_pieces.len() as u64,
            owner_account_id: SqlAccountId(account_id.clone()),
            creation_timestamp: now,
            signature: KeypairSignature::from_raw([0; 64]), // Placeholder, will be signed below
        };

        let signed_infohash_value = metadata::models::InfohashValue {
            signature,
            ..infohash_value
        };

        // display chunks_with_pieces
        debug!("Chunks with pieces: {:?}", chunks_with_pieces);

        // display hash of chunks_with_pieces
        let serialized = bincode::serialize(&chunks_with_pieces)?;
        debug!(
            "HASH: {}",
            hex::encode(blake3::hash(&serialized).as_bytes())
        );

        match metadata::db::MetadataDB::insert_object(
            &metadatadb_sender,
            signed_infohash_value,
            &nonce,
            chunks_with_pieces,
        )
        .await
        {
            Ok(_) => debug!("Successfully inserted object into local database"),
            Err(e) => {
                error!("Failed to insert object into local database: {}", e);
                return Err(e.into());
            }
        }
        debug!("Inserted object entry with infohash {infohash_str} into local database");

        Ok(infohash_str)
    }
}

#[instrument(
    name = "upload.produce_bytes",
    skip(stream, tx),
    fields(total_size, chunk_size,)
)]
async fn produce_bytes<S, E>(
    stream: S,
    tx: mpsc::Sender<(u64, Vec<u8>)>,
    total_size: u64,
    chunk_size: usize,
    metrics: Arc<UploadMetrics>,
) -> Result<()>
where
    S: Stream<Item = Result<Bytes, E>> + Unpin,
    E: Into<Box<dyn std::error::Error + Send + Sync>>,
{
    let mut total_processed: u64 = 0;
    let mut buffer = Vec::with_capacity(chunk_size);

    let mut stream = stream.map_err(|e| anyhow::anyhow!("Stream error: {}", e.into()));
    let mut chunk_idx: u64 = 0;

    while let Some(chunk_result) = stream.try_next().await? {
        buffer.extend_from_slice(&chunk_result);
        let n = chunk_result.len() as u64;
        metrics.bytes_received.add(n, &[]);
        // Process full chunks
        while buffer.len() >= chunk_size {
            let chunk_data = buffer.drain(..chunk_size).collect::<Vec<u8>>();
            total_processed += chunk_data.len() as u64;

            if total_processed > total_size {
                return Err(anyhow::anyhow!("Upload size exceeds expected size"));
            }

            if total_processed.is_multiple_of(LOGGING_INTERVAL_MB * BYTES_PER_MB) {
                info!(
                    "Processing: {} MB / {} MB",
                    total_processed / (1024 * 1024),
                    total_size / (1024 * 1024)
                );
            }

            tx.send((chunk_idx, chunk_data)).await?;
            chunk_idx += 1;
        }
    }

    // Send any remaining data
    if !buffer.is_empty() {
        total_processed += buffer.len() as u64;
        // TODO: is sending the chunk idx and remaining buffer correct here?
        tx.send((chunk_idx, buffer)).await?;
    }

    info!("Total bytes processed: {}", total_processed);
    Ok(())
}

#[instrument(
    name = "upload.consume_bytes",
    skip(validator_base_neuron, scoring_system, metadatadb_sender, rx),
    fields(
        miner_count = miner_connections.len(),
    )
)]
async fn consume_bytes(
    validator_base_neuron: Arc<RwLock<BaseNeuron>>,
    scoring_system: Arc<RwLock<ScoringSystem>>,
    metadatadb_sender: mpsc::Sender<metadata::db::MetadataDBCommand>,
    mut rx: mpsc::Receiver<(u64, Vec<u8>)>,
    miner_uids: Vec<NodeUID>,
    miner_connections: Vec<(SocketAddr, Connection)>,
    metrics: Arc<UploadMetrics>,
) -> Result<(
    Vec<(
        metadata::models::ChunkValue,
        Vec<metadata::models::PieceValue>,
    )>,
    Vec<PieceHash>,
)> {
    let mut chunks_with_pieces: Vec<(
        metadata::models::ChunkValue,
        Vec<metadata::models::PieceValue>,
    )> = Vec::new();

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
    let miner_uids: Vec<NodeUID> = indices.iter().map(|&i| miner_uids[i]).collect();

    while let Some((chunk_idx, chunk)) = rx.recv().await {
        let chunk_start = Instant::now();
        metrics.chunks_processed.add(1, &[]);

        // Encode the chunk using FEC
        let encoded = encode_chunk(&chunk, chunk_idx);
        let target_piece_count = encoded.pieces.len();
        // Create filled vector of length target_piece_count
        let mut chunk_piece_hashes: Vec<PieceHash> = vec![PieceHash([0; 32]); target_piece_count];
        let mut chunk_hash_raw = blake3::Hasher::new();

        // Distribute pieces to miners
        let mut futures = Vec::new();
        let successful_pieces = Arc::new(RwLock::new(HashSet::new()));
        let upload_count = Arc::new(AtomicUsize::new(0));

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
                let metadatadb_sender_clone = metadatadb_sender.clone();

                let piece_data = piece.clone();
                let metrics_clone = metrics.clone();
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
                            Some(metadatadb_sender_clone),
                            Some(metrics_clone)
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
        let mut miners_for_piece: HashMap<PieceHash, Vec<NodeUID>> = HashMap::new();
        // Process successful uploads
        for result in results {
            match result {
                // TODO: remove unused variables from the match arm?
                Ok(Ok(Some((piece_idx, _, piece_hash, miner_uid)))) => {
                    // Process successful upload
                    chunk_hash_raw.update(&piece_hash.0);
                    chunk_piece_hashes[piece_idx as usize] = piece_hash;

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

        let chunk_value = metadata::models::ChunkValue {
            chunk_hash: ChunkHash(*chunk_hash),
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
            .map(|(piece_idx, piece)| metadata::models::PieceValue {
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

        let chunk_with_pieces = (chunk_value.clone(), chunk_piece_values.clone());
        chunks_with_pieces.push(chunk_with_pieces);
        let elapsed_ms = chunk_start.elapsed().as_secs_f64() * 1_000.0;
        metrics.chunk_processing_time.record(elapsed_ms, &[]);
    }

    // get pieces from chunks_with_pieces
    let piece_hashes: Vec<PieceHash> = chunks_with_pieces
        .iter()
        .flat_map(|(_, pieces)| pieces.iter().map(|p| p.piece_hash))
        .collect();

    Ok((chunks_with_pieces, piece_hashes))
}

/// Upload a piece to a miner and verify the upload by checking the hash, then update the miner's stats
#[instrument(
    name = "upload.upload_piece_to_miner",
    skip(validator_base_neuron, conn, scoring_system, metadatadb_sender),
    fields(
        miner_uid = ?miner_info.neuron_info.uid,
        piece_idx = piece.piece_idx,
    )
)]
pub async fn upload_piece_to_miner(
    validator_base_neuron: Arc<RwLock<BaseNeuron>>,
    miner_info: NodeInfo,
    conn: &Connection,
    piece: base::piece::Piece,
    scoring_system: Arc<RwLock<ScoringSystem>>,
    metadatadb_sender: Option<mpsc::Sender<metadata::db::MetadataDBCommand>>,
    metrics: Option<Arc<UploadMetrics>>,
) -> Result<(PieceHash, NodeUID)> {
    let piece_hash = PieceHash(*blake3::hash(&piece.data).as_bytes());

    // If there is a metadatadb_sender, check if the piece is already uploaded
    if let Some(metadatadb_sender) = &metadatadb_sender {
        // Check if the piece is already uploaded
        // if trying to get_piece returns an error, it means that the piece is not in the database
        match metadata::db::MetadataDB::get_piece(metadatadb_sender, piece_hash).await {
            Ok(_) => {
                // Piece already exists, no need to upload
                debug!(
                    "Piece with hash {} already exists, skipping upload",
                    hex::encode(piece_hash)
                );
                return Ok((piece_hash, miner_info.neuron_info.uid));
            }
            Err(e) => {
                // We assume that the piece does not exist if we get an error
                debug!(
                    "Piece with hash {} not found in database, proceeding with upload: {}",
                    hex::encode(piece_hash),
                    e
                );
            }
        };
    }

    let miner_uid = miner_info.neuron_info.uid;
    if let Some(ref metrics) = metrics {
        metrics
            .pieces_uploaded
            .add(1, &[KeyValue::new("miner_uid", miner_uid as f64)]);
    }

    let upload_start = Instant::now();
    let upload_result =
        upload_piece_data(validator_base_neuron.clone(), miner_info, conn, piece.data).await?;
    let elapsed_ms = upload_start.elapsed().as_secs_f64() * 1_000.0;
    if let Some(ref metrics) = metrics {
        metrics
            .piece_processing_time
            .record(elapsed_ms, &[KeyValue::new("miner_uid", miner_uid as f64)]);
    }

    // Verification: Check if the hash received from the miner is the same
    // as the hash of the piece and update the stats of the miner in the database accordingly

    // log hash that we get vs. hash that we expect
    info!(
        "Hash from miner: {}, Expected hash: {}",
        hex::encode(upload_result.as_bytes_ref()),
        hex::encode(piece_hash)
    );

    // Update stats
    let db = scoring_system.write().await.db.clone();
    db.conn.lock().await.execute(
        "UPDATE miner_stats SET store_attempts = store_attempts + 1 WHERE miner_uid = ?",
        [miner_uid],
    )?;

    if upload_result.as_bytes_ref() == piece_hash.0 {
        db.conn.lock().await.execute(
            "UPDATE miner_stats SET store_successes = store_successes + 1, total_successes = total_successes + 1 WHERE miner_uid = ?",
            [miner_uid],
        )?;
        let mut scoring_system_guard = scoring_system.write().await;
        scoring_system_guard
            .update_alpha_beta_db(miner_uid, 1.0, true)
            .await?;
        if let Some(ref metrics) = metrics {
            metrics
                .pieces_succeeded
                .add(1, &[KeyValue::new("miner_uid", miner_uid as f64)]);
        }
    } else {
        let mut scoring_system_guard = scoring_system.write().await;
        scoring_system_guard
            .update_alpha_beta_db(miner_uid, 1.0, false)
            .await?;
        if let Some(ref metrics) = metrics {
            metrics
                .pieces_failed
                .add(1, &[KeyValue::new("miner_uid", miner_uid as f64)]);
        }
    }

    Ok((piece_hash, miner_uid))
}
