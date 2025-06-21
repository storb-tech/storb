use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::anyhow;
use base::constants::MIN_BANDWIDTH;
use base::piece::{
    encode_chunk, get_infohash_with_identity, piece_length, ChunkHash, Piece, PieceHash,
};
use base::utils::multiaddr_to_socketaddr;
use base::verification::{HandshakePayload, KeyRegistrationInfo, VerificationMessage};
use base::{BaseNeuron, BaseNeuronConfig, NeuronError, NodeUID};
use chrono::Utc;
use crabtensor::sign::{sign_message, KeypairSignature};
use crabtensor::wallet::Signer;
use crabtensor::weights::set_weights_payload;
use crabtensor::weights::NormalizedWeight;
use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::Multiaddr;
use ndarray::Array1;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use reqwest::StatusCode;
use subxt::ext::codec::Compact;
use tokio::sync::{mpsc, RwLock};
use tokio::task;
use tracing::{debug, error, info, warn};

use crate::constants::{
    MAX_CHALLENGE_PIECE_NUM, MAX_SYNTH_CHALLENGE_MINER_NUM, MAX_SYNTH_CHUNK_SIZE,
    MIN_SYNTH_CHUNK_SIZE, SYNTH_CHALLENGE_TIMEOUT, SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE,
};
use crate::metadata;
use crate::metadata::models::SqlAccountId;
use crate::quic::{create_quic_client, make_client_endpoint};
use crate::scoring::{normalize_min_max, ScoringSystem};
use crate::upload::upload_piece_to_miner;
use crate::utils::{generate_synthetic_data, get_id_quic_uids};

#[derive(Debug)]
// TODO: remove latency and bytes_len from ChallengeResult?
#[allow(dead_code)]
struct ChallengeResult {
    miner_uid: NodeUID,
    latency: f64,
    bytes_len: usize,
    piece_hash: PieceHash,
    success: bool,
    error: Option<String>, // Add error field to track failure reasons
}

#[derive(Clone, Debug)]
pub struct ValidatorConfig {
    pub scores_state_file: PathBuf,
    pub crsqlite_file: PathBuf,
    pub sync_stake_threshold: u64,
    pub api_keys_db: PathBuf,
    pub neuron_config: BaseNeuronConfig,
    pub otel_api_key: String,
    pub otel_service_name: String,
    pub otel_endpoint: String,
}

/// The Storb validator.
#[derive(Clone)]
pub struct Validator {
    pub config: ValidatorConfig,
    pub neuron: Arc<RwLock<BaseNeuron>>,
    pub scoring_system: Arc<RwLock<ScoringSystem>>,
    pub metadatadb_sender: mpsc::Sender<metadata::db::MetadataDBCommand>,
}

impl Validator {
    pub async fn new(config: ValidatorConfig) -> Result<Self, NeuronError> {
        let neuron_config = config.neuron_config.clone();

        let scoring_system = Arc::new(RwLock::new(
            ScoringSystem::new(&config.neuron_config.db_file, &config.scores_state_file)
                .await
                .map_err(|err| NeuronError::ConfigError(err.to_string()))?,
        ));

        let neuron = Arc::new(RwLock::new(BaseNeuron::new(neuron_config).await?));

        info!("Validator initialized with config: {:?}", config);
        // TODO(metadatadb): use config varibable to get path for crsqlite library?
        let (mut metadatadb, metadatadb_sender) = metadata::db::MetadataDB::new(
            &config.neuron_config.metadatadb_file,
            &PathBuf::from(&config.crsqlite_file),
        )
        .map_err(|e| NeuronError::ConfigError(e.to_string()))?;
        info!("MetadataDB initialized");

        // spawn task to process metadata commands
        tokio::spawn(async move {
            let _ = metadatadb.process_events().await;
        });

        let validator = Validator {
            config,
            neuron,
            scoring_system,
            metadatadb_sender,
        };
        Ok(validator)
    }

    pub async fn run_synthetic_challenges(
        &self,
    ) -> Result<(), Box<dyn std::error::Error + std::marker::Send + Sync>> {
        let (peer_count, signer) = {
            let neuron_guard = self.neuron.read().await;
            (
                neuron_guard.address_book.clone().len(),
                self.neuron.read().await.signer.clone(),
            )
        };

        if peer_count == 0 {
            info!("No peers available, skipping synthetic challenges");
            return Ok(());
        }

        let mut rng: StdRng = SeedableRng::from_entropy();

        // ----==== Synthetic store challenges ====----

        info!("Running store retrieval challenges");

        let synth_size = rng.gen_range(MIN_SYNTH_CHUNK_SIZE..MAX_SYNTH_CHUNK_SIZE + 1);
        // TODO: generate synthetic data separately and in its own interval
        // - we want to scale back generation of synthetic data if there is enough organic data
        let synthetic_chunk = generate_synthetic_data(synth_size);

        let encoded = encode_chunk(&synthetic_chunk, 0);

        let mut store_futures = FuturesUnordered::new();

        // random .bin filename
        let filename = format!("chunk_{}.bin", rng.gen::<u64>());

        let now = Utc::now();

        let metadatadb_sender = self.metadatadb_sender.clone();

        // get piece hashes
        let piece_hashes = encoded
            .pieces
            .iter()
            .map(|piece| PieceHash(blake3::hash(&piece.data).as_bytes().to_owned()))
            .collect::<Vec<_>>();
        // Get the validator's account ID for the infohash
        let validator_account_id = signer.account_id().clone();

        // Generate a nonce for this upload operation
        let nonce = match metadata::db::MetadataDB::generate_nonce(
            &self.metadatadb_sender,
            validator_account_id.clone(),
        )
        .await
        {
            Ok(nonce) => nonce,
            Err(e) => {
                error!("Failed to generate nonce: {}", e);
                return Err(anyhow::anyhow!("Failed to generate nonce: {}", e).into());
            }
        };

        let infohash = get_infohash_with_identity(piece_hashes, &validator_account_id);

        let infohash_value = metadata::models::InfohashValue {
            infohash,
            name: filename,
            length: 69420, // placeholder for total file size
            chunk_size: encoded.chunk_size,
            chunk_count: 69420, // placeholder for chunk count
            owner_account_id: SqlAccountId(validator_account_id),
            creation_timestamp: now,
            // create dummy signature
            signature: KeypairSignature::from_raw([0; 64]),
        };

        // Sign the infohash value (which now includes the nonce)
        let message = infohash_value.get_signature_message(&nonce);
        let signature = sign_message(&signer, message);

        let infohash_value = metadata::models::InfohashValue {
            signature,
            ..infohash_value
        };

        for piece in encoded.pieces.clone() {
            let piece_hash_raw = blake3::hash(&piece.data);
            let piece_hash = piece_hash_raw.as_bytes();

            let vali_arc = Arc::new(self.clone());
            let (_, quic_addresses, quic_miner_uids) = get_id_quic_uids(vali_arc.clone()).await;
            let num_miners_to_query =
                std::cmp::min(MAX_SYNTH_CHALLENGE_MINER_NUM, quic_miner_uids.len());
            let idxs_to_query: Vec<usize> =
                (0..quic_miner_uids.len()).choose_multiple(&mut rng, num_miners_to_query);

            for idx in idxs_to_query {
                let quic_addr = quic_addresses[idx].clone();
                let quic_miner_uid = quic_miner_uids[idx];

                let neuron_guard = self.neuron.read().await;

                let miner_info = neuron_guard
                    .address_book
                    .clone()
                    .get(&quic_miner_uid)
                    .ok_or("No NodeInfo found for the given miner")?
                    .clone();
                drop(neuron_guard);

                let validator = self.clone();
                let piece_hash_copy = PieceHash(piece_hash.to_owned());

                let piece_clone = piece.clone();
                store_futures.push(task::spawn(async move {
                    validator
                        .run_store_challenge(
                            quic_miner_uid,
                            quic_addr,
                            piece_clone,
                            piece_hash_copy,
                            miner_info,
                        )
                        .await
                }));
            }
        }

        let mut miners_for_piece_hash: HashMap<PieceHash, Vec<Compact<NodeUID>>> = HashMap::new();
        while let Some(result) = store_futures.next().await {
            match result {
                Ok(Ok(challenge_result)) => {
                    if challenge_result.success {
                        info!(
                            "Store challenge succeeded for miner {}",
                            challenge_result.miner_uid
                        );
                        // add the miner uid to the miners_for_piece_hash
                        miners_for_piece_hash
                            .entry(challenge_result.piece_hash)
                            .or_default()
                            .push(Compact(challenge_result.miner_uid));
                    } else {
                        error!(
                            "Store challenge failed for miner {}: {}",
                            challenge_result.miner_uid,
                            challenge_result
                                .error
                                .unwrap_or_else(|| "Unknown error".to_string())
                        );
                    }
                }
                Ok(Err(e)) => error!("Store challenge error: {}", e),
                Err(e) => error!("Store challenge task error: {}", e),
            }
        }

        let mut chunk_hash_raw = blake3::Hasher::new();
        let mut chunk_piece_hashes = Vec::new();

        // Calculate chunk hash and collect piece hashes
        for piece in &encoded.pieces {
            let piece_hash = blake3::hash(&piece.data).as_bytes().to_owned();
            chunk_hash_raw.update(&piece_hash);
            chunk_piece_hashes.push(piece_hash);
        }

        let chunk_hash = ChunkHash(chunk_hash_raw.finalize().as_bytes().to_owned());
        // let chunk_key = libp2p::kad::RecordKey::new(chunk_hash.as_bytes());

        let validator_id = {
            let neuron_guard = self.neuron.read().await;
            neuron_guard
                .local_node_info
                .uid
                .ok_or("Failed to get validator UID")?
        };

        let vali_id_compact = Compact(validator_id);

        let chunk_value = metadata::models::ChunkValue {
            chunk_hash,
            k: encoded.k,
            m: encoded.m,
            chunk_size: encoded.chunk_size,
            padlen: encoded.padlen,
            original_chunk_size: encoded.original_chunk_size,
        };

        // Create piece values
        let piece_values: Vec<metadata::models::PieceValue> = encoded
            .pieces
            .iter()
            .map(|piece| {
                let piece_hash = PieceHash(blake3::hash(&piece.data).as_bytes().to_owned());
                metadata::models::PieceValue {
                    piece_hash,
                    piece_size: piece.data.len() as u64,
                    piece_type: piece.piece_type.clone(),
                    miners: miners_for_piece_hash
                        .get(&piece_hash)
                        .cloned()
                        .unwrap_or_else(|| vec![vali_id_compact]),
                }
            })
            .collect();

        let chunks_with_pieces: Vec<(
            metadata::models::ChunkValue,
            Vec<metadata::models::PieceValue>,
        )> = vec![(chunk_value, piece_values.clone())];

        // attempt to insert object, show success message or error message
        match metadata::db::MetadataDB::insert_object(
            &metadatadb_sender,
            infohash_value,
            &nonce,
            chunks_with_pieces,
        )
        .await
        {
            Ok(_) => {
                info!("Inserted synthetic chunk into MetadataDB");
            }
            Err(e) => {
                error!("Failed to insert synthetic chunk into MetadataDB: {}", e);
                return Err(anyhow::anyhow!(
                    "Failed to insert synthetic chunk into MetadataDB: {}",
                    e
                )
                .into());
            }
        }
        // wait a few seconds before running retrieval challenges
        tokio::time::sleep(Duration::from_secs_f64(
            SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE,
        ))
        .await;

        info!("Completed synthetic store challenges");

        // ----==== Synthetic retrieval challenges ====----

        info!("Running synthetic retrieval challenges");

        // pick pieces, ask some of the miners that have the pieces
        let chunk_entry = match metadata::db::MetadataDB::get_random_chunk(&metadatadb_sender).await
        {
            Ok(chunk) => chunk,
            Err(e) => {
                error!("Failed to get random chunk: {}", e);
                return Err(anyhow::anyhow!("Failed to get random chunk: {}", e).into());
            }
        };

        debug!("Selected chunk key: {:?}", chunk_entry);

        let challenge_pieces = match metadata::db::MetadataDB::get_pieces_by_chunk(
            &metadatadb_sender,
            chunk_entry.chunk_hash,
        )
        .await
        {
            Ok(pieces) => pieces,
            Err(e) => {
                error!("Failed to get pieces for chunk: {}", e);
                return Err(anyhow::anyhow!("Failed to get pieces for chunk: {}", e).into());
            }
        };

        let mut pieces_checked = 0;

        let address_book = {
            let neuron_guard = self.neuron.read().await;
            neuron_guard.address_book.clone()
        };

        let mut retrieval_futures = FuturesUnordered::new();

        while pieces_checked < MAX_CHALLENGE_PIECE_NUM {
            let idx = rng.gen_range(0..challenge_pieces.len());

            let chall_piece_hash = challenge_pieces[idx].piece_hash;

            let piece_miners = challenge_pieces[idx].miners.clone();

            if piece_miners.is_empty() {
                // return Err(anyhow!("No providers found for piece {:?}", piece_key).into());
                warn!("No miners found for piece {:?}", chall_piece_hash);
                pieces_checked += 1;
                continue;
            }
            // go through bimap, get QUIC addresses of miners
            for miner_uid in piece_miners {
                let miner_uid = miner_uid.0;
                let miner_info = if let Some(miner_info) = address_book.get(&miner_uid) {
                    miner_info.clone()
                } else {
                    warn!("Miner info not found in address book for uid {}", miner_uid);
                    continue;
                };

                let miner_info_clone = miner_info.clone();
                let signer_clone = signer.clone();

                // TODO: is cloning here the best way to do this?
                let validator = self.clone();
                let piece_size = piece_length(chunk_entry.chunk_size, None, None);
                retrieval_futures.push(task::spawn(async move {
                    validator
                        .run_retrieval_challenge(
                            miner_uid,
                            chall_piece_hash,
                            piece_size as usize,
                            miner_info_clone,
                            signer_clone,
                            validator_id,
                        )
                        .await
                }));
            }

            pieces_checked += 1;
        }

        while let Some(result) = retrieval_futures.next().await {
            match result {
                Ok(Ok(challenge_result)) => {
                    let db = self.scoring_system.write().await.db.clone();

                    // Update attempts count
                    if let Err(e) = db.conn.lock().await.execute(
                        "UPDATE miner_stats SET retrieval_attempts = retrieval_attempts + 1 WHERE miner_uid = ?",
                        [challenge_result.miner_uid],
                    ) {
                        error!("Failed to update retrieval attempts: {}", e);
                    }

                    if challenge_result.success {
                        info!(
                            "Retrieval challenge succeeded for miner {}",
                            challenge_result.miner_uid
                        );
                        // Update success count
                        if let Err(e) = db.conn.lock().await.execute(
                            "UPDATE miner_stats SET retrieval_successes = retrieval_successes + 1, total_successes = total_successes + 1 WHERE miner_uid = ?",
                            [challenge_result.miner_uid],
                        ) {
                            error!("Failed to update retrieval successes: {}", e);
                        }
                        let mut scoring_system_guard = self.scoring_system.write().await;
                        scoring_system_guard
                            .update_alpha_beta_db(
                                challenge_result.miner_uid,
                                challenge_result.latency,
                                true,
                            )
                            .await
                            .unwrap_or_else(|e| {
                                error!("Failed to update alpha/beta in DB: {}", e);
                            });
                    } else {
                        error!(
                            "Retrieval challenge failed for miner {}: {}",
                            challenge_result.miner_uid,
                            challenge_result
                                .error
                                .unwrap_or_else(|| "Unknown error".to_string())
                        );
                        let mut scoring_system_guard = self.scoring_system.write().await;
                        scoring_system_guard
                            .update_alpha_beta_db(
                                challenge_result.miner_uid,
                                challenge_result.latency,
                                false,
                            )
                            .await
                            .unwrap_or_else(|e| {
                                error!("Failed to update alpha/beta in DB: {}", e);
                            });
                    }
                }
                Ok(Err(e)) => error!("Retrieval challenge error: {}", e),
                Err(e) => error!("Retrieval challenge task error: {}", e),
            }
        }

        Ok(())
    } // This closes the run_synthetic_challenges method

    /// Set weights for each miner to publish to the chain.
    pub async fn set_weights(
        neuron: BaseNeuron,
        scores: Array1<f64>,
        config: ValidatorConfig,
        weights_counter: opentelemetry::metrics::Counter<f64>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let normed_scores = normalize_min_max(&scores);
        debug!("weights: {:?}", normed_scores);
        // Convert normalized scores to Vec<NormalizedWeight>
        let weights: Vec<NormalizedWeight> = normed_scores
            .iter()
            .enumerate()
            .map(|(uid, &score)| NormalizedWeight {
                uid: uid as NodeUID,
                weight: (score * u16::MAX as f64) as u16,
            })
            .collect();

        for weight in &weights {
            weights_counter.add(
                weight.weight as f64,
                &[opentelemetry::KeyValue::new("miner_uid", weight.uid as i64)],
            );
        }
        let payload = set_weights_payload(config.neuron_config.netuid, weights, 1);

        let subtensor = BaseNeuron::get_subtensor_connection(
            neuron.config.subtensor.insecure,
            &neuron.config.subtensor.address,
        )
        .await?;
        subtensor
            .tx()
            .sign_and_submit_default(&payload, &neuron.signer)
            .await?;

        drop(neuron);

        Ok(())
    }

    async fn run_store_challenge(
        &self,
        miner_uid: NodeUID,
        quic_addr: Multiaddr,
        piece: Piece,
        piece_hash: PieceHash,
        miner_info: base::NodeInfo,
    ) -> Result<ChallengeResult, Box<dyn std::error::Error + Send + Sync>> {
        let client = make_client_endpoint(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
            .map_err(|e| anyhow!("Failed to create client endpoint: {}", e))?;

        let socket_addr =
            multiaddr_to_socketaddr(&quic_addr).ok_or_else(|| anyhow!("Invalid QUIC address"))?;

        let start_time = Instant::now();

        let piece_len = piece.data.len();

        let quic_conn = match create_quic_client(&client, socket_addr).await {
            Ok(conn) => conn,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    piece_hash,
                    success: false,
                    error: Some(format!("QUIC connection failed: {}", e)),
                })
            }
        };

        let scoring_clone = self.scoring_system.clone();

        let (hash, _) = match upload_piece_to_miner(
            self.neuron.clone(),
            miner_info,
            &quic_conn,
            piece,
            scoring_clone,
            None,
        )
        .await
        {
            Ok(h) => h,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: piece_len,
                    piece_hash,
                    success: false,
                    error: Some(format!("Upload failed: {}", e)),
                })
            }
        };

        let latency = start_time.elapsed().as_secs_f64();
        let success = hash == piece_hash;

        Ok(ChallengeResult {
            miner_uid,
            latency: if success {
                latency
            } else {
                SYNTH_CHALLENGE_TIMEOUT
            },
            bytes_len: piece_len,
            piece_hash,
            success,
            error: if !success {
                Some("Hash mismatch".to_string())
            } else {
                None
            },
        })
    }

    async fn run_retrieval_challenge(
        &self,
        miner_uid: NodeUID,
        piece_hash: PieceHash,
        piece_size: usize,
        miner_info: base::NodeInfo,
        signer: Signer,
        validator_id: NodeUID,
    ) -> Result<ChallengeResult, Box<dyn std::error::Error + Send + Sync>> {
        let message = VerificationMessage {
            netuid: self.config.neuron_config.netuid,
            miner: KeyRegistrationInfo {
                uid: miner_uid,
                account_id: miner_info.neuron_info.hotkey.clone(),
            },
            validator: KeyRegistrationInfo {
                uid: validator_id,
                account_id: signer.account_id().clone(),
            },
        };

        let piece_len = piece_size;

        let signature = sign_message(&signer, &message);
        let payload = HandshakePayload { signature, message };
        let payload_bytes = match bincode::serialize(&payload) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    piece_hash,
                    success: false,
                    error: Some(format!("Serialization failed: {}", e)),
                })
            }
        };

        let url = match miner_info
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
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    piece_hash,
                    success: false,
                    error: Some("Invalid HTTP address".to_string()),
                })
            }
        };

        let min_bandwidth = MIN_BANDWIDTH as f64;
        let timeout_duration = Duration::from_secs_f64(piece_size as f64 / min_bandwidth);

        let req_client = reqwest::Client::builder()
            .timeout(timeout_duration)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;

        // log timeout
        debug!(
            "Timeout duration for retrieval challenge: {} milliseconds",
            timeout_duration.as_millis()
        );

        let start_time = Instant::now();

        // Handle all possible failure points in the request chain
        let response = match req_client.get(&url).send().await {
            Ok(resp) => resp,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    piece_hash,
                    success: false,
                    error: Some(format!("Request failed: {}", e)),
                })
            }
        };

        let latency = start_time.elapsed().as_secs_f64();

        if response.status() != StatusCode::OK {
            return Ok(ChallengeResult {
                miner_uid,
                latency: SYNTH_CHALLENGE_TIMEOUT,
                bytes_len: 0,
                piece_hash,
                success: false,
                error: Some(format!("Bad status code: {}", response.status())),
            });
        }

        let body_bytes = match response.bytes().await {
            Ok(bytes) => bytes,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    piece_hash,
                    success: false,
                    error: Some(format!("Failed to read response body: {}", e)),
                })
            }
        };

        let piece_data = match base::piece::deserialise_piece_response(&body_bytes, &piece_hash) {
            Ok(data) => data,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    piece_hash,
                    success: false,
                    error: Some(format!("Failed to deserialize piece: {}", e)),
                })
            }
        };

        let computed_hash = blake3::hash(&piece_data);
        let success = PieceHash(*computed_hash.as_bytes()) == piece_hash;

        Ok(ChallengeResult {
            miner_uid,
            latency: if success {
                latency
            } else {
                SYNTH_CHALLENGE_TIMEOUT
            },
            bytes_len: piece_len,
            piece_hash,
            success,
            error: if !success {
                Some("Hash verification failed".to_string())
            } else {
                None
            },
        })
    }
}
