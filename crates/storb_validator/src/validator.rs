use std::collections::HashMap;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::{anyhow, Context};
use base::constants::CLIENT_TIMEOUT;
use base::piece::{encode_chunk, Piece};
use base::swarm::models::ChunkDHTValue;
use base::utils::multiaddr_to_socketaddr;
use base::verification::{HandshakePayload, KeyRegistrationInfo, VerificationMessage};
use base::{swarm, BaseNeuron, BaseNeuronConfig, NeuronError};
use crabtensor::sign::sign_message;
use crabtensor::wallet::Signer;
use crabtensor::weights::set_weights_payload;
use crabtensor::weights::NormalizedWeight;
use futures::stream::{FuturesUnordered, StreamExt};
use libp2p::kad::RecordKey;
use libp2p::Multiaddr;
use ndarray::Array1;
use rand::rngs::StdRng;
use rand::seq::IteratorRandom;
use rand::{Rng, SeedableRng};
use reqwest::StatusCode;
use subxt::ext::codec::Compact;
use subxt::ext::sp_core::hexdisplay::AsBytesRef;
use tokio::sync::RwLock;
use tokio::task;
use tracing::{debug, error, info, warn};

use crate::constants::{
    MAX_CHALLENGE_PIECE_NUM, MAX_SYNTH_CHALLENGE_MINER_NUM, MAX_SYNTH_CHUNK_SIZE,
    MIN_SYNTH_CHUNK_SIZE, SYNTH_CHALLENGE_TIMEOUT, SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE,
};
use crate::quic::{create_quic_client, make_client_endpoint};
use crate::scoring::{normalize_min_max, select_random_chunk_from_db, ScoringSystem};
use crate::upload::upload_piece_to_miner;
use crate::utils::{generate_synthetic_data, get_id_quic_uids};

#[derive(Debug)]
struct ChallengeResult {
    miner_uid: u16,
    latency: f64,
    bytes_len: usize,
    success: bool,
    error: Option<String>, // Add error field to track failure reasons
}

#[derive(Default)]
struct LatencyStats {
    weighted_latency: f64,
    total_bytes: usize,
}

#[derive(Default)]
struct MinerLatencyMap {
    stats: HashMap<u16, LatencyStats>,
}

impl MinerLatencyMap {
    fn record_latency(&mut self, miner_uid: u16, latency: f64, bytes_len: usize) {
        let stats = self.stats.entry(miner_uid).or_default();
        stats.weighted_latency += latency * bytes_len as f64;
        stats.total_bytes += bytes_len;
    }

    fn get_all_latencies(&self) -> HashMap<u16, f64> {
        self.stats
            .iter()
            .map(|(&uid, stats)| {
                let avg = if stats.total_bytes > 0 {
                    stats.weighted_latency / stats.total_bytes as f64
                } else {
                    0.0
                };
                (uid, avg)
            })
            .collect()
    }
}

#[derive(Clone)]
pub struct ValidatorConfig {
    pub scores_state_file: PathBuf,
    pub moving_average_alpha: f64,
    pub api_keys_db: PathBuf,
    pub neuron_config: BaseNeuronConfig,
}

/// The Storb validator.
#[derive(Clone)]
pub struct Validator {
    pub config: ValidatorConfig,
    pub neuron: Arc<RwLock<BaseNeuron>>,
    pub scoring_system: Arc<RwLock<ScoringSystem>>,
}

impl Validator {
    pub async fn new(config: ValidatorConfig) -> Result<Self, NeuronError> {
        let neuron_config = config.neuron_config.clone();

        let scoring_system = Arc::new(RwLock::new(
            ScoringSystem::new(
                &config.neuron_config.db_file,
                &config.scores_state_file,
                config.moving_average_alpha,
            )
            .await
            .map_err(|err| NeuronError::ConfigError(err.to_string()))?,
        ));

        let neuron = Arc::new(RwLock::new(
            BaseNeuron::new(neuron_config, Some(scoring_system.read().await.db.clone())).await?,
        ));

        let validator = Validator {
            config,
            neuron,
            scoring_system,
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
        let mut store_latencies = MinerLatencyMap::default();
        let mut retrieval_latencies = MinerLatencyMap::default();

        // ----==== Synthetic store challenges ====----

        info!("Running store retrieval challenges");

        let synth_size = rng.gen_range(MIN_SYNTH_CHUNK_SIZE..MAX_SYNTH_CHUNK_SIZE + 1);
        // TODO: generate synthetic data separately and in its own interval
        // - we want to scale back generation of synthetic data if there is enough organic data
        let synthetic_chunk = generate_synthetic_data(synth_size);

        let encoded = encode_chunk(&synthetic_chunk, 0);

        let mut store_futures = FuturesUnordered::new();

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
                let peer_id = neuron_guard
                    .peer_node_uid
                    .get_by_right(&quic_miner_uid)
                    .ok_or("No peer ID found for the miner UID")?;

                let miner_info = neuron_guard
                    .address_book
                    .clone()
                    .get(peer_id)
                    .ok_or("No NodeInfo found for the given miner")?
                    .clone();
                drop(neuron_guard);

                let validator = self.clone();
                let piece_hash_copy = piece_hash.to_owned();

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

        while let Some(result) = store_futures.next().await {
            match result {
                Ok(Ok(challenge_result)) => {
                    let db = self.scoring_system.write().await.db.clone();

                    // Update attempts count
                    if let Err(e) = db.conn.lock().await.execute(
                        "UPDATE miner_stats SET store_attempts = store_attempts + 1 WHERE miner_uid = ?",
                        [challenge_result.miner_uid],
                    ) {
                        error!("Failed to update store attempts: {}", e);
                    }

                    if challenge_result.success {
                        info!(
                            "Store challenge succeeded for miner {}",
                            challenge_result.miner_uid
                        );
                        // Update success count
                        if let Err(e) = db.conn.lock().await.execute(
                            "UPDATE miner_stats SET store_successes = store_successes + 1, total_successes = total_successes + 1 WHERE miner_uid = ?",
                            [challenge_result.miner_uid],
                        ) {
                            error!("Failed to update store successes: {}", e);
                        }

                        store_latencies.record_latency(
                            challenge_result.miner_uid,
                            challenge_result.latency,
                            challenge_result.bytes_len,
                        );
                    } else {
                        error!(
                            "Store challenge failed for miner {}: {}",
                            challenge_result.miner_uid,
                            challenge_result
                                .error
                                .unwrap_or_else(|| "Unknown error".to_string())
                        );
                        store_latencies.record_latency(
                            challenge_result.miner_uid,
                            SYNTH_CHALLENGE_TIMEOUT,
                            challenge_result.bytes_len,
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

        let chunk_hash = chunk_hash_raw.finalize();
        let chunk_key = libp2p::kad::RecordKey::new(chunk_hash.as_bytes());

        // Create chunk DHT value
        let validator_id = {
            let neuron_guard = self.neuron.read().await;
            neuron_guard
                .local_node_info
                .uid
                .ok_or("Failed to get validator UID")?
        };

        let chunk_msg = (
            chunk_key.clone(),
            validator_id,
            chunk_piece_hashes.clone(),
            0, // chunk_idx for synthetic challenges is 0
            encoded.k,
            encoded.m,
            encoded.chunk_size,
            encoded.padlen,
            encoded.original_chunk_size,
        );

        let chunk_msg_bytes = bincode::serialize(&chunk_msg)?;
        let chunk_sig = sign_message(&signer, chunk_msg_bytes);

        let vali_id_compact = Compact(validator_id);

        let chunk_dht_value = ChunkDHTValue {
            chunk_hash: chunk_key.clone(),
            validator_id: vali_id_compact,
            piece_hashes: chunk_piece_hashes,
            chunk_idx: 0,
            k: encoded.k,
            m: encoded.m,
            chunk_size: encoded.chunk_size,
            padlen: encoded.padlen,
            original_chunk_size: encoded.original_chunk_size,
            signature: chunk_sig,
        };

        // Put chunk entry in DHT

        let neuron_guard = self.neuron.read().await;
        swarm::dht::StorbDHT::put_chunk_entry(
            neuron_guard.command_sender.clone(),
            chunk_key,
            chunk_dht_value,
        )
        .await?;
        drop(neuron_guard);

        // wait a few seconds before running retrieval challenges
        tokio::time::sleep(Duration::from_secs_f64(
            SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE,
        ))
        .await;

        info!("Inserted synthetic chunk into DHT");
        info!("Completed synthetic store challenges");

        // ----==== Synthetic retrieval challenges ====----

        info!("Running synthetic retrieval challenges");

        // pick pieces, ask some of the miners that have the pieces
        let db_conn = self.scoring_system.write().await.db.conn.clone();
        let chunk_key = select_random_chunk_from_db(db_conn).await?;
        debug!("Selected chunk key: {:?}", chunk_key);

        // request pieces
        let chunk_entry = swarm::dht::StorbDHT::get_chunk_entry(
            self.neuron.read().await.command_sender.clone(),
            chunk_key,
        )
        .await?
        .context("Chunk for the given record key was not found")?;
        let piece_hashes = chunk_entry.piece_hashes;

        let mut pieces_checked = 0;

        let (dht_sender, address_book) = {
            let neuron_guard = self.neuron.read().await;
            (
                neuron_guard.command_sender.clone(),
                neuron_guard.address_book.clone(),
            )
        };

        let mut retrieval_futures = FuturesUnordered::new();

        while pieces_checked < MAX_CHALLENGE_PIECE_NUM {
            let idx = rng.gen_range(0..piece_hashes.len());

            let piece_hash = piece_hashes[idx];
            let piece_key = RecordKey::new(&piece_hash);

            let piece_providers =
                swarm::dht::StorbDHT::get_piece_providers(&dht_sender, piece_key.clone()).await?;

            if piece_providers.is_empty() {
                // return Err(anyhow!("No providers found for piece {:?}", piece_key).into());
                warn!("No providers found for piece {:?}", piece_key);
                pieces_checked += 1;
                continue;
            }
            // go through bimap, get QUIC addresses of miners
            for peer_id in piece_providers {
                let miner_info = if let Some(miner_info) = address_book.get(&peer_id) {
                    miner_info.clone()
                } else {
                    warn!("Miner info not found in address book for peer {}", peer_id);
                    continue;
                };
                let miner_uid = miner_info.neuron_info.uid;

                let miner_info_clone = miner_info.clone();
                let signer_clone = signer.clone();

                // TODO: is cloning here the best way to do this?
                let validator = self.clone();
                retrieval_futures.push(task::spawn(async move {
                    validator
                        .run_retrieval_challenge(
                            miner_uid,
                            piece_hash,
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

                        retrieval_latencies.record_latency(
                            challenge_result.miner_uid,
                            challenge_result.latency,
                            challenge_result.bytes_len,
                        );
                    } else {
                        error!(
                            "Retrieval challenge failed for miner {}: {}",
                            challenge_result.miner_uid,
                            challenge_result
                                .error
                                .unwrap_or_else(|| "Unknown error".to_string())
                        );
                        retrieval_latencies.record_latency(
                            challenge_result.miner_uid,
                            SYNTH_CHALLENGE_TIMEOUT,
                            challenge_result.bytes_len,
                        );
                    }
                }
                Ok(Err(e)) => error!("Retrieval challenge error: {}", e),
                Err(e) => error!("Retrieval challenge task error: {}", e),
            }
        }

        let store_avg_latencies = store_latencies.get_all_latencies();
        let retrieval_avg_latencies = retrieval_latencies.get_all_latencies();
        let moving_average_alpha = { self.scoring_system.read().await.moving_average_alpha };

        // Update scoring system with new latencies using EMA
        self.scoring_system
            .write()
            .await
            .state
            .update_latency_scores(
                store_avg_latencies,
                retrieval_avg_latencies,
                moving_average_alpha,
            );

        // record latency and response rate scores
        Ok(())
    }

    /// Set weights for each miner to publish to the chain.
    pub async fn set_weights(
        neuron: BaseNeuron,
        scores: Array1<f64>,
        config: ValidatorConfig,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let normed_scores = normalize_min_max(&scores);
        debug!("weights: {:?}", normed_scores);

        // Convert normalized scores to Vec<NormalizedWeight>
        let weights: Vec<NormalizedWeight> = normed_scores
            .iter()
            .enumerate()
            .map(|(uid, &score)| NormalizedWeight {
                uid: uid as u16,
                weight: (score * u16::MAX as f64) as u16,
            })
            .collect();

        let payload = set_weights_payload(config.neuron_config.netuid, weights, 1);

        neuron
            .subtensor
            .tx()
            .sign_and_submit_default(&payload, &neuron.signer)
            .await?;

        drop(neuron);

        Ok(())
    }

    async fn run_store_challenge(
        &self,
        miner_uid: u16,
        quic_addr: Multiaddr,
        piece: Piece,
        piece_hash: [u8; 32],
        miner_info: base::NodeInfo,
    ) -> Result<ChallengeResult, Box<dyn std::error::Error + Send + Sync>> {
        let client = make_client_endpoint(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
            .map_err(|e| anyhow!("Failed to create client endpoint: {}", e))?;

        let socket_addr =
            multiaddr_to_socketaddr(&quic_addr).ok_or_else(|| anyhow!("Invalid QUIC address"))?;

        let start_time = Instant::now();

        let quic_conn = match create_quic_client(&client, socket_addr).await {
            Ok(conn) => conn,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    success: false,
                    error: Some(format!("QUIC connection failed: {}", e)),
                })
            }
        };

        let piece_len = piece.data.len();
        let scoring_clone = self.scoring_system.clone();

        let hash = match upload_piece_to_miner(
            self.neuron.clone(),
            miner_info,
            &quic_conn,
            piece,
            scoring_clone,
        )
        .await
        {
            Ok(h) => h,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: piece_len,
                    success: false,
                    error: Some(format!("Upload failed: {}", e)),
                })
            }
        };

        let latency = start_time.elapsed().as_secs_f64();
        let success = hash.as_bytes_ref() == piece_hash;

        Ok(ChallengeResult {
            miner_uid,
            latency: if success {
                latency
            } else {
                SYNTH_CHALLENGE_TIMEOUT
            },
            bytes_len: piece_len,
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
        miner_uid: u16,
        piece_hash: [u8; 32],
        miner_info: base::NodeInfo,
        signer: Signer,
        validator_id: u16,
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

        let signature = sign_message(&signer, &message);
        let payload = HandshakePayload { signature, message };
        let payload_bytes = match bincode::serialize(&payload) {
            Ok(bytes) => bytes,
            Err(e) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
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
                    success: false,
                    error: Some("Invalid HTTP address".to_string()),
                })
            }
        };

        let req_client = reqwest::Client::builder()
            .timeout(CLIENT_TIMEOUT)
            .build()
            .map_err(|e| anyhow!("Failed to create HTTP client: {}", e))?;

        let start_time = Instant::now();

        // Handle all possible failure points in the request chain
        let response = match tokio::time::timeout(
            Duration::from_secs_f64(SYNTH_CHALLENGE_TIMEOUT),
            req_client.get(&url).send(),
        )
        .await
        {
            Ok(Ok(resp)) => resp,
            Ok(Err(e)) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    success: false,
                    error: Some(format!("HTTP request failed: {}", e)),
                })
            }
            Err(_) => {
                return Ok(ChallengeResult {
                    miner_uid,
                    latency: SYNTH_CHALLENGE_TIMEOUT,
                    bytes_len: 0,
                    success: false,
                    error: Some("Request timed out".to_string()),
                })
            }
        };

        let latency = start_time.elapsed().as_secs_f64();

        if response.status() != StatusCode::OK {
            return Ok(ChallengeResult {
                miner_uid,
                latency: SYNTH_CHALLENGE_TIMEOUT,
                bytes_len: 0,
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
                    success: false,
                    error: Some(format!("Failed to deserialize piece: {}", e)),
                })
            }
        };

        let computed_hash = blake3::hash(&piece_data);
        let success = *computed_hash.as_bytes() == piece_hash;

        Ok(ChallengeResult {
            miner_uid,
            latency: if success {
                latency
            } else {
                SYNTH_CHALLENGE_TIMEOUT
            },
            bytes_len: piece_data.len(),
            success,
            error: if !success {
                Some("Hash verification failed".to_string())
            } else {
                None
            },
        })
    }
}
