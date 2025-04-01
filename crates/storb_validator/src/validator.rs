use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;
use std::time::Instant;

use anyhow::{anyhow, Context};
use base::constants::CLIENT_TIMEOUT;
use base::piece::encode_chunk;
use base::swarm::models::ChunkDHTValue;
use base::utils::multiaddr_to_socketaddr;
use base::verification::{HandshakePayload, KeyRegistrationInfo, VerificationMessage};
use base::{swarm, BaseNeuron, BaseNeuronConfig, NeuronError};
use crabtensor::sign::sign_message;
use crabtensor::weights::set_weights_payload;
use crabtensor::weights::NormalizedWeight;
use libp2p::kad::RecordKey;
use ndarray::Array1;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use reqwest::StatusCode;
use subxt::ext::codec::Compact;
use subxt::ext::sp_core::hexdisplay::AsBytesRef;
use tokio::sync::RwLock;
use tracing::{debug, error, info, trace};

use crate::quic::{create_quic_client, make_client_endpoint};
use crate::scoring::{normalize_min_max, select_random_chunk_from_db, ScoringSystem};
use crate::upload::upload_piece_data;
use crate::utils::{generate_synthetic_data, get_id_quic_uids};
use rand::seq::IteratorRandom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use crate::constants::{
    MAX_CHALLENGE_PIECE_NUM, MAX_SYNTH_CHUNK_SIZE, MIN_SYNTH_CHUNK_SIZE, SYNTH_CHALLENGE_TIMEOUT,
    SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE,
};

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
        let signer = self.neuron.read().await.signer.clone();
        let mut rng: StdRng = SeedableRng::from_entropy();
        let mut store_latencies = MinerLatencyMap::default();
        let mut retrieval_latencies = MinerLatencyMap::default();
        // let num_neurons = self.neuron.neurons.len();

        // ----==== Synthetic store challenges ====----

        info!("Running store retrieval challenges");

        let synth_size = rng.gen_range(MIN_SYNTH_CHUNK_SIZE..MAX_SYNTH_CHUNK_SIZE + 1);
        // TODO: generate synthetic data separately and in its own interval
        // - we want to scale back generation of synthetic data if there is enough organic data
        let synthetic_chunk = generate_synthetic_data(synth_size);

        let encoded = encode_chunk(&synthetic_chunk, 0);

        for piece in encoded.pieces.clone() {
            let piece_hash_raw = blake3::hash(&piece.data);
            let piece_hash = piece_hash_raw.as_bytes();

            // TODO: if we cannot get quic connections of certain miners should we should punish them?
            let vali_arc = Arc::new(self.clone());
            let (_, quic_addresses, quic_miner_uids) = get_id_quic_uids(vali_arc.clone()).await;

            let num_miners_to_query = std::cmp::min(10, quic_miner_uids.len()); // TODO: use a constant
            let idxs_to_query: Vec<usize> =
                (0..quic_miner_uids.len()).choose_multiple(&mut rng, num_miners_to_query);

            for idx in idxs_to_query {
                let quic_addr = &quic_addresses[idx];
                let quic_miner_uid = quic_miner_uids[idx];
                let db = self.scoring_system.write().await.db.clone();

                let neuron_guard = self.neuron.read().await;
                let peer_id = neuron_guard
                    .peer_node_uid
                    .get_by_right(&quic_miner_uid)
                    .ok_or("No peer ID found for the miner UID")?;
                let miner_info = self
                    .neuron
                    .read()
                    .await
                    .address_book
                    .clone()
                    .read()
                    .await
                    .get(peer_id)
                    .ok_or("No NodeInfo found for the given miner")?
                    .clone();

                info!("Challenging miner {quic_miner_uid} with store request");
                db.conn.lock().await.execute(
                    "UPDATE miner_stats SET store_attempts = store_attempts + 1 WHERE miner_uid = ?",
                    [quic_miner_uid],
                )?;

                let client =
                    make_client_endpoint(SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), 0))
                        .map_err(|e| anyhow!(e.to_string()))?;
                let socket_addr = multiaddr_to_socketaddr(quic_addr)
                .context("Could not get SocketAddr from Multiaddr. Ensure that the Multiaddr is not missing any components")?;

                let (_, quic_conn) = match create_quic_client(&client, socket_addr).await {
                    Ok(connection) => (socket_addr, connection),
                    Err(e) => {
                        error!("Failed to establish connection with {}: {}", socket_addr, e);
                        continue; // TODO: punish miner
                    }
                };

                let start_time = Instant::now();
                let hash_vec = upload_piece_data(
                    self.neuron.clone(),
                    miner_info,
                    &quic_conn,
                    piece.data.clone(),
                )
                .await?;
                let latency = start_time.elapsed().as_secs_f64();
                let hash = hash_vec.as_bytes_ref();

                info!("Received hash: {:?}", hash);
                info!("Expected hash: {:?}", piece_hash);

                if hash != piece_hash {
                    error!("Hash mismatch for piece from miner {quic_miner_uid}");
                    store_latencies.record_latency(
                        quic_miner_uid,
                        SYNTH_CHALLENGE_TIMEOUT,
                        piece.data.len(),
                    );
                    continue;
                } else {
                    info!("Received hash for piece from miner {quic_miner_uid}");
                    // Increase store successes
                    db.conn.lock().await.execute(
                    "UPDATE miner_stats SET store_successes = store_successes + 1 WHERE miner_uid = ?",
                    [quic_miner_uid],
                    )?;
                    // Increase total successes
                    db.conn.lock().await.execute(
                    "UPDATE miner_stats SET total_successes = total_successes + 1 WHERE miner_uid = ?",
                    [quic_miner_uid],
                    )?;
                    store_latencies.record_latency(quic_miner_uid, latency, piece.data.len());
                }
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
        let validator_id = self
            .neuron
            .read()
            .await
            .local_node_info
            .uid
            .ok_or("Failed to get validator UID")?;

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
        swarm::dht::StorbDHT::put_chunk_entry(
            // TODO: read or write?
            // self.neuron.write().await.command_sender.clone(),
            self.neuron.read().await.command_sender.clone(),
            chunk_key,
            chunk_dht_value,
        )
        .await?;

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

        let neuron_guard = self.neuron.read().await;
        let dht_sender = neuron_guard.command_sender.clone();
        let address_book = neuron_guard.address_book.clone();

        while pieces_checked < MAX_CHALLENGE_PIECE_NUM {
            let idx = rng.gen_range(0..piece_hashes.len());

            let piece_hash = piece_hashes[idx];
            let piece_key = RecordKey::new(&piece_hash);

            let piece_providers =
                swarm::dht::StorbDHT::get_piece_providers(&dht_sender, piece_key.clone()).await?;

            if piece_providers.is_empty() {
                return Err(anyhow!("No providers found for piece {:?}", piece_key).into());
            }

            let mut miner_uids: Vec<u16> = Vec::new();
            for peer_id in piece_providers.clone() {
                let addr_book_guard = address_book.read().await;
                let miner_info = addr_book_guard.get(&peer_id).ok_or_else(|| {
                    anyhow!("Miner info not found in address book for peer {}", peer_id)
                })?;
                let miner_uid = miner_info.neuron_info.uid;
                miner_uids.push(miner_uid);
            }

            // go through bimap, get QUIC addresses of miners
            for peer_id in piece_providers {
                let addr_book_guard = address_book.read().await;
                let miner_info = addr_book_guard.get(&peer_id).ok_or_else(|| {
                    anyhow!("Miner info not found in address book for peer {}", peer_id)
                })?;
                let miner_uid = miner_info.neuron_info.uid;

                let db = self.scoring_system.write().await.db.clone();
                debug!("Updating retrieval attempts in sqlite db");
                db.conn.lock().await.execute("UPDATE miner_stats SET retrieval_attempts = retrieval_attempts + 1 WHERE miner_uid = $1", [&miner_uid])?;
                debug!("Updated");

                let message = VerificationMessage {
                    netuid: self.config.neuron_config.netuid,
                    miner: KeyRegistrationInfo {
                        uid: miner_uid,
                        account_id: miner_info.neuron_info.hotkey.clone(),
                    },
                    validator: KeyRegistrationInfo {
                        uid: validator_id as u16,
                        account_id: signer.account_id().clone(),
                    },
                };
                let signature = sign_message(&signer, &message);
                let payload = HandshakePayload { signature, message };
                let payload_bytes = bincode::serialize(&payload)?;

                let req_client = reqwest::Client::builder()
                    .timeout(CLIENT_TIMEOUT)
                    .build()
                    .context("Failed to build reqwest client")?;

                let url = miner_info
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
                    })
                    .ok_or_else(|| anyhow!("Invalid HTTP address in node_info"))?;

                let start_time = Instant::now();
                let node_response = tokio::time::timeout(
                    Duration::from_secs_f64(SYNTH_CHALLENGE_TIMEOUT),
                    req_client.get(&url).send(),
                )
                .await?
                .context("Failed to query node")?;

                let latency = start_time.elapsed().as_secs_f64();

                trace!("Node response from {:?}: {:?}", peer_id, node_response);

                let response_status = node_response.status();
                let body_bytes = node_response
                    .bytes()
                    .await
                    .context("Failed to read response body")?;
                let piece_data = base::piece::deserialise_piece_response(&body_bytes, &piece_hash)
                    .context("Failed to deserialise piece response")?;

                // Check status of response
                if response_status != StatusCode::OK {
                    let err_msg = bincode::deserialize::<String>(&piece_data[..])?;
                    error!(
                        "Response returned with status code {}: {}",
                        response_status, err_msg
                    );
                    continue;
                }

                // Verify the integrity of the piece_data using Blake3.
                let computed_hash = blake3::hash(&piece_data);
                let valid_hash = computed_hash.as_bytes() == &piece_hash;

                if valid_hash {
                    retrieval_latencies.record_latency(miner_uid, latency, piece_data.len());
                } else {
                    error!("Hash mismatch for provider {:?}", peer_id);
                    retrieval_latencies.record_latency(
                        miner_uid,
                        SYNTH_CHALLENGE_TIMEOUT,
                        piece_data.len(),
                    );
                    continue;
                }

                // Update the scoring system with the successful retrieval.
                db.conn.lock().await.execute("UPDATE miner_stats SET retrieval_successes = retrieval_successes + 1 WHERE miner_uid = $1", [&miner_uid])?;
                db.conn.lock().await.execute("UPDATE miner_stats SET total_successes = total_successes + 1 WHERE miner_uid = $1", [&miner_uid])?;
            }

            // TODO: latency scoring?
            // for miner_uid in miner_uids {
            //     match new_ret_latency_map.get(&miner_uid) {
            //         Some(curr_latency) => {
            //             new_ret_la
            //         }
            //     }
            // }

            pieces_checked += 1;
        }

        let store_avg_latencies = store_latencies.get_all_latencies();
        let retrieval_avg_latencies = retrieval_latencies.get_all_latencies();
        let moving_average_alpha = self.scoring_system.read().await.moving_average_alpha;

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

    /// Sync the validator with the metagraph.
    // pub async fn sync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
    //     info!("Syncing validator");

    //     let uids_to_update = self.neuron.sync_metagraph().await?;
    //     let neuron_count = self.neuron.neurons.read().await.len();
    //     self.scoring_system
    //         .write()
    //         .await
    //         .update_scores(neuron_count, uids_to_update)
    //         .await;

    //     match Self::set_weights().await {
    //         Ok(_) => info!("Successfully set weights on chain"),
    //         Err(e) => error!("Failed to set weights on chain: {}", e),
    //     };

    //     info!("Done syncing validator");

    //     Ok(())
    // }

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

        Ok(())
    }
}
