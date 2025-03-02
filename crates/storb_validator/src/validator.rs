use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::{anyhow, Context};
use base::constants::CLIENT_TIMEOUT;
use base::piece::encode_chunk;
use base::sync::Synchronizable;
use base::utils::multiaddr_to_socketaddr;
use base::{swarm, BaseNeuron, BaseNeuronConfig, NeuronError};
use libp2p::kad::RecordKey;
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use subxt::ext::sp_core::hexdisplay::AsBytesRef;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, error, info, trace};

use crate::quic::{create_quic_client, make_client_endpoint};
use crate::scoring::{select_random_chunk_from_db, ScoringSystem};
use crate::upload::upload_piece_data;
use crate::utils::{generate_synthetic_data, get_id_quic_uids};
use rand::seq::IteratorRandom;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

const MAX_CHALLENGE_PIECE_NUM: i32 = 5;
const SYNTH_CHALLENGE_TIMEOUT: f64 = 1.0; // TODO: modify this
const MIN_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10; // minimum size of synthetic data in bytes
const MAX_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10 * 10 * 10; // maximum size of synthetic data in bytes

#[derive(Clone)]
pub struct ValidatorConfig {
    pub scores_state_file: PathBuf,
    pub neuron_config: BaseNeuronConfig,
}

/// The Storb validator.
#[derive(Clone)]
pub struct Validator {
    pub config: ValidatorConfig,
    pub neuron: BaseNeuron,
    pub scoring_system: Arc<RwLock<ScoringSystem>>,
}

impl Validator {
    pub async fn new(config: ValidatorConfig) -> Result<Self, NeuronError> {
        let neuron_config = config.neuron_config.clone();
        let neuron = BaseNeuron::new(neuron_config).await?;

        let scoring_system = Arc::new(RwLock::new(
            ScoringSystem::new(&config.neuron_config.db_file, &config.scores_state_file)
                .await
                .map_err(|err| NeuronError::ConfigError(err.to_string()))?,
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
        let mut rng: StdRng = SeedableRng::from_entropy();
        // let num_neurons = self.neuron.neurons.len();

        // ----==== Synthetic store challenges ====----

        info!("Running store retrieval challenges");

        let synth_size = rng.gen_range(MIN_SYNTH_CHUNK_SIZE..MAX_SYNTH_CHUNK_SIZE + 1);
        // TODO: generate synthetic data separately and in its own interval
        // - we want to scale back generation of synthetic data if there is enough organic data
        let synthetic_chunk = generate_synthetic_data(synth_size);

        let encoded = encode_chunk(&synthetic_chunk, 0);

        for piece in encoded.pieces {
            let piece_hash_raw = blake3::hash(&piece.data);
            let piece_hash = piece_hash_raw.as_bytes();

            let vali_arc = Arc::new(RwLock::new(self.clone()));

            // TODO: if we cannot get quic connections of certain miners should we should punish them?
            let (_, quic_addresses, quic_miner_uids) = get_id_quic_uids(vali_arc).await;

            let num_miners_to_query = std::cmp::min(10, quic_miner_uids.len()); // TODO: use a constant
            let idxs_to_query: Vec<usize> =
                (0..quic_miner_uids.len()).choose_multiple(&mut rng, num_miners_to_query);

            for idx in idxs_to_query {
                let quic_addr = &quic_addresses[idx];
                let quic_miner_uid = quic_miner_uids[idx];
                let db = self.scoring_system.write().await.db.clone();

                info!("Challenging miner {quic_miner_uid} with store request");
                db.conn.lock().await.execute(
                    "UPDATE miner_stats SET store_attempts = store_attempts + 1 WHERE miner_uid = ?",
                    [quic_miner_uid],
                )?;

                let client =
                    make_client_endpoint(SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0))
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

                let hash_vec = upload_piece_data(&quic_conn, piece.data.clone()).await?; // TODO: error handle
                let hash = hash_vec.as_bytes_ref();

                info!("Received hash: {:?}", hash);
                info!("Expected hash: {:?}", piece_hash);

                if hash != piece_hash {
                    error!("Hash mismatch for piece from miner {quic_miner_uid}");
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
                }
            }
        }

        info!("Completed synthetic store challenges");

        // ----==== Synthetic retrieval challenges ====----

        info!("Running synthetic retrieval challenges");

        // pick pieces, ask some of the miners that have the pieces
        let db_conn = self.scoring_system.write().await.db.conn.clone();
        let chunk_key = select_random_chunk_from_db(db_conn).await?;
        debug!("Selected chunk key: {:?}", chunk_key);

        // request pieces
        let chunk_entry =
            swarm::dht::StorbDHT::get_chunk_entry(self.neuron.command_sender.clone(), chunk_key)
                .await?
                .context("Chunk for the given record key was not found")?;
        let piece_hashes = chunk_entry.piece_hashes;

        // TODO: latency scoring
        let mut new_ret_latency_map = HashMap::<u16, f64>::new();
        // let mut new_ret_score_map = HashMap::<u16, f64>::new();

        let mut pieces_checked = 0;
        while pieces_checked < MAX_CHALLENGE_PIECE_NUM {
            let idx = rng.gen_range(0..piece_hashes.len());

            let piece_hash = piece_hashes[idx];
            let piece_key = RecordKey::new(&piece_hash);

            let dht_sender = self.neuron.command_sender.clone();
            let piece_providers =
                swarm::dht::StorbDHT::get_piece_providers(&dht_sender, piece_key.clone()).await?;

            if piece_providers.is_empty() {
                error!("No providers found for piece {:?}", piece_key); // TODO: what else should we do here?
            }

            let mut miner_uids: Vec<u16> = Vec::new();
            for peer_id in piece_providers.clone() {
                let address_book = self.neuron.address_book.read().await;
                let miner_info = address_book.get(&peer_id).unwrap(); // TODO: handle this
                let miner_uid = miner_info.neuron_info.uid;
                miner_uids.push(miner_uid);
            }

            // go through bimap, get QUIC addresses of miners
            for peer_id in piece_providers {
                let address_book = self.neuron.address_book.read().await;
                let miner_info = address_book.get(&peer_id).unwrap(); // TODO: handle this
                let miner_uid = miner_info.neuron_info.uid;

                let db = self.scoring_system.write().await.db.clone();
                debug!("Updating retrieval attempts in sqlite db");
                db.conn.lock().await.execute("UPDATE miner_stats SET retrieval_attempts = retrieval_attempts + 1 WHERE miner_uid = $1", [&miner_uid])?;
                debug!("Updated");

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
                            "http://{}:{}/piece?piecehash={}",
                            socket_addr.ip(),
                            socket_addr.port(),
                            hex::encode(piece_hash)
                        )
                    })
                    .ok_or_else(|| anyhow!("Invalid HTTP address in node_info"))?;

                let start_time = Instant::now();
                let node_response = req_client
                    .get(&url)
                    .send()
                    .await
                    .context("Failed to query node")?;

                let latency = start_time.elapsed().as_secs_f64();

                trace!("Node response from {:?}: {:?}", peer_id, node_response);

                let body_bytes = node_response
                    .bytes()
                    .await
                    .context("Failed to read response body")?;

                let piece_data = base::piece::deserialise_piece_response(&body_bytes, &piece_hash)
                    .context("Failed to deserialise piece response")?;

                // Verify the integrity of the piece_data using Blake3.
                let computed_hash = blake3::hash(&piece_data);
                let valid_hash = computed_hash.as_bytes() == &piece_hash;

                if valid_hash {
                    new_ret_latency_map.insert(miner_uid, latency);
                } else {
                    error!("Hash mismatch for provider {:?}", peer_id);
                    new_ret_latency_map.insert(miner_uid, SYNTH_CHALLENGE_TIMEOUT);
                    continue;
                }

                // Update the scoring system with the successful retrieval.
                db.conn.lock().await.execute("UPDATE miner_stats SET retrieval_successes = retrieval_successes + 1 WHERE miner_uid = $1", [&miner_uid])?;
                db.conn.lock().await.execute("UPDATE miner_stats SET total_successes = total_successes + 1 WHERE miner_uid = $1", [&miner_uid])?;
            }

            // for miner_uid in miner_uids {
            //     match new_ret_latency_map.get(&miner_uid) {
            //         Some(curr_latency) => {
            //             new_ret_la
            //         }
            //     }
            // }

            pieces_checked += 1;
        }

        // record latency and response rate scores
        Ok(())
    }

    /// Sync the validator with the metagraph.
    pub async fn sync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Syncing validator");

        let uids_to_update = self.neuron.sync_metagraph().await?;
        let neuron_count = self.neuron.neurons.len();
        self.scoring_system
            .write()
            .await
            .update_scores(neuron_count, uids_to_update)
            .await;
        self.scoring_system.write().await.set_weights();

        info!("Done syncing validator");

        Ok(())
    }
}
