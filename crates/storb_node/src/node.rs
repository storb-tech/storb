use std::fs;
use std::net::{AddrParseError, Ipv4Addr};
use std::path::PathBuf;
use std::sync::Arc;

use crabtensor::api::runtime_apis::neuron_info_runtime_api::NeuronInfoRuntimeApi;
use crabtensor::api::runtime_types::pallet_subtensor::rpc_info::neuron_info::NeuronInfoLite;
use crabtensor::axon::{serve_axon_payload, AxonProtocol};
use crabtensor::subtensor::Subtensor;
use crabtensor::wallet::{
    hotkey_location, load_key_seed, signer_from_seed, AccountLoadingError, Signer,
};
use crabtensor::AccountId;
use multiaddr::{multiaddr, Multiaddr};
use serde::{Deserialize, Serialize};
use sp_core::crypto::SecretStringError;
use subxt::utils::H256;
use thiserror::Error;
use tokio::sync::RwLock;
use tokio::time::Instant;
use tracing::{debug, error, info, warn};
use storb_protocol::node::{NodeUid, NodeInfo, LocalNodeInfo};

use crate::config::NodeConfig;
use crate::constants::SUBTENSOR_SERVING_RATE_LIMIT_EXCEEDED;
use crate::sync::Synchronizable;

pub enum NodeType {
    Miner,
    Validator,
}

#[derive(Debug, Error)]
pub enum NodeError {
    #[error("Failed to load neuron state from disk: {0}")]
    NeuronStateLoadError(String),

    #[error("Failed to save neuron state to disk: {0}")]
    NeuronStateSaveError(String),

    #[error("Failed to parse address: {0}")]
    AddrParseError(#[from] AddrParseError),

    #[error("Subxt error: {0}")]
    SubxtError(#[from] subxt::Error),

    #[error("The node is not registered on the subnet")]
    RegistrationError,

    #[error("Failed to load account from key seed: {0}")]
    AccountLoadingError(#[from] AccountLoadingError),

    #[error("Failed to sign: {0}")]
    SecretStringError(#[from] SecretStringError),

    #[error("Node UID does not exist")]
    NonexistentNodeUid,
}

/// Represents a node on the subnet.
pub struct Node {
    /// The node type.
    pub node_type: NodeType,

    /// Configuration for the node.
    pub config: NodeConfig,

    /// Nodes (neurons) in the subnet.
    pub nodes: Arc<RwLock<Vec<NeuronInfoLite<AccountId>>>>,

    /// Signer for the node, used for signing transactions and messages.
    pub signer: Signer,

    /// Information about the node itself.
    pub local_info: LocalNodeInfo,
}

impl Node {
    /// Creates a new [`Node`] instance from the provided configuration.
    pub async fn new(node_type: NodeType, config: NodeConfig) -> Result<Self, NodeError> {
        let wallet_path: PathBuf = config.wallet_path.clone();
        let hotkey_location: PathBuf = hotkey_location(
            wallet_path,
            config.wallet_name.clone(),
            config.hotkey_name.clone(),
        );
        info!("Loading hotkey: path = {hotkey_location:?}");

        let seed = load_key_seed(&hotkey_location)?;
        let signer = signer_from_seed(&seed)?;

        // Try and load the neuron state from disk
        let nodes = match Self::load_neurons_state_from_disk(&config.neurons_dir) {
            Ok(loaded_neurons) => {
                info!("Loaded {} neurons from saved state", loaded_neurons.len());
                Arc::new(RwLock::new(loaded_neurons))
            }
            Err(err) => {
                error!("Failed to load neuron state from disk: {err}");
                info!("Creating new empty neuron state");
                Arc::new(RwLock::new(Vec::new()))
            }
        };

        let external_ip: Ipv4Addr = config.external_ip.parse()?;
        let local_http_address = Some(multiaddr!(Ip4(external_ip), Tcp(config.api_port)));

        let local_quic_address = config
            .quic_port
            .map(|port| multiaddr!(Ip4(external_ip), Udp(port)));

        let local_node_info = LocalNodeInfo {
            uid: None,
            http_address: local_http_address,
            quic_address: local_quic_address,
        };

        let neuron = Node {
            node_type,
            config: config.clone(),
            nodes,
            signer,
            local_info: local_node_info,
        };
        neuron.check_registration().await?;

        // Post IP if specified
        info!("Should post ip?: {}", config.post_ip);
        if config.post_ip {
            let address = format!("{}:{}", config.external_ip, config.api_port)
                .parse()
                .expect("Failed to parse address when attempting to post IP. Is it correct?");
            info!("Serving axon as: {address}");

            let payload = serve_axon_payload(config.netuid, address, AxonProtocol::Udp);

            let subtensor = Self::get_subtensor_connection(
                &config.subtensor.address,
                config.subtensor.insecure,
            )
            .await?;
            subtensor
                .tx()
                .sign_and_submit_default(&payload, &neuron.signer)
                .await
                .unwrap_or_else(|err| {
                    error!("Failed to post IP to Subtensor");
                    if format!("{err:?}").contains(SUBTENSOR_SERVING_RATE_LIMIT_EXCEEDED) {
                        error!("Invalid Transaction: {err}");
                        error!("Axon info not updated due to rate limit");
                        H256::zero()
                    } else {
                        panic!("Unexpected error: {err}");
                    }
                });

            info!("Successfully served axon!");
        }

        Ok(neuron)
    }

    pub(crate) fn load_neurons_state_from_disk(
        state_file: &PathBuf,
    ) -> Result<Vec<NeuronInfoLite<AccountId>>, NodeError> {
        if !state_file.exists() {
            return Ok(Vec::new());
        }

        let buf = fs::read(state_file).map_err(|err| {
            NodeError::NeuronStateLoadError(format!("Could not read file: {err}"))
        })?;
        bincode::deserialize(&buf).map_err(|err| {
            NodeError::NeuronStateLoadError(format!("Failed to deserialize neurons: {err}"))
        })
    }

    pub(crate) fn save_neurons_state_to_disk(
        state_file: &PathBuf,
        neurons: &[NeuronInfoLite<AccountId>],
    ) -> Result<(), NodeError> {
        let buf = bincode::serialize(&neurons).map_err(|err| {
            NodeError::NeuronStateSaveError(format!("Failed to serialize neurons: {err}"))
        })?;

        fs::write(state_file, buf).map_err(|err| {
            NodeError::NeuronStateSaveError(format!("Failed to write neurons state: {err}"))
        })
    }

    /// Check whether the neuron is registered in the subnet or not.
    pub(crate) async fn check_registration(&self) -> Result<(), NodeError> {
        let subtensor = Self::get_subtensor_connection(
            &self.config.subtensor.address,
            self.config.subtensor.insecure,
        )
        .await?;

        let current_block = subtensor.blocks().at_latest().await?;
        let runtime_api = subtensor.runtime_api().at(current_block.reference());

        let neurons_payload =
            NeuronInfoRuntimeApi::get_neurons_lite(&NeuronInfoRuntimeApi {}, self.config.netuid);

        let neurons: Vec<NeuronInfoLite<AccountId>> = runtime_api.call(neurons_payload).await?;
        match neurons
            .iter()
            .find(|neuron| &neuron.hotkey == self.signer.account_id())
        {
            Some(_) => Ok(()),
            None => Err(NodeError::RegistrationError),
        }
    }

    /// Get the Subtensor client connection from the URL.
    pub(crate) async fn get_subtensor_connection(
        url: &String,
        insecure: bool,
    ) -> Result<Subtensor, NodeError> {
        if insecure {
            return Subtensor::from_insecure_url(url)
                .await
                .map_err(|err| NodeError::SubxtError(err));
        }

        Subtensor::from_url(url)
            .await
            .map_err(|err| NodeError::SubxtError(err))
    }
}

impl Synchronizable for Node {
    type Error = NodeError;

    async fn sync_metagraph(&mut self) -> Result<Vec<NodeUid>, Self::Error> {
        debug!("Starting sync_metagraph");

        let start = Instant::now();
        let subtensor = Self::get_subtensor_connection(
            &self.config.subtensor.address,
            self.config.subtensor.insecure,
        )
        .await?;

        let current_block = subtensor.blocks().at_latest().await?;
        debug!("Got latest block in {:?}", start.elapsed());

        let runtime_api = subtensor.runtime_api().at(current_block.reference());
        let mut local_node_info = self.local_info.clone();

        let neurons_payload =
            NeuronInfoRuntimeApi::get_neurons_lite(&NeuronInfoRuntimeApi {}, self.config.netuid);

        let neurons: Vec<NeuronInfoLite<AccountId>> = runtime_api.call(neurons_payload).await?;
        debug!("Got neurons from metagraph");
        let original_neurons = self.nodes.read().await.clone();

        // Check if neurons have changed or not
        let mut changed_neurons: Vec<NodeUid> =
            neurons.iter().map(|neuron| neuron.uid.into()).collect();

        if original_neurons.len() < neurons.len() {
            changed_neurons = neurons
                .iter()
                .zip(original_neurons.iter())
                .filter(|(curr, og)| curr.hotkey != og.hotkey)
                .map(|(curr, _)| curr.uid.into())
                .collect();

            let mut new_neurons: Vec<NodeUid> = neurons[original_neurons.len()..]
                .iter()
                .map(|neuron| neuron.uid.into())
                .collect();

            changed_neurons.append(&mut new_neurons);

            let mut neurons_guard = self.nodes.write().await;
            *neurons_guard = neurons.clone();
        } else if !original_neurons.is_empty() {
            changed_neurons = neurons
                .iter()
                .zip(original_neurons.iter())
                .filter(|(curr, og)| curr.hotkey != og.hotkey)
                .map(|(curr, _)| curr.uid.into())
                .collect();

            let mut neurons_guard = self.nodes.write().await;
            *neurons_guard = neurons.clone();
        }

        drop(original_neurons);
        debug!("Updated local neurons state");

        debug!("Saving neurons state to disk");
        Self::save_neurons_state_to_disk(&self.config.neurons_dir, &self.nodes.read().await)?;
        debug!("Saved neurons state to disk");

        if local_node_info.uid.is_none() {
            // Find our local node uid or crash the program
            // TODO: do we want to crash or return error?
            let self_neuron = neurons
                .iter()
                .find(|neuron| &neuron.hotkey == self.signer.account_id())
                .expect("Local node not found in neuron list");
            local_node_info.uid = Some(self_neuron.uid.into());
        }

        self.local_info = local_node_info;

        // Create futures for all node info requests
        let mut futures = Vec::new();
        for (neuron_uid, _) in neurons.iter().enumerate() {
            let neuron_info: NeuronInfoLite<AccountId> = neurons[neuron_uid].clone();

            // Skip self to avoid deadlock
            if neuron_uid
                == self
                    .local_info
                    .uid
                    .ok_or(NodeError::NonexistentNodeUid)?
                    .into()
            {
                continue;
            }

            let neuron_ip = neuron_info.axon_info.ip;
            let neuron_port = neuron_info.axon_info.port;

            // Skip invalid IPs/ports
            if neuron_ip == 0 {
                warn!(
                    "Invalid IP for neuron {:?}. Node has never been started",
                    neuron_info.uid
                );
                continue;
            }
            if neuron_port == 0 {
                error!("Invalid port for neuron: {:?}", neuron_info.uid);
                continue;
            }

            // TODO: use storb_protocol
            let ip = Ipv4Addr::from((neuron_ip & 0xffff_ffff) as u32);
            let url_raw = format!("http://{}:{}/info", ip, neuron_port);
            let url = reqwest::Url::parse(&url_raw).map_err(|e| {
                error!("Failed to parse URL: {:?}", e);
                SyncError::GetNodeInfoError(format!("Failed to parse URL: {:?}", e))
            })?;

            // Create future for this node's info request
            let future = async move {
                match Self::get_remote_node_info(url).await {
                    Ok(remote_node_info) => Some((neuron_info, remote_node_info)),
                    Err(err) => {
                        warn!(
                            "Failed to get remote node info (it may be offline): {:?}",
                            err
                        );
                        None
                    }
                }
            };

            futures.push(future);
        }

        Ok()
    }

    // async fn get
}
