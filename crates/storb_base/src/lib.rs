use memory_db::MemoryDb;
use std::collections::HashMap;
use std::net::Ipv4Addr;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::sync::RwLock;

use crabtensor::api::runtime_apis::neuron_info_runtime_api::NeuronInfoRuntimeApi;
use crabtensor::api::runtime_types::pallet_subtensor::rpc_info::neuron_info::NeuronInfoLite;
use crabtensor::axon::{serve_axon_payload, AxonProtocol};
use crabtensor::subtensor::Subtensor;
use crabtensor::wallet::{hotkey_location, load_key_seed, signer_from_seed, Signer};
use crabtensor::AccountId;
use libp2p::{multiaddr::multiaddr, Multiaddr, PeerId};
use tokio::sync::{mpsc, Mutex};
use tracing::info;

use crate::swarm::dht::StorbDHT;
use crate::version::Version;

pub mod constants;
pub mod memory_db;
pub mod piece;
pub mod piece_hash;
pub mod swarm;
pub mod sync;
pub mod utils;
pub mod verification;
pub mod version;

pub type AddressBook = Arc<RwLock<HashMap<PeerId, NodeInfo>>>;

#[derive(Debug)]
pub enum NeuronError {
    SubtensorError(String),
    ConfigError(String),
    NodeInfoError(String),
}

impl std::fmt::Display for NeuronError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NeuronError::SubtensorError(e) => write!(f, "Subtensor error: {}", e),
            NeuronError::ConfigError(e) => write!(f, "Configuration error: {}", e),
            NeuronError::NodeInfoError(e) => write!(f, "Node info error: {}", e),
        }
    }
}

impl std::error::Error for NeuronError {}

#[derive(Clone)]
pub struct SubtensorConfig {
    pub network: String,
    pub address: String,
    pub insecure: bool,
}

#[derive(Clone)]
pub struct NeuronConfig {
    pub sync_frequency: u64,
}

#[derive(Clone)]
pub struct DhtConfig {
    pub port: u16,
    pub bootstrap_nodes: Option<Vec<Multiaddr>>,
}

#[derive(Clone)]
pub struct BaseNeuronConfig {
    pub version: Version,
    pub netuid: u16,
    pub external_ip: String,
    pub api_port: u16,
    pub quic_port: Option<u16>,
    pub post_ip: bool,

    pub wallet_path: PathBuf,
    pub wallet_name: String,
    pub hotkey_name: String,

    pub mock: bool,

    pub load_old_nodes: bool,
    pub min_stake_threshold: u64,

    pub db_file: PathBuf,
    pub dht_dir: PathBuf,
    pub pem_file: PathBuf,

    pub subtensor: SubtensorConfig,

    pub neuron: NeuronConfig,

    pub dht: DhtConfig,
}

#[derive(Debug, Clone)]
pub struct NodeInfo {
    pub neuron_info: NeuronInfoLite<AccountId>,
    pub peer_id: Option<PeerId>,
    pub http_address: Option<Multiaddr>,
    pub quic_address: Option<Multiaddr>,
    pub version: Version,
}

pub enum NodeKey {
    Uid(u16),
    PeerId(PeerId),
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
pub struct LocalNodeInfo {
    pub uid: Option<u16>,
    pub peer_id: Option<PeerId>,
    pub http_address: Option<Multiaddr>,
    pub quic_address: Option<Multiaddr>,
    pub version: Version,
}

#[derive(Clone)]
pub struct BaseNeuron {
    pub config: BaseNeuronConfig,
    pub neurons: Arc<RwLock<Vec<NeuronInfoLite<AccountId>>>>,
    pub address_book: AddressBook,
    pub peer_node_uid: bimap::BiMap<PeerId, u16>,
    pub command_sender: mpsc::Sender<swarm::dht::DhtCommand>,
    pub signer: Signer,
    pub local_node_info: LocalNodeInfo,
    pub subtensor: Subtensor,
}

// TODO: add a function for loading neuron state? (see TODO(420))
impl BaseNeuron {
    async fn get_subtensor_connection(
        insecure: bool,
        url: &String,
    ) -> Result<Subtensor, NeuronError> {
        if insecure {
            return Subtensor::from_insecure_url(url)
                .await
                .map_err(|e| NeuronError::SubtensorError(e.to_string()));
        }

        Subtensor::from_url(url)
            .await
            .map_err(|e| NeuronError::SubtensorError(e.to_string()))
    }

    pub async fn new(
        config: BaseNeuronConfig,
        mem_db: Option<Arc<MemoryDb>>,
    ) -> Result<Self, NeuronError> {
        let subtensor =
            Self::get_subtensor_connection(config.subtensor.insecure, &config.subtensor.address)
                .await?;

        let wallet_path: PathBuf = config.wallet_path.clone();

        let hotkey_location: PathBuf = hotkey_location(
            wallet_path,
            config.wallet_name.clone(),
            config.hotkey_name.clone(),
        );

        info!("Loading hotkey: path = {:?}", hotkey_location);

        let seed = load_key_seed(&hotkey_location).unwrap();

        let signer = signer_from_seed(&seed).unwrap();

        let mut ed_keypair = ed25519_dalek::SigningKey::from_bytes(&seed).to_keypair_bytes();
        let libp2p_keypair =
            match libp2p::identity::ed25519::Keypair::try_from_bytes(&mut ed_keypair) {
                Ok(keypair) => keypair,
                Err(_) => {
                    return Err(NeuronError::ConfigError(
                        "Failed to create ed25519 keypair from bytes".to_string(),
                    ));
                }
            };

        let bootstrap_nodes = config.dht.bootstrap_nodes.clone();
        let neurons_arc = Arc::new(RwLock::new(Vec::new()));
        let address_book = Arc::new(RwLock::new(HashMap::new()));
        let peer_verifier = Arc::new(swarm::dht::BittensorPeerVerifier {
            address_book: address_book.clone(),
        });

        let (dht_og, command_sender) = StorbDHT::new(
            config.dht_dir.clone(),
            mem_db,
            config.dht.port,
            bootstrap_nodes,
            libp2p_keypair.into(),
            peer_verifier,
        )
        .expect("Failed to create StorbDHT instance");

        let dht = Arc::new(Mutex::new(dht_og));

        let dht_locked = dht.lock().await;
        let peer_id = *dht_locked.swarm.local_peer_id();
        drop(dht_locked);

        // Spawn a separate task for processing events
        tokio::spawn(async move {
            loop {
                let mut dht_locked = dht.lock().await;
                dht_locked.process_events().await;
                drop(dht_locked);
                tokio::task::yield_now().await;
            }
        });

        let external_ip: Ipv4Addr = config.external_ip.parse().expect("Invalid IP address");

        let local_http_address = Some(multiaddr!(Ip4(external_ip), Tcp(config.api_port)));

        let local_quic_address = config
            .quic_port
            .map(|port| multiaddr!(Ip4(external_ip), Udp(port)));

        let local_node_info = LocalNodeInfo {
            uid: None,
            peer_id: Some(peer_id),
            http_address: local_http_address,
            quic_address: local_quic_address,
            version: config.version.clone(),
        };

        let neuron = BaseNeuron {
            config: config.clone(),
            command_sender,
            neurons: neurons_arc,
            address_book: address_book.clone(),
            peer_node_uid: bimap::BiMap::new(),
            subtensor,
            signer,
            local_node_info,
        };

        // Check registration status
        neuron.check_registration().await?;

        // Post IP if specified
        info!("Should post ip?: {}", config.post_ip);
        if config.post_ip {
            let address = format!("{}:{}", config.external_ip, config.api_port)
                .parse()
                .unwrap();
            info!("Serving axon as: {}", address);
            let payload = serve_axon_payload(config.netuid, address, AxonProtocol::Udp);
            neuron
                .subtensor
                .tx()
                .sign_and_submit_default(&payload, &neuron.signer)
                .await
                .unwrap();
            info!("Successfully served axon!");
        }

        info!(
            "My peer address: /ip4/{:?}/udp/{:?}/quic-v1/p2p/{}",
            external_ip,
            config.quic_port.unwrap(),
            peer_id.to_string()
        );
        Ok(neuron.clone())
    }

    /// Get info of a neuron (node) from a list of neurons that matches the account ID.
    pub fn find_neuron_info<'a>(
        neurons: &'a [NeuronInfoLite<AccountId>],
        account_id: &AccountId,
    ) -> Option<&'a NeuronInfoLite<AccountId>> {
        neurons.iter().find(|neuron| &neuron.hotkey == account_id)
    }

    /// Check whether the neuron is registered in the subnet or not.
    pub async fn check_registration(&self) -> Result<(), NeuronError> {
        let current_block = self.subtensor.blocks().at_latest().await.unwrap();
        let runtime_api = self.subtensor.runtime_api().at(current_block.reference());

        // TODO: is there a nicer way to pass self to NeuronInfoRuntimeApi?
        let neurons_payload =
            NeuronInfoRuntimeApi::get_neurons_lite(&NeuronInfoRuntimeApi {}, self.config.netuid);

        // TODO: error out if cant access chain when calling above function

        let neurons: Vec<NeuronInfoLite<AccountId>> =
            runtime_api.call(neurons_payload).await.unwrap();

        let neuron_info = Self::find_neuron_info(&neurons, self.signer.account_id());
        match neuron_info {
            Some(_) => Ok(()),
            None => Err(NeuronError::ConfigError(
                "Neuron is not registered".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, create_dir_all};

    fn setup_test_wallet() -> (PathBuf, String) {
        let temp_dir = std::env::temp_dir().join("storb_test_wallets");
        create_dir_all(&temp_dir).unwrap();

        let wallet_name = "test_wallet";
        let hotkey_name = "test_hotkey";

        let wallet_path = temp_dir.join(wallet_name).join("hotkeys");
        create_dir_all(&wallet_path).unwrap();

        let hotkey_file = wallet_path.join(hotkey_name);
        let random_seed = vec![0u8; 32];
        fs::write(&hotkey_file, random_seed).unwrap();

        (temp_dir, wallet_name.to_string())
    }

    fn get_test_config() -> BaseNeuronConfig {
        let (wallet_path, wallet_name) = setup_test_wallet();

        BaseNeuronConfig {
            version: Version {
                major: 1,
                minor: 0,
                patch: 0,
            },
            netuid: 1,
            external_ip: "127.0.0.1".to_string(),
            api_port: 8080,
            quic_port: Some(8081),
            post_ip: false,
            wallet_path,
            wallet_name,
            hotkey_name: "test_hotkey".to_string(),
            mock: false,
            load_old_nodes: false,
            min_stake_threshold: 0,
            db_file: "./test.db".into(),
            dht_dir: "./test_dht".into(),
            pem_file: "cert.pem".into(),
            subtensor: SubtensorConfig {
                network: "finney".to_string(),
                address: "wss://test.finney.opentensor.ai:443".to_string(),
                insecure: false,
            },
            neuron: NeuronConfig {
                sync_frequency: 100,
            },
            dht: DhtConfig {
                port: 8081,
                bootstrap_nodes: None,
            },
        }
    }

    #[tokio::test]
    async fn test_get_subtensor_connection() {
        let config = get_test_config();
        let result = BaseNeuron::get_subtensor_connection(
            config.subtensor.insecure,
            &config.subtensor.address,
        )
        .await;
        assert!(result.is_ok());
    }

    // TODO: update tests

    // #[tokio::test]
    // async fn test_find_neuron_info() {
    //     let neurons = vec![];
    //     let account_id = AccountId::from([0; 32]);
    //     info!("account_id: {account_id}");
    //     let result = BaseNeuron::find_neuron_info(&neurons, &account_id);
    //     assert!(result.is_none());
    // }

    // TODO: get wallet handling working properly for testing
    // #[tokio::test]
    // async fn test_check_registration() {
    //     let config = get_test_config();
    //     let neuron = BaseNeuron::new(config).await.unwrap();
    //     let result = neuron.check_registration().await;
    //     assert!(result.is_err()); // Should fail since test wallet not registered
    // }

    // #[tokio::test]
    // async fn test_sync_metagraph() {
    //     let config = get_test_config();
    //     let mut neuron = BaseNeuron::new(config).await.unwrap();
    //     neuron.sync_metagraph().await;
    //     let neurons = neuron.get_neurons();
    //     let neurons_read = neurons.read().unwrap();
    //     assert!(neurons_read.len() > 0); // Verify we can read neurons after sync
    // }

    // impl Drop for BaseNeuron {
    //     fn drop(&mut self) {
    //         if Path::new(&self.config.wallet_path).exists() {
    //             let _ = fs::remove_dir_all(&self.config.wallet_path);
    //         }
    //     }
    // }
}
