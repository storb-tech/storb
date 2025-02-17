pub mod challenge;
pub mod constants;
pub mod piece;
pub mod piece_hash;
pub mod swarm;

use crabtensor::api::runtime_apis::neuron_info_runtime_api::NeuronInfoRuntimeApi;
use crabtensor::axon::{serve_axon_payload, AxonProtocol};
use crabtensor::rpc::api::runtime_types::pallet_subtensor::rpc_info::neuron_info::NeuronInfoLite;
use crabtensor::subtensor::Subtensor;
use crabtensor::wallet::{hotkey_location, load_key_seed, signer_from_seed, Signer};
use crabtensor::AccountId;
use ed25519_dalek::{SecretKey, SigningKey};
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};
use swarm::dht::StorbDHT;
use tracing::info;

#[derive(Debug)]
pub enum NeuronError {
    SubtensorError(String),
    ConfigError(String),
}

impl std::fmt::Display for NeuronError {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NeuronError::SubtensorError(e) => write!(f, "Subtensor error: {}", e),
            NeuronError::ConfigError(e) => write!(f, "Configuration error: {}", e),
        }
    }
}

impl std::error::Error for NeuronError {}

pub struct Version {
    pub major: u16,
    pub minor: u16,
    pub patch: u16,
}

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
    pub file: String,
    pub bootstrap_ip: String,
    pub bootstrap_port: u16,
}

#[derive(Clone)]
pub struct BaseNeuronConfig {
    // pub version: Version,
    pub netuid: u16,
    pub external_ip: String,
    pub api_port: u16,
    pub post_ip: bool,

    pub wallet_path: String,
    pub wallet_name: String,
    pub hotkey_name: String,

    pub mock: bool,

    pub load_old_nodes: bool,
    pub min_stake_threshold: u64,

    pub db_dir: PathBuf,
    pub pem_file: String,

    pub subtensor: SubtensorConfig,

    pub neuron: NeuronConfig,

    pub dht: DhtConfig,
}

#[derive(Clone)]
pub struct BaseNeuron {
    pub config: BaseNeuronConfig,
    pub neurons: Arc<RwLock<Vec<NeuronInfoLite<AccountId>>>>,
    pub signer: Signer,
    pub subtensor: Subtensor,
}

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

    pub async fn new(config: BaseNeuronConfig) -> Result<Self, NeuronError> {
        let subtensor =
            Self::get_subtensor_connection(config.subtensor.insecure, &config.subtensor.address)
                .await?;

        let wallet_path: PathBuf = PathBuf::from(config.wallet_path.clone());

        let hotkey_location: PathBuf = hotkey_location(
            wallet_path,
            config.wallet_name.clone(),
            config.hotkey_name.clone(),
        );

        info!("Loading hotkey: path = {:?}", hotkey_location);

        let seed = load_key_seed(&hotkey_location).unwrap();

        let signer = signer_from_seed(&seed).unwrap();

        let mut neuron = BaseNeuron {
            config: config.clone(),
            subtensor,
            neurons: Arc::new(RwLock::new(Vec::new())),
            signer,
        };

        // sync metagraph
        neuron.sync_metagraph().await;
        // check registration status
        neuron.check_registration().await?;
        // post ip if specified
        info!("Should post ip?: {}", config.post_ip);
        if config.post_ip {
            // TODO: using Udp for now
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

        let secret_key = SecretKey::from(seed);
        let keypair_bytes = SigningKey::from(secret_key).to_bytes();
        let libp2p_keypair = libp2p::identity::Keypair::ed25519_from_bytes(keypair_bytes)
            .unwrap_or_else(|_| {
                panic!("Failed to create libp2p keypair from bytes");
            });

        let dht = StorbDHT::new(config.db_dir.clone(), config.dht.port, libp2p_keypair)
            .expect("Failed to create StorbDHT instance");

        tokio::spawn(async {
            dht.run().await.expect("Failed to run StorbDHT");
        });

        Ok(neuron)
    }

    fn find_neuron_info<'a>(
        neurons: &'a [NeuronInfoLite<AccountId>],
        account_id: &AccountId,
    ) -> Option<&'a NeuronInfoLite<AccountId>> {
        neurons.iter().find(|neuron| &neuron.hotkey == account_id)
    }

    pub fn get_neurons(&self) -> Arc<RwLock<Vec<NeuronInfoLite<AccountId>>>> {
        self.neurons.clone()
    }

    /// Check whether the neuron is registered in the subnet or not
    pub async fn check_registration(&self) -> Result<(), NeuronError> {
        let neurons = self.neurons.read().unwrap();
        let neuron_info = Self::find_neuron_info(&neurons, self.signer.account_id());
        match neuron_info {
            Some(_) => Ok(()),
            None => Err(NeuronError::ConfigError(
                "Neuron is not registered".to_string(),
            )),
        }
    }

    // TODO: Remove unwraps and handle errors properly
    /// Synchronise the local metagraph state with chain
    pub async fn sync_metagraph(&mut self) {
        let current_block = self.subtensor.blocks().at_latest().await.unwrap();
        let runtime_api = self.subtensor.runtime_api().at(current_block.reference());

        // TODO: do this? specifically with needing to pass &self
        let neurons_payload =
            NeuronInfoRuntimeApi::get_neurons_lite(&NeuronInfoRuntimeApi {}, self.config.netuid);

        // let neurons = call_runtime_api_decoded(&runtime_api, neurons_payload)
        //     .await
        //     .unwrap();

        let neurons = runtime_api.call(neurons_payload).await.unwrap();

        *self.neurons.write().unwrap() = neurons;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::fs::{self, create_dir_all};
    // use std::path::Path;

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
            netuid: 1,
            external_ip: "127.0.0.1".to_string(),
            api_port: 8080,
            post_ip: false,
            wallet_path: wallet_path.to_str().unwrap().to_string(),
            wallet_name,
            hotkey_name: "test_hotkey".to_string(),
            mock: false,
            load_old_nodes: false,
            min_stake_threshold: 0,
            db_dir: "./test.db".into(),
            pem_file: "cert.pem".to_string(),
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
                file: "dht.db".to_string(),
                bootstrap_ip: "127.0.0.1".to_string(),
                bootstrap_port: 8082,
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

    #[tokio::test]
    async fn test_find_neuron_info() {
        let neurons = vec![];
        let account_id = AccountId::from([0; 32]);
        info!("account_id: {account_id}");
        let result = BaseNeuron::find_neuron_info(&neurons, &account_id);
        assert!(result.is_none());
    }

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
