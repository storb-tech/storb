use crabtensor::{
    api::apis,
    axon::{serve_axon_payload, AxonProtocol},
    rpc::{call_runtime_api_decoded, types::NeuronInfoLite},
    subtensor::Subtensor,
    wallet::{hotkey_location, load_key_seed, signer_from_seed, Signer},
    AccountId,
};
use std::{
    path::PathBuf,
    sync::{Arc, RwLock},
};
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

    pub db_dir: String,
    pub pem_file: String,

    pub subtensor: SubtensorConfig,

    pub neuron: NeuronConfig,

    pub dht: DhtConfig,
}

#[derive(Clone)]
pub struct BaseNeuron {
    pub config: BaseNeuronConfig,
    pub neurons: Arc<RwLock<Vec<NeuronInfoLite>>>,
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
            // TODO: using Tcp for now
            let address = format!("{}:{}", config.external_ip, config.api_port)
                .parse()
                .unwrap();
            info!("Serving axon as: {}", address);
            let payload = serve_axon_payload(config.netuid, address, AxonProtocol::Tcp);
            neuron
                .subtensor
                .tx()
                .sign_and_submit_default(&payload, &neuron.signer)
                .await
                .unwrap();
            info!("Successfully served axon!");
        }

        Ok(neuron)
    }

    fn find_neuron_info<'a>(
        neurons: &'a [NeuronInfoLite],
        account_id: &AccountId,
    ) -> Option<&'a NeuronInfoLite> {
        neurons.iter().find(|neuron| &neuron.hotkey == account_id)
    }

    pub fn get_neurons(&self) -> Arc<RwLock<Vec<NeuronInfoLite>>> {
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

    /// Synchronise the local metagraph state with chain
    pub async fn sync_metagraph(&mut self) {
        let current_block = self.subtensor.blocks().at_latest().await.unwrap();
        let runtime_api = self.subtensor.runtime_api().at(current_block.reference());

        let neurons_payload = apis()
            .neuron_info_runtime_api()
            .get_neurons_lite(self.config.netuid);

        let neurons = call_runtime_api_decoded(&runtime_api, neurons_payload)
            .await
            .unwrap();

        *self.neurons.write().unwrap() = neurons;
    }
}
