use base::{BaseNeuron, BaseNeuronConfig, NeuronError};
use tracing::info;

#[derive(Clone)]
pub struct MinerConfig {
    pub neuron_config: BaseNeuronConfig,
    pub store_dir: String,
}

/// The Storb miner
#[derive(Clone)]
pub struct Miner {
    pub config: MinerConfig,
    pub neuron: BaseNeuron,
}

impl Miner {
    pub async fn new(config: MinerConfig) -> Result<Self, NeuronError> {
        let neuron_config = config.neuron_config.clone();
        let neuron = BaseNeuron::new(neuron_config).await?;
        let miner = Miner { config, neuron };
        Ok(miner)
    }

    pub async fn sync(&mut self) {
        info!("Syncing miner");
        self.neuron.sync_metagraph().await;
        info!("Done syncing miner");
    }
}
