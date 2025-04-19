use std::path::PathBuf;

use base::sync::Synchronizable;
use base::{BaseNeuron, BaseNeuronConfig, NeuronError};
use tracing::info;

#[derive(Clone)]
pub struct MinerConfig {
    pub neuron_config: BaseNeuronConfig,
    pub store_dir: PathBuf,
}

/// The Storb miner.
#[derive(Clone)]
pub struct Miner {
    pub config: MinerConfig,
    pub neuron: BaseNeuron,
}

impl Miner {
    pub async fn new(config: MinerConfig) -> Result<Self, NeuronError> {
        let neuron_config = config.neuron_config.clone();
        let neuron = BaseNeuron::new(neuron_config, None).await?;
        let miner = Miner { config, neuron };
        Ok(miner)
    }

    /// Sync the miner with the metagraph.
    pub async fn sync(&mut self) -> Result<(), Box<dyn std::error::Error>> {
        info!("Syncing miner");
        match self.neuron.sync_metagraph().await {
            Ok(_) => {
                info!("Sync completed!!!");
            }
            Err(e) => {
                info!("Sync failed: {:?}", e);
            }
        }
        info!("Done syncing miner");

        Ok(())
    }
}
