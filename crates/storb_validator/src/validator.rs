use base::{BaseNeuron, BaseNeuronConfig, NeuronError};
use tracing::info;

#[derive(Clone)]
pub struct ValidatorConfig {
    pub neuron_config: BaseNeuronConfig,
}

/// The Storb validator
#[derive(Clone)]
pub struct Validator {
    pub config: ValidatorConfig,
    pub neuron: BaseNeuron,
}

impl Validator {
    pub async fn new(config: ValidatorConfig) -> Result<Self, NeuronError> {
        let neuron_config = config.neuron_config.clone();
        let neuron = BaseNeuron::new(neuron_config).await?;
        let validator = Validator { config, neuron };
        Ok(validator)
    }

    pub async fn sync(&mut self) {
        info!("Syncing validator");
        self.neuron.sync_metagraph().await;
        info!("Done syncing validator");
    }
}
