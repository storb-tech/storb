use std::path::PathBuf;
use std::sync::Arc;

use base::sync::Synchronizable;
use base::{BaseNeuron, BaseNeuronConfig, NeuronError};
use tokio::sync::RwLock;
use tracing::info;

use crate::scoring::ScoringSystem;

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
