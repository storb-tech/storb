use std::path::PathBuf;
use std::sync::Arc;

use base::sync::Synchronizable;
use base::{BaseNeuron, BaseNeuronConfig, NeuronError};
use ndarray::{s, Array, Array1};
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

        // if # of neurons has changed we add new entrie(s) to scores
        // if a neuron has been replaced by another we zero out its scores

        let uids_to_update = self.neuron.sync_metagraph().await?;
        let neuron_count = self.neuron.neurons.len();

        // Create a new, extended array and copy the old contents into the new one.
        let extend_array = |old: &Array1<f64>, new_size: usize| -> Array1<f64> {
            let mut new_array = Array::<f64, _>::zeros(new_size);
            new_array.slice_mut(s![0..old.len()]).assign(old);
            new_array
        };

        info!("uids to update: {:?}", uids_to_update);
        // Update the scores state if we have to
        if !uids_to_update.is_empty() {
            let state = &mut self.scoring_system.write().await.state;

            // TODO: if the array size remains the same then don't extend?

            let mut new_ema_scores = extend_array(&state.ema_scores, neuron_count);
            let mut new_retrieve_latencies = extend_array(&state.retrieve_latencies, neuron_count);
            let mut new_store_latencies = extend_array(&state.store_latencies, neuron_count);
            let mut new_retrieve_latency_scores =
                extend_array(&state.retrieve_latency_scores, neuron_count);
            let mut new_store_latency_scores =
                extend_array(&state.store_latency_scores, neuron_count);
            let mut new_final_latency_scores =
                extend_array(&state.final_latency_scores, neuron_count);

            // Reset or initialise UIDs
            for uid in uids_to_update {
                let uid = uid as usize;

                new_ema_scores[uid] = 0.0;
                new_retrieve_latencies[uid] = 0.0;
                new_store_latencies[uid] = 0.0;
                new_retrieve_latency_scores[uid] = 0.0;
                new_store_latency_scores[uid] = 0.0;
                new_final_latency_scores[uid] = 0.0;
            }

            state.ema_scores = new_ema_scores;
            state.retrieve_latencies = new_retrieve_latencies;
            state.store_latencies = new_store_latencies;
            state.retrieve_latency_scores = new_retrieve_latency_scores;
            state.store_latency_scores = new_store_latency_scores;
            state.final_latency_scores = new_final_latency_scores;
        }

        let state = &self.scoring_system.read().await.state;
        info!("new scores: {}", state.ema_scores);
        info!("new retrieve latencies: {}", state.retrieve_latencies);
        info!("new store_latencies: {}", state.store_latencies);
        info!(
            "new retrieve_latency_scores: {}",
            state.retrieve_latency_scores
        );
        info!("new store_latency_scores: {}", state.store_latency_scores);
        info!("new final_latency_scores: {}", state.final_latency_scores);

        info!("Done syncing validator");

        Ok(())
    }
}
