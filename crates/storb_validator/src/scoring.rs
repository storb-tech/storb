use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use ndarray::{array, s, Array, Array1};
use serde::{Deserialize, Serialize};
use tracing::{info, warn};

use crate::db::MemoryDb;

/// ScoreState stores the scores for each miner.
///
/// The final EMA score consists of the weighted sum of the normalized response
/// rate, latency, and challenge scores.
///
/// The latency scores are stored in the score state file, although response rate
/// and challenge statistics are stored in a separate SQLite database. Those stats
/// are calculated then saved into the EMA score.
#[derive(Clone, Deserialize, Serialize)]
pub struct ScoreState {
    /// The current exponential moving average (EMA) score of the miners.
    pub ema_scores: Array1<f64>,
    /// The average time (in ms) it takes to retrieve files.
    pub retrieve_latencies: Array1<f64>,
    /// The average time (in ms) it takes to store files.
    pub store_latencies: Array1<f64>,
    /// The EMA score of retrieve latencies.
    pub retrieve_latency_scores: Array1<f64>,
    /// The EMA score of store latencies.
    pub store_latency_scores: Array1<f64>,
    /// Combination of retrieve and store scores.
    pub final_latency_scores: Array1<f64>,
}

pub struct ScoringSystem {
    /// Database connection pool for the given DB driver.
    pub db: Arc<MemoryDb>,
    /// Path to the state file.
    pub state_file: PathBuf,
    /// Score state for miners.
    pub state: ScoreState,
}

// // General L_p norm
// fn lp_norm(arr: &Array1<f64>, p: f64) -> f64 {
//     arr.mapv(|x| x.abs().powf(p)).sum().powf(1.0 / p)
// }

impl ScoringSystem {
    pub async fn new(db_file: &PathBuf, scoring_state_file: &Path) -> Result<Self> {
        let db_path = PathBuf::new().join(db_file);
        if !fs::exists(&db_path)? {
            warn!(
                "Database file did not exist at location {:?}. Created a new file as a result",
                &db_path
            );
            File::create(&db_path)?;
        }
        let db_path_str = db_path
            .to_str()
            .context("Could not convert path to string")?;

        if !fs::exists(&scoring_state_file)? {
            warn!(
                "Score state file did not exist at location {:?}. Created a new file as a result",
                &scoring_state_file
            );
            File::create(&scoring_state_file)?;
        }

        // create new MemoryDb
        let db = Arc::new(MemoryDb::new(db_path_str).await?);

        let state = ScoreState {
            ema_scores: array![],
            retrieve_latencies: array![],
            store_latencies: array![],
            retrieve_latency_scores: array![],
            store_latency_scores: array![],
            final_latency_scores: array![],
        };

        Ok(Self {
            db,
            state,
            state_file: scoring_state_file.to_path_buf(),
        })
    }

    /// Load scores state from teh state file.
    fn load_state(&mut self) -> Result<()> {
        let buf: Vec<u8> = fs::read(&self.state_file)?;
        self.state = bincode::deserialize::<ScoreState>(&buf[..])?;

        Ok(())
    }

    /// Save scores state to the state file.
    fn save_state(&mut self) -> Result<()> {
        let buf = bincode::serialize(&self.state)?;
        fs::write(&self.state_file, buf)?;
        Ok(())
    }

    /// Load miner stats from the database.
    fn load_stats() {}

    /// Save miner stats into the database.
    fn save_stats() {}

    /// Submit latency data for the scoring system to use. The latency data
    /// will be incorporated accordingly.
    pub fn submit_latency_data(
        &mut self,
        retrieve_latencies: Option<Vec<f64>>,
        store_latencies: Option<Vec<f64>>,
    ) {
        //
    }

    /// Submit response rate data for the scoring system to use. The latency data
    /// will be incorporated accordingly.
    pub fn submit_response_rate_data(
        &mut self,
        retrieve_response_rates: Option<Vec<f64>>,
        store_response_rates: Option<Vec<f64>>,
    ) {
        //
    }

    /// Update scores for each of the miners.
    pub fn update_scores(&mut self, neuron_count: usize, uids_to_update: Vec<u16>) {
        // if # of neurons has changed we add new entries to scores
        // if a neuron has been replaced by another w zero out its scores

        // Create a new, extended array and copy the old contents into the new one.
        let extend_array = |old: &Array1<f64>, new_size: usize| -> Array1<f64> {
            let mut new_array = Array::<f64, _>::zeros(new_size);
            new_array.slice_mut(s![0..old.len()]).assign(old);
            new_array
        };

        info!("uids to update: {:?}", uids_to_update);
        // Update the scores state if we have to
        if !uids_to_update.is_empty() {
            let state = &mut self.state;

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

        let state = &self.state;
        info!("new scores: {}", state.ema_scores);
        info!("new retrieve latencies: {}", state.retrieve_latencies);
        info!("new store_latencies: {}", state.store_latencies);
        info!(
            "new retrieve_latency_scores: {}",
            state.retrieve_latency_scores
        );
        info!("new store_latency_scores: {}", state.store_latency_scores);
        info!("new final_latency_scores: {}", state.final_latency_scores);

        // score miners
        // Resp Rate
        // Latency
        // Request Challenge (part 2)
        // PDP Challenge (part 2)

        // etc.
    }

    /// Set weights for each miner to publish to the chain.
    pub fn set_weights(&mut self) {}
}
