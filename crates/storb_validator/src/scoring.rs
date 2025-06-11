use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use base::memory_db::MemoryDb;
use ndarray::{array, s, Array, Array1};
use rusqlite::params;
use serde::{Deserialize, Serialize};
use tracing::{debug, error, info, warn};

/// ScoreState stores the scores for each miner.
///
/// The final EMA score consists of the weighted sum of the normalized response
/// rate and challenge scores.
///
/// The response rate /// and challenge statistics are stored in an SQLite database.
/// Those stats are calculated then saved into the EMA score.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScoreState {
    /// The current exponential moving average (EMA) score of the miners.
    pub ema_scores: Array1<f64>,
    /// Previous EMA scores before reset, used as baseline
    pub previous_scores: Array1<f64>,
}

pub struct ScoringSystem {
    /// Database connection pool for the given DB driver.
    pub db: Arc<MemoryDb>,
    /// Path to the state file.
    pub state_file: PathBuf,
    /// Score state for miners.
    pub state: ScoreState,
    pub initial_alpha: f64,
    pub initial_beta: f64,
    // forgetting factor for challenges
    pub lambda: f64,
}

#[inline]
pub fn normalize_min_max(arr: &Array1<f64>) -> Array1<f64> {
    let min = arr.iter().cloned().fold(f64::INFINITY, f64::min);
    let max = arr.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

    if (max - min).abs() < 1e-9 {
        return arr.mapv(|_| 0.0); // Avoid division by zero
    }

    arr.mapv(|x| (x - min) / (max - min))
}

fn get_new_alpha_beta(
    beta: f64,
    alpha: f64,
    lambda: f64,
    weight: f64,
    success: bool,
) -> (f64, f64) {
    let v = if success { 1.0 } else { 0.0 };
    let new_alpha = lambda * alpha + weight * (1.0 + v) / 2.0;
    let new_beta = lambda * beta + weight * (1.0 - v) / 2.0;
    (new_beta, new_alpha)
}

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

        if !fs::exists(scoring_state_file)? {
            warn!(
                "Score state file did not exist at location {:?}. Created a new file as a result",
                &scoring_state_file
            );
            File::create(scoring_state_file)?;
        }

        // create new MemoryDb
        let db = Arc::new(MemoryDb::new(db_path_str).await?);

        let state = ScoreState {
            ema_scores: array![],
            previous_scores: array![],
        };

        let mut scoring_system = Self {
            db,
            state,
            state_file: scoring_state_file.to_path_buf(),
            // TODO(scoring): make these configurable
            initial_alpha: 500.0,
            initial_beta: 1000.0,
            lambda: 0.99, // forgetting factor for challenges
        };

        match scoring_system.load_state() {
            Ok(_) => debug!("Loaded state successfully"),
            Err(e) => {
                error!("Could not load the state: {}", e);
            }
        }

        Ok(scoring_system)
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

    /// Reset all statistics for a given miner UID in the database
    async fn reset_miner_stats(&self, uid: usize) -> Result<(), rusqlite::Error> {
        let db = self.db.clone();
        let conn = db.conn.lock().await;
        // TODO(scoring): set the alpha and beta to be configurable/constants?
        conn.execute(
            "UPDATE miner_stats SET 
                alpha = 500.0,
                beta = 1000.0,
                store_successes = 0,
                store_attempts = 0,
                retrieval_successes = 0,
                retrieval_attempts = 0,
                total_successes = 0
            WHERE miner_uid = ?",
            [uid],
        )?;
        debug!("Reset stats for UID {} in database", uid);
        Ok(())
    }

    pub async fn update_alpha_beta_db(
        &mut self,
        miner_uid: u16,
        weight: f64,
        success: bool,
    ) -> Result<(), rusqlite::Error> {
        let conn = self.db.conn.lock().await;
        // TODO(scoring): set the alpha and beta to be configurable/constants?
        // get current alpha and beta from the database
        let (alpha, beta): (f64, f64) = conn.query_row(
            "SELECT alpha, beta FROM miner_stats WHERE miner_uid = ?",
            [miner_uid],
            |row| Ok((row.get(0)?, row.get(1)?)),
        )?;

        // calculate new alpha and beta
        let (new_beta, new_alpha) = get_new_alpha_beta(beta, alpha, 0.99, weight, success);
        // update alpha and beta in the database
        conn.execute(
            "UPDATE miner_stats SET alpha = ?1, beta = ?2 WHERE miner_uid = ?3",
            params![new_alpha, new_beta, miner_uid],
        )?;
        debug!(
            "Updated alpha and beta for miner UID {}: alpha = {}, beta = {}",
            miner_uid, new_alpha, new_beta
        );
        Ok(())
    }

    /// Update scores for each of the miners.
    pub async fn update_scores(&mut self, neuron_count: usize, uids_to_update: Vec<u16>) {
        let extend_array = |old: &Array1<f64>, new_size: usize| -> Array1<f64> {
            let mut new_array = Array::<f64, _>::zeros(new_size);
            new_array.slice_mut(s![0..old.len()]).assign(old);
            new_array
        };

        info!("uids to update: {:?}", uids_to_update);

        // Only extend/initialize if current arrays are empty or new size is larger
        let current_size = self.state.ema_scores.len();
        if current_size < neuron_count {
            let state = &self.state;

            let mut new_ema_scores = extend_array(&state.ema_scores, neuron_count);
            let mut new_previous_scores = extend_array(&state.previous_scores, neuron_count);

            // Initialize new UIDs and reset scores for updated UIDs
            for uid in uids_to_update.iter() {
                let uid = *uid as usize;
                new_ema_scores[uid] = 0.0;
                new_previous_scores[uid] = 0.0;

                // Reset database stats
                if let Err(e) = self.reset_miner_stats(uid).await {
                    error!("Failed to reset stats for UID {} in database: {}", uid, e);
                }
            }

            self.state.ema_scores = new_ema_scores;
            self.state.previous_scores = new_previous_scores;
        } else {
            // If we don't need to extend arrays, just reset scores for updated UIDs
            for uid in uids_to_update.iter() {
                let uid = *uid as usize;
                self.state.ema_scores[uid] = 0.0;
                self.state.previous_scores[uid] = 0.0;

                // Reset database stats
                if let Err(e) = self.reset_miner_stats(uid).await {
                    error!("Failed to reset stats for UID {} in database: {}", uid, e);
                }
            }
        }

        // Rest of the existing update_scores implementation...
        // connect to db and compute new scores
        let state = &mut self.state;

        let mut response_rate_scores = Array1::<f64>::zeros(state.ema_scores.len());
        for (miner_uid, _) in state.ema_scores.iter().enumerate() {
            let db = self.db.clone();
            let conn = db.conn.lock().await;

            // TODO: error handling
            let alpha: f64 = conn
                .query_row(
                    "SELECT alpha FROM miner_stats WHERE miner_uid = ?",
                    [miner_uid],
                    |row| row.get(0),
                )
                .unwrap_or(500.0);
            let beta: f64 = conn
                .query_row(
                    "SELECT beta FROM miner_stats WHERE miner_uid = ?",
                    [miner_uid],
                    |row| row.get(0),
                )
                .unwrap_or(1000.0);

            response_rate_scores[miner_uid] = alpha / (alpha + beta);
        }

        // zero out nans in response rate scores
        let nan_indices: Vec<_> = response_rate_scores
            .iter()
            .enumerate()
            .filter(|(_, &score)| score.is_nan())
            .map(|(i, _)| i)
            .collect();

        for i in nan_indices {
            response_rate_scores[i] = 0.0;
        }

        state.ema_scores = response_rate_scores.clone();

        info!("response rate scores: {}", response_rate_scores);
        info!("new scores: {}", state.ema_scores);

        match self.save_state() {
            Ok(_) => info!("Saved state successfully"),
            Err(e) => {
                error!("Could not save the state: {}", e);
                // TODO: submit to telemetry
            }
        }
    }
}
