use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use base::memory_db::MemoryDb;
use libp2p::kad::RecordKey;
use ndarray::{array, s, Array, Array1};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::constants::{MIN_REQUESTS_FOR_SCORE, STATS_RESET_THRESHOLD, Z_SCORE, Z_SQUARED};

// Add these error types at the top of the file
#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("No chunks available in database")]
    EmptyTable,
    #[error("Invalid chunk size: expected 32 bytes, got {0}")]
    InvalidSize(usize),
    #[error("Database error: {0}")]
    Database(#[from] rusqlite::Error),
}

/// ScoreState stores the scores for each miner.
///
/// The final EMA score consists of the weighted sum of the normalized response
/// rate, latency, and challenge scores.
///
/// The latency scores are stored in the score state file, although response rate
/// and challenge statistics are stored in a separate SQLite database. Those stats
/// are calculated then saved into the EMA score.
#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct ScoreState {
    /// The current exponential moving average (EMA) score of the miners.
    pub ema_scores: Array1<f64>,
    /// The EMA score of retrieve latencies.
    pub retrieve_latency_scores: Array1<f64>,
    /// The EMA score of store latencies.
    pub store_latency_scores: Array1<f64>,
    /// Combination of retrieve and store scores.
    pub final_latency_scores: Array1<f64>,
    /// Previous EMA scores before reset, used as baseline
    pub previous_scores: Array1<f64>,
    /// Request counters per miner
    pub request_counters: Array1<u32>,
}

impl ScoreState {
    fn normalize_latencies(latencies: &HashMap<u16, f64>) -> HashMap<u16, f64> {
        if latencies.is_empty() {
            return HashMap::new();
        }

        // Find min and max latencies
        let mut min_latency = f64::MAX;
        let mut max_latency = f64::MIN;

        for &latency in latencies.values() {
            min_latency = min_latency.min(latency);
            max_latency = max_latency.max(latency);
        }

        // Normalize and invert (so faster = higher score)
        let range = max_latency - min_latency;
        latencies
            .iter()
            .map(|(&uid, &latency)| {
                let normalized = if range > 0.0 {
                    1.0 - ((latency - min_latency) / range)
                } else {
                    1.0
                };
                (uid, normalized)
            })
            .collect()
    }

    pub fn update_latency_scores(
        &mut self,
        store_latencies: HashMap<u16, f64>,
        retrieval_latencies: HashMap<u16, f64>,
        alpha: f64,
    ) {
        // Get normalized scores
        let normalized_store = Self::normalize_latencies(&store_latencies);
        let normalized_retrieval = Self::normalize_latencies(&retrieval_latencies);

        // Update arrays with new EMA scores
        for (uid, score) in normalized_store {
            if uid < self.store_latency_scores.len() as u16 {
                // EMA = alpha * new_value + (1 - alpha) * previous_ema
                let old_score = self.store_latency_scores[uid as usize];
                self.store_latency_scores[uid as usize] = alpha * score + (1.0 - alpha) * old_score;
            }
        }

        for (uid, score) in normalized_retrieval {
            if uid < self.retrieve_latency_scores.len() as u16 {
                let old_score = self.retrieve_latency_scores[uid as usize];
                self.retrieve_latency_scores[uid as usize] =
                    alpha * score + (1.0 - alpha) * old_score;
            }
        }

        // Update final latency scores (50/50 weight between store and retrieve)
        for i in 0..self.final_latency_scores.len() {
            self.final_latency_scores[i] =
                0.5 * self.store_latency_scores[i] + 0.5 * self.retrieve_latency_scores[i];
        }
    }
}

pub struct ScoringSystem {
    /// Database connection pool for the given DB driver.
    pub db: Arc<MemoryDb>,
    /// Path to the state file.
    pub state_file: PathBuf,
    /// Score state for miners.
    pub state: ScoreState,
    /// Moving average alpha for scores
    pub moving_average_alpha: f64,
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

#[allow(dead_code)]
pub async fn select_random_chunk_from_db(
    db_conn: Arc<Mutex<Connection>>,
) -> Result<RecordKey, ChunkError> {
    // Get row count
    let row_count: u64 = db_conn
        .lock()
        .await
        .query_row("SELECT COUNT(*) FROM chunks", [], |row| row.get(0))
        .map_err(ChunkError::Database)?;

    debug!("Num. entries in sqlite db: {row_count}");

    // Check for empty table
    if row_count == 0 {
        return Err(ChunkError::EmptyTable);
    }

    let mut rng: StdRng = SeedableRng::from_entropy();
    let offset_rows_by: u64 = rng.gen_range(0..row_count);

    let row_idx = offset_rows_by + 1;
    debug!("Getting chunk at row {row_idx} from sqlite db");

    let chosen_chunk_vec: Vec<u8> = db_conn
        .lock()
        .await
        .query_row(
            "SELECT chunk_hash FROM chunks ORDER BY chunk_hash LIMIT 1 OFFSET ?",
            [offset_rows_by],
            |row| row.get(0),
        )
        .map_err(ChunkError::Database)?;

    debug!("Chosen chunk: {:?}", chosen_chunk_vec);

    // Validate chunk size
    if chosen_chunk_vec.len() != 32 {
        return Err(ChunkError::InvalidSize(chosen_chunk_vec.len()));
    }

    // Convert to fixed size array
    let vec_len = chosen_chunk_vec.len();
    let chosen_chunk_raw: [u8; 32] = chosen_chunk_vec
        .try_into()
        .map_err(|_| ChunkError::InvalidSize(vec_len))?;

    Ok(RecordKey::new(&chosen_chunk_raw))
}

/// Calculates the Lower Confidence Bound (LCB) of a binomial proportion
/// using the Wilson Score Interval.
///
/// # Arguments
/// * `successes` - The number of successful trials.
/// * `attempts` - The total number of trials (n).
///
/// # Returns
/// The lower bound of the confidence interval for the success rate.
/// Returns 0.0 if attempts are 0.
fn calculate_wilson_lcb(successes: f64, attempts: f64) -> f64 {
    if attempts <= 0.0 {
        return 0.0; // No data, return lowest score
    }
    if successes < 0.0 || successes > attempts {
        // Handle potential invalid input data gracefully, though ideally this shouldn't happen
        warn!(
            "Invalid input for LCB: successes={}, attempts={}, returning 0",
            successes, attempts
        );
        return 0.0;
    }

    let p_hat = successes / attempts; // Observed success rate

    let numerator = p_hat + Z_SQUARED / (2.0 * attempts)
        - Z_SCORE
            * ((p_hat * (1.0 - p_hat) / attempts) + Z_SQUARED / (4.0 * attempts * attempts)).sqrt();

    let denominator = 1.0 + Z_SQUARED / attempts;

    // Clamp the result between 0 and 1, as floating point inaccuracies could theoretically push it slightly outside.
    (numerator / denominator).clamp(0.0, 1.0)
}

impl ScoringSystem {
    pub async fn new(
        db_file: &PathBuf,
        scoring_state_file: &Path,
        moving_average_alpha: f64,
    ) -> Result<Self> {
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
            retrieve_latency_scores: array![],
            store_latency_scores: array![],
            final_latency_scores: array![],
            previous_scores: array![],
            request_counters: array![],
        };

        let mut scoring_system = Self {
            db,
            state,
            state_file: scoring_state_file.to_path_buf(),
            moving_average_alpha,
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
        conn.execute(
            "UPDATE miner_stats SET 
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

    /// Used to increment request counter when miner is sent a request. Return true if stats were reset.
    pub async fn increment_request_counter(&mut self, uid: usize) -> Result<bool> {
        if uid >= self.state.request_counters.len() {
            return Ok(false);
        }

        self.state.request_counters[uid] += 1;

        // Check if we should reset stats
        if self.state.request_counters[uid] >= STATS_RESET_THRESHOLD {
            // Store current score before reset
            self.state.previous_scores[uid] = self.state.ema_scores[uid];

            // Reset database stats
            self.reset_miner_stats(uid).await?;
            self.state.request_counters[uid] = 0;

            return Ok(true);
        }

        Ok(false)
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
            let mut new_retrieve_latency_scores =
                extend_array(&state.retrieve_latency_scores, neuron_count);
            let mut new_store_latency_scores =
                extend_array(&state.store_latency_scores, neuron_count);
            let mut new_final_latency_scores =
                extend_array(&state.final_latency_scores, neuron_count);
            let mut new_previous_scores = extend_array(&state.previous_scores, neuron_count);
            let mut new_request_counters =
                extend_array(&state.request_counters.mapv(|x| x as f64), neuron_count)
                    .mapv(|x| x as u32);

            // Initialize new UIDs and reset scores for updated UIDs
            for uid in uids_to_update.iter() {
                let uid = *uid as usize;
                new_ema_scores[uid] = 0.0;
                new_retrieve_latency_scores[uid] = 0.0;
                new_store_latency_scores[uid] = 0.0;
                new_final_latency_scores[uid] = 0.0;
                new_previous_scores[uid] = 0.0;
                new_request_counters[uid] = 0;

                // Reset database stats
                if let Err(e) = self.reset_miner_stats(uid).await {
                    error!("Failed to reset stats for UID {} in database: {}", uid, e);
                }
            }

            self.state.ema_scores = new_ema_scores;
            self.state.retrieve_latency_scores = new_retrieve_latency_scores;
            self.state.store_latency_scores = new_store_latency_scores;
            self.state.final_latency_scores = new_final_latency_scores;
            self.state.previous_scores = new_previous_scores;
            self.state.request_counters = new_request_counters;
        } else {
            // If we don't need to extend arrays, just reset scores for updated UIDs
            for uid in uids_to_update.iter() {
                let uid = *uid as usize;
                self.state.ema_scores[uid] = 0.0;
                self.state.retrieve_latency_scores[uid] = 0.0;
                self.state.store_latency_scores[uid] = 0.0;
                self.state.final_latency_scores[uid] = 0.0;
                self.state.previous_scores[uid] = 0.0;
                self.state.request_counters[uid] = 0;

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
            let store_successes: f64 = conn
                .query_row(
                    "SELECT store_successes FROM miner_stats WHERE miner_uid = ?",
                    [miner_uid],
                    |row| row.get(0),
                )
                .unwrap();
            let store_attempts: f64 = conn
                .query_row(
                    "SELECT store_attempts FROM miner_stats WHERE miner_uid = ?",
                    [miner_uid],
                    |row| row.get(0),
                )
                .unwrap();

            let retrieval_successes: f64 = conn
                .query_row(
                    "SELECT retrieval_successes FROM miner_stats WHERE miner_uid = ?",
                    [miner_uid],
                    |row| row.get(0),
                )
                .unwrap();
            let retrieval_attempts: f64 = conn
                .query_row(
                    "SELECT retrieval_attempts FROM miner_stats WHERE miner_uid = ?",
                    [miner_uid],
                    |row| row.get(0),
                )
                .unwrap();

            let total_attempts = store_attempts + retrieval_attempts;

            // Get the previous score before the mutable borrow
            let previous_score = state.previous_scores[miner_uid];

            if total_attempts < MIN_REQUESTS_FOR_SCORE as f64 {
                // Use previous score for new/recently reset miners
                response_rate_scores[miner_uid] = previous_score;
            } else {
                // Calculate new Wilson score
                let store_lcb = calculate_wilson_lcb(store_successes, store_attempts);
                let retrieval_lcb = calculate_wilson_lcb(retrieval_successes, retrieval_attempts);

                // Combine with historical performance
                let current_score = 0.5 * store_lcb + 0.5 * retrieval_lcb;

                // Weight current performance more heavily
                response_rate_scores[miner_uid] = 0.7 * current_score + 0.3 * previous_score;
            }
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

        // Calculate final EMA scores: 75% weight for response rates (store/retrieval success)
        // and 25% weight for latency performance. Response rates prioritized as they indicate
        // basic miner availability and reliability.
        state.ema_scores =
            0.75 * response_rate_scores.clone() + 0.25 * state.final_latency_scores.clone();

        info!("response rate scores: {}", response_rate_scores);
        info!("new scores: {}", state.ema_scores);
        info!(
            "new retrieve_latency_scores: {}",
            state.retrieve_latency_scores
        );
        info!("new store_latency_scores: {}", state.store_latency_scores);
        info!("new final_latency_scores: {}", state.final_latency_scores);

        match self.save_state() {
            Ok(_) => info!("Saved state successfully"),
            Err(e) => {
                error!("Could not save the state: {}", e);
                // TODO: submit to telemetry
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use rusqlite::Connection;
    use tokio::sync::Mutex;

    use super::*;

    async fn setup_test_db() -> Arc<Mutex<Connection>> {
        let conn = Connection::open_in_memory().unwrap();

        // Create test table and insert sample data
        conn.execute("CREATE TABLE chunks (chunk_hash BLOB NOT NULL)", [])
            .unwrap();

        // Insert 3 sample chunks
        let test_chunks = vec![vec![1u8; 32], vec![2u8; 32], vec![3u8; 32]];

        for chunk in test_chunks {
            conn.execute("INSERT INTO chunks (chunk_hash) VALUES (?)", [chunk])
                .unwrap();
        }

        Arc::new(Mutex::new(conn))
    }

    #[tokio::test]
    async fn test_select_random_chunk_success() {
        let db_conn = setup_test_db().await;

        // Call function multiple times to test randomness
        for _ in 0..10 {
            let result = select_random_chunk_from_db(db_conn.clone()).await;
            assert!(result.is_ok());
            let key = result.unwrap();
            assert_eq!(key.as_ref().len(), 32); // Verify key length
        }
    }

    #[tokio::test]
    async fn test_select_random_chunk_empty_table() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE chunks (chunk BLOB NOT NULL)", [])
            .unwrap();
        let db_conn = Arc::new(Mutex::new(conn));

        let result = select_random_chunk_from_db(db_conn).await;
        assert!(matches!(result.unwrap_err(), ChunkError::EmptyTable));
    }

    #[tokio::test]
    async fn test_select_random_chunk_invalid_data() {
        let conn = Connection::open_in_memory().unwrap();
        conn.execute("CREATE TABLE chunks (chunk_hash BLOB NOT NULL)", [])
            .unwrap();

        // Insert invalid chunk hash (wrong size)
        conn.execute(
            "INSERT INTO chunks (chunk_hash) VALUES (?)",
            [vec![1u8; 16]], // Only 16 bytes instead of 32
        )
        .unwrap();

        let db_conn = Arc::new(Mutex::new(conn));
        let result = select_random_chunk_from_db(db_conn).await;
        assert!(matches!(result.unwrap_err(), ChunkError::InvalidSize(16)));
    }
}
