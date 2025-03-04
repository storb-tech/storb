use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use anyhow::{Context, Result};
use libp2p::kad::RecordKey;
use ndarray::{array, s, Array, Array1};
use rand::rngs::StdRng;
use rand::{Rng, SeedableRng};
use rusqlite::Connection;
use serde::{Deserialize, Serialize};
use thiserror::Error;
use tokio::sync::Mutex;
use tracing::{debug, error, info, warn};

use crate::db::MemoryDb;

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
#[derive(Clone, Deserialize, Serialize)]
pub struct ScoreState {
    /// The current exponential moving average (EMA) score of the miners.
    pub ema_scores: Array1<f64>,
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

// #[inline]
// pub fn normalize_min_max(arr: &Array1<f64>) -> Array1<f64> {
//     let min = arr.iter().cloned().fold(f64::INFINITY, f64::min);
//     let max = arr.iter().cloned().fold(f64::NEG_INFINITY, f64::max);

//     if (max - min).abs() < 1e-9 {
//         return arr.mapv(|_| 0.0); // Avoid division by zero
//     }

//     arr.mapv(|x| (x - min) / (max - min))
// }

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
    let target_row: u64 = rng.gen_range(1..=row_count);

    debug!("Getting chunk at row {target_row} from sqlite db");

    let chosen_chunk_vec: Vec<u8> = db_conn
        .lock()
        .await
        .query_row(
            "SELECT chunk_hash FROM chunks ORDER BY chunk_hash LIMIT 1 OFFSET ?",
            [target_row],
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
            retrieve_latency_scores: array![],
            store_latency_scores: array![],
            final_latency_scores: array![],
        };

        let mut scoring_system = Self {
            db,
            state,
            state_file: scoring_state_file.to_path_buf(),
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

    /// Update scores for each of the miners.
    pub async fn update_scores(&mut self, neuron_count: usize, uids_to_update: Vec<u16>) {
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
                new_retrieve_latency_scores[uid] = 0.0;
                new_store_latency_scores[uid] = 0.0;
                new_final_latency_scores[uid] = 0.0;
            }

            state.ema_scores = new_ema_scores;
            state.retrieve_latency_scores = new_retrieve_latency_scores;
            state.store_latency_scores = new_store_latency_scores;
            state.final_latency_scores = new_final_latency_scores;
        }

        // connect to db and compute new scores
        let state = &self.state;

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
            let store_rate = store_successes / store_attempts;

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
            let retrieval_rate = retrieval_successes / retrieval_attempts;

            response_rate_scores[miner_uid] = 0.5 * store_rate + 0.5 * retrieval_rate;
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

    /// Set weights for each miner to publish to the chain.
    pub fn set_weights(&mut self) {}
}

#[cfg(test)]
mod tests {
    use super::*;
    use rusqlite::Connection;
    use std::sync::Arc;
    use tokio::sync::Mutex;

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
