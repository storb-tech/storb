use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use ndarray::{array, Array1};
use sqlx::sqlite::SqlitePoolOptions;
use sqlx::{Pool, Sqlite};

const DB_MAX_CONNECTIONS: u32 = 5;

/// ScoreState stores the scores for each miner.
///
/// The final EMA score consists of the weighted sum of the normalized response
/// rate, latency, and challenge scores.
///
/// The latency scores are stored in the score state file, although response rate
/// and challenge statistics are stored in a separate SQLite database. Those stats
/// are calculated then saved into the EMA score.
#[derive(Clone)]
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
    pub db_conn: Pool<Sqlite>,
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
        let path = PathBuf::new().join(db_file);
        let path_str = path.to_str().context("Could not convert path to string")?;
        let sqlite_url = format!("sqlite://{}", path_str);

        // if !fs::exists(path) {
        //     //
        // }

        let db_conn = SqlitePoolOptions::new()
            .max_connections(DB_MAX_CONNECTIONS)
            .connect(sqlite_url.as_str())
            .await?;

        let state = ScoreState {
            ema_scores: array![],
            retrieve_latencies: array![],
            store_latencies: array![],
            retrieve_latency_scores: array![],
            store_latency_scores: array![],
            final_latency_scores: array![],
        };

        Ok(Self {
            db_conn,
            state,
            state_file: scoring_state_file.to_path_buf(),
        })
    }

    // TODO: implement saving and loading state
    fn load_state(&self) {

        // bincode::deserialize(bytes)
    }
    fn save_state(&self) {}

    /// Load miner stats from the database.
    fn load_stats() {}

    /// Save miner stats into the database.
    fn save_stats() {}
}
