use std::time::Duration;

/// Buffer size for the metadatadb's MPSC channel.
pub const DB_MPSC_BUFFER_SIZE: usize = 100;

pub const METADATADB_SYNC_FREQUENCY: u64 = 600; // 5 minutes

pub const NONCE_CLEANUP_FREQUENCY: u64 = 240; // Every 2 minutes
pub const NONCE_EXPIRATION_TIME: u64 = 3600; // 60 minutes

pub const PIECE_REPAIR_FREQUENCY: u64 = 660; // Every 11 minutes

// TODO: should we increase min required miners?
// TODO: should we use it in consume_bytes to determine number of miners to distribute to?
// NOTE: see: https://github.com/storb-tech/storb/issues/66
pub const MIN_REQUIRED_MINERS: usize = 1;
pub const SYNTHETIC_CHALLENGE_FREQUENCY: u64 = 300;

pub const MAX_CHALLENGE_PIECE_NUM: i32 = 5;
pub const SYNTH_CHALLENGE_TIMEOUT: f64 = 1.0; // TODO: modify this
pub const SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE: f64 = 3.0;
pub const MIN_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10; // minimum size of synthetic data in bytes
pub const MAX_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10 * 10 * 10; // maximum size of synthetic data in bytes
pub const MAX_SYNTH_CHALLENGE_MINER_NUM: usize = 25; // maximum number of miners to challenge

// constants for MetadataDB
pub const DB_MAX_LIFETIME: u64 = 3600; // Close connections after 1 hour
pub const IDLE_TIMEOUT: u64 = 600; // Close connections after 10 minutes of inactivity
pub const CONNECTION_TIMEOUT: u64 = 30; // Timeout for establishing a connection
pub const TAO_IN_RAO: f64 = 1_000_000_000.0; // 1 TAO = 1,000,000,000 RAO

pub const INFO_API_RATE_LIMIT_DURATION: Duration = Duration::from_secs(60);
pub const INFO_API_RATE_LIMIT_MAX_REQUESTS: usize = 10;

// Initial values for alpha and beta used in the scoring system
// These were empirically derived to minimise reliable node churn
pub const INITIAL_ALPHA: f64 = 10.0;
pub const INITIAL_BETA: f64 = 20.0;
