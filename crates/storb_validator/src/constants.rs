use std::time::Duration;

// TODO: should we increase min required miners?
// TODO: should we use it in consume_bytes to determine number of miners to distribute to?
// NOTE: see: https://github.com/storb-tech/storb/issues/66
pub const MIN_REQUIRED_MINERS: usize = 1;
pub const SYNTHETIC_CHALLENGE_FREQUENCY: u64 = 600;

pub const MAX_CHALLENGE_PIECE_NUM: i32 = 5;
pub const SYNTH_CHALLENGE_TIMEOUT: f64 = 1.0; // TODO: modify this
pub const SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE: f64 = 3.0;
pub const MIN_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10; // minimum size of synthetic data in bytes
pub const MAX_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10 * 10 * 10; // maximum size of synthetic data in bytes

// Define the confidence level (e.g., 95%)
// Z-score for 95% confidence interval (two-sided) is approximately 1.96
pub const Z_SCORE: f64 = 1.96;
pub const Z_SQUARED: f64 = Z_SCORE * Z_SCORE; // Pre-calculated

pub const INFO_API_RATE_LIMIT_DURATION: Duration = Duration::from_secs(60);
pub const INFO_API_RATE_LIMIT_MAX_REQUESTS: usize = 10; // 1000 requests per minute
