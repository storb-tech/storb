use std::time::Duration;

pub const NEURON_SYNC_TIMEOUT: Duration = Duration::from_secs(60);

pub const PIECE_LENGTH_FUNC_MIN_SIZE: u64 = 16 * 1024; // 16 KiB
pub const PIECE_LENGTH_FUNC_MAX_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB
pub const PIECE_LENGTH_SCALING: f64 = 0.5;
pub const PIECE_LENGTH_OFFSET: f64 = 8.39;

/// parameters for erasure coding
/// TODO(k_and_m): we might change how we determined these in the future - related issue: https://github.com/storb-tech/storb/issues/66
pub const CHUNK_K: usize = 4;
pub const CHUNK_M: usize = 8;

/// Buffer size for the DHT database's MPSC channel.
pub const DB_MPSC_BUFFER_SIZE: usize = 100;

/// Timeout for HTTP requests to /info endpoint.
pub const INFO_REQ_TIMEOUT: Duration = Duration::from_secs(5);

/// Timeout params for upload requests
pub const MIN_BANDWIDTH: u64 = 20 * 1024; // minimum "bandwidth"

pub const SYNC_BUFFER_SIZE: usize = 32;
