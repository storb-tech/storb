// TODO: should we increase min required miners?
// TODO: should we use it in consume_bytes to determin number of miners to distribute to?
// NOTE: see: https://github.com/storb-tech/storb/issues/66
pub const MIN_REQUIRED_MINERS: usize = 1;
pub const SYNTHETIC_CHALLENGE_FREQUENCY: u64 = 300;

pub const MAX_CHALLENGE_PIECE_NUM: i32 = 5;
pub const SYNTH_CHALLENGE_TIMEOUT: f64 = 1.0; // TODO: modify this
pub const SYNTH_CHALLENGE_WAIT_BEFORE_RETRIEVE: f64 = 3.0;
pub const MIN_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10; // minimum size of synthetic data in bytes
pub const MAX_SYNTH_CHUNK_SIZE: usize = 1024 * 10 * 10 * 10 * 10; // maximum size of synthetic data in bytes
