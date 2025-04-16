use std::time::Duration;

use libp2p::StreamProtocol;

pub const PIECE_LENGTH_FUNC_MIN_SIZE: u64 = 16 * 1024; // 16 KiB
pub const PIECE_LENGTH_FUNC_MAX_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB
pub const PIECE_LENGTH_SCALING: f64 = 0.5;
pub const PIECE_LENGTH_OFFSET: f64 = 8.39;

/// parameters for erasure coding
/// TODO(k_and_m): we might change how we determined these in the future - related issue: https://github.com/storb-tech/storb/issues/66
pub const CHUNK_K: usize = 4;
pub const CHUNK_M: usize = 8;

/// The protocol name used for Kademlia in the Storb network.
pub const STORB_KAD_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/storb/kad/1.0.0");

/// Timeout for DHT queries in seconds.
pub const DHT_QUERY_TIMEOUT: u64 = 3;
/// Buffer size for the DHT database's MPSC channel.
pub const DB_MPSC_BUFFER_SIZE: usize = 100;

/// Timeout for node HTTP requests.
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(1);

/// Timeout params for upload requests
pub const MIN_BANDWIDTH: u64 = 10 * 1024; // minimum ~10KiB/s "bandwidth"

pub const DHT_MAX_RETRIES: u64 = 3; // maximum number of retries for DHT queries
pub const PEER_VERIFICATION_TIMEOUT: u64 = 100; // timeout for peer verification in seconds
