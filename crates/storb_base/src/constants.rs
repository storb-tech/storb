use std::time::Duration;

use libp2p::StreamProtocol;

pub const MIN_PIECE_SIZE: u64 = 16 * 1024; // 16 KiB
pub const MAX_PIECE_SIZE: u64 = 256 * 1024 * 1024; // 256 MiB
pub const PIECE_LENGTH_SCALING: f64 = 0.5;
pub const PIECE_LENGTH_OFFSET: f64 = 8.39;

/// The protocol name used for Kademlia in the Storb network.
pub const STORB_KAD_PROTOCOL_NAME: StreamProtocol = StreamProtocol::new("/storb/kad/1.0.0");

/// Timeout for DHT queries in seconds.
pub const DHT_QUERY_TIMEOUT: u64 = 3;
/// Buffer size for the DHT database's MPSC channel.
pub const DB_MPSC_BUFFER_SIZE: usize = 100;

/// Timeout for node HTTP requests.
pub const CLIENT_TIMEOUT: Duration = Duration::from_secs(1);
