use std::time::Duration;

pub const PROTOCOL_VERSION: u16 = 1;

pub(crate) const MAX_ATTEMPTS: i32 = 5;
pub const QUIC_CONNECTION_TIMEOUT: Duration = Duration::from_secs(3);
