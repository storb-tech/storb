use chrono::{DateTime, Utc};
use crabtensor::sign::KeypairSignature;
use rusqlite::{
    types::{FromSql, FromSqlResult, ToSqlOutput},
    ToSql,
};
use serde::{Deserialize, Serialize};
use subxt::ext::codec::Compact;

use crate::piece::PieceType;

// Newtype wrapper for DateTime<Utc>
#[derive(Debug, Clone, PartialEq, PartialOrd, Serialize, Deserialize)]
pub struct SqlDateTime(pub DateTime<Utc>);

impl FromSql for SqlDateTime {
    // from datetime to sql datetime, to the nanosecond
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> FromSqlResult<Self> {
        let timestamp = i64::column_result(value)?;
        Option::from(DateTime::<Utc>::from_timestamp_nanos(timestamp))
            .map(SqlDateTime)
            .ok_or(rusqlite::types::FromSqlError::InvalidType)
    }
}

impl ToSql for SqlDateTime {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput> {
        let timestamp =
            self.0
                .timestamp_nanos_opt()
                .ok_or(rusqlite::Error::ToSqlConversionFailure(
                    "Failed to convert DateTime to i64".into(),
                ))?;
        Ok(ToSqlOutput::from(timestamp))
    }
}

/// Represents a chunk entry
///
/// Contains metadata for a chunk including its hash, associated piece hashes,
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkValue {
    pub chunk_hash: [u8; 32],
    pub chunk_idx: u64,
    pub k: u64,
    pub m: u64,
    pub chunk_size: u64,
    pub padlen: u64,
    pub original_chunk_size: u64,
}

/// Represents a tracker entry
///
/// This struct holds the information required to track a file,
/// including its infohash, size parameters, and a cryptographic signature of the data blob owner.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InfohashValue {
    pub infohash: [u8; 32],
    pub length: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub creation_timestamp: DateTime<Utc>,
    pub signature: KeypairSignature,
}

/// Represents a piece entry
///
/// Contains the piece hash, indices indicating its position, its type,
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PieceValue {
    pub piece_hash: [u8; 32],
    pub validator_id: Compact<u16>,
    pub chunk_idx: u64,
    pub piece_idx: u64,
    pub piece_size: u64,
    pub piece_type: PieceType,
    pub miners: Vec<Compact<u16>>,
}

/// Represents a piece challenge history entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PieceChallengeHistory {
    pub piece_repair_hash: [u8; 32],
    pub piece_hash: [u8; 32],
    pub chunk_hash: [u8; 32],
    pub validator_id: Compact<u16>,
    pub timestamp: DateTime<Utc>,
    pub signature: KeypairSignature,
}

// Represents a chunk challenge history entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkChallengeHistory {
    pub challenge_hash: [u8; 32],
    pub chunk_hash: [u8; 32],
    pub validator_id: Compact<u16>,
    pub miners_challenged: Vec<Compact<u16>>,
    pub miners_successful: Vec<Compact<u16>>,
    // reference to the piece repair hash if any
    pub piece_repair_hash: [u8; 32],
    pub timestamp: DateTime<Utc>,
    pub signature: KeypairSignature,
}
