use base::{
    piece::{ChunkHash, InfoHash, PieceHash, PieceType},
    NodeUID,
};
use chrono::{DateTime, Utc};
use crabtensor::{sign::KeypairSignature, AccountId};
use rusqlite::types::ValueRef;
use rusqlite::{
    types::{FromSql, FromSqlResult, ToSqlOutput},
    ToSql,
};
use serde::{Deserialize, Serialize};
use subxt::ext::codec::Compact;

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
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
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
    pub chunk_hash: ChunkHash,
    pub k: u64,
    pub m: u64,
    pub chunk_size: u64,
    pub padlen: u64,
    pub original_chunk_size: u64,
}

/// Represents a tracker entry with identity and nonce for replay protection
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct InfohashValue {
    pub infohash: InfoHash,
    pub name: String,
    pub length: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub owner_account_id: SqlAccountId,
    pub creation_timestamp: DateTime<Utc>,
    pub signature: KeypairSignature,
}

impl InfohashValue {
    /// Verify that the signature is valid for this infohash value
    pub fn verify_signature(&self, nonce: &[u8; 32]) -> bool {
        let message = self.get_signature_message(nonce);
        crabtensor::sign::verify_signature(&self.owner_account_id.0, &self.signature, message)
    }

    /// Get the message that should be signed for this infohash
    pub fn get_signature_message(&self, nonce: &[u8; 32]) -> Vec<u8> {
        let mut message = Vec::new();
        message.extend_from_slice(nonce); // Include nonce in signature
        message
    }
}

/// Represents a piece entry
///
/// Contains the piece hash, indices indicating its position, its type,
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PieceValue {
    pub piece_hash: PieceHash,
    pub piece_size: u64,
    pub piece_type: PieceType,
    // TODO: shouldn't this be a set instead of a vector?
    pub miners: Vec<Compact<NodeUID>>,
}

/// Represents a piece challenge history entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PieceChallengeHistory {
    pub piece_repair_hash: [u8; 32],
    pub piece_hash: PieceHash,
    pub chunk_hash: ChunkHash,
    pub validator_id: Compact<NodeUID>,
    pub timestamp: DateTime<Utc>,
    pub signature: KeypairSignature,
}

/// Represents a chunk challenge history entry
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkChallengeHistory {
    pub challenge_hash: [u8; 32],
    pub chunk_hash: ChunkHash,
    pub validator_id: Compact<NodeUID>,
    pub miners_challenged: Vec<Compact<NodeUID>>,
    pub miners_successful: Vec<Compact<NodeUID>>,
    // reference to the piece repair hash if any
    pub piece_repair_hash: [u8; 32],
    pub timestamp: DateTime<Utc>,
    pub signature: KeypairSignature,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum CrSqliteValue {
    Null,
    Integer(i64),
    Real(f64),
    Text(String),
    Blob(Vec<u8>),
}

impl From<ValueRef<'_>> for CrSqliteValue {
    fn from(value_ref: ValueRef<'_>) -> Self {
        match value_ref {
            ValueRef::Null => CrSqliteValue::Null,
            ValueRef::Integer(i) => CrSqliteValue::Integer(i),
            ValueRef::Real(f) => CrSqliteValue::Real(f),
            ValueRef::Text(t) => CrSqliteValue::Text(String::from_utf8_lossy(t).to_string()),
            ValueRef::Blob(b) => CrSqliteValue::Blob(b.to_vec()),
        }
    }
}

// write impl for converting CrSqliteValue to a sqlite value
impl ToSql for CrSqliteValue {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        match self {
            CrSqliteValue::Null => Ok(ToSqlOutput::from(rusqlite::types::Null)),
            CrSqliteValue::Integer(i) => Ok(ToSqlOutput::from(*i)),
            CrSqliteValue::Real(f) => Ok(ToSqlOutput::from(*f)),
            CrSqliteValue::Text(t) => Ok(ToSqlOutput::from(t.clone())),
            CrSqliteValue::Blob(b) => Ok(ToSqlOutput::from(b.clone())),
        }
    }
}

/// Represents the changes in the SQLite database
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct CrSqliteChanges {
    pub table: String,
    pub pk: CrSqliteValue,
    pub cid: String,
    pub val: CrSqliteValue,
    pub col_version: u64,
    pub db_version: u64,
    pub site_id: Vec<u8>,
    pub cl: u64,
    pub seq: u64,
}

// Newtype wrapper for AccountId
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SqlAccountId(pub AccountId);

impl From<&AccountId> for SqlAccountId {
    fn from(id: &AccountId) -> Self {
        SqlAccountId(id.clone())
    }
}

impl AsRef<AccountId> for SqlAccountId {
    fn as_ref(&self) -> &AccountId {
        &self.0
    }
}

impl ToSql for SqlAccountId {
    fn to_sql(&self) -> rusqlite::Result<ToSqlOutput<'_>> {
        Ok(ToSqlOutput::from(&self.0 .0[..]))
    }
}

impl FromSql for SqlAccountId {
    fn column_result(value: rusqlite::types::ValueRef<'_>) -> FromSqlResult<Self> {
        match value {
            ValueRef::Blob(b) => {
                if b.len() == 32 {
                    let mut array = [0u8; 32];
                    array.copy_from_slice(b);
                    Ok(SqlAccountId(AccountId::from(array)))
                } else {
                    Err(rusqlite::types::FromSqlError::InvalidBlobSize {
                        expected_size: 32,
                        blob_size: b.len(),
                    })
                }
            }
            _ => Err(rusqlite::types::FromSqlError::InvalidType),
        }
    }
}

// Add nonce management models
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NonceRequest {
    pub account_id: AccountId,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct NonceResponse {
    pub nonce: [u8; 32],
    pub timestamp: DateTime<Utc>,
}
