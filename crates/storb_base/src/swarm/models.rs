use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use std::io::{Cursor, Read};
use std::{error, fmt};

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkDHTValue {
    pub chunk_hash: [u8; 32],
    pub validator_id: u32,
    pub piece_hashes: Vec<[u8; 32]>,
    pub chunk_idx: u64,
    pub k: u64,
    pub m: u64,
    pub chunk_size: u64,
    pub padlen: u64,
    pub original_chunk_size: u64,
    pub signature: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackerDHTValue {
    pub infohash: [u8; 32],
    pub validator_id: u32,
    pub filename: String,
    pub length: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub chunk_hashes: Vec<[u8; 32]>,
    pub creation_timestamp: DateTime<Utc>,
    pub signature: [u8; 32],
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PieceType {
    Data,
    Parity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PieceDHTValue {
    pub piece_hash: [u8; 32],
    pub validator_id: u32,
    pub chunk_idx: u64,
    pub piece_idx: u64,
    pub piece_type: PieceType,
    pub signature: [u8; 32],
}

#[repr(u8)]
enum DHTType {
    Chunk = 1,
    Piece = 2,
    Tracker = 3,
}

#[derive(Debug)]
pub enum DHTError {
    UnknownTag(u8),
    IoError(std::io::Error),
    SerializationError(Box<bincode::ErrorKind>),
}

impl fmt::Display for DHTError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            DHTError::UnknownTag(tag) => write!(f, "Unknown DHT type tag: {}", tag),
            DHTError::IoError(err) => write!(f, "I/O error: {}", err),
            DHTError::SerializationError(err) => write!(f, "Serialization error: {}", err),
        }
    }
}

impl error::Error for DHTError {}

impl From<std::io::Error> for DHTError {
    fn from(err: std::io::Error) -> Self {
        DHTError::IoError(err)
    }
}

impl From<Box<bincode::ErrorKind>> for DHTError {
    fn from(err: Box<bincode::ErrorKind>) -> Self {
        DHTError::SerializationError(err)
    }
}

#[derive(Debug)]
pub enum DHTValue {
    Chunk(ChunkDHTValue),
    Piece(PieceDHTValue),
    Tracker(TrackerDHTValue),
}

pub fn serialize_dht_value(value: &DHTValue) -> Result<Vec<u8>, DHTError> {
    let mut buf = Vec::new();
    match value {
        DHTValue::Chunk(chunk) => {
            buf.push(DHTType::Chunk as u8);
            bincode::serialize_into(&mut buf, chunk)?;
        }
        DHTValue::Piece(piece) => {
            buf.push(DHTType::Piece as u8);
            bincode::serialize_into(&mut buf, piece)?;
        }
        DHTValue::Tracker(tracker) => {
            buf.push(DHTType::Tracker as u8);
            bincode::serialize_into(&mut buf, tracker)?;
        }
    }
    Ok(buf)
}

pub fn deserialize_dht_value(bytes: &[u8]) -> Result<DHTValue, DHTError> {
    let mut cursor = Cursor::new(bytes);
    let mut tag_buf = [0u8; 1];
    cursor.read_exact(&mut tag_buf)?;
    let tag: u8 = tag_buf[0];
    match tag {
        x if x == DHTType::Chunk as u8 => {
            let chunk: ChunkDHTValue = bincode::deserialize_from(&mut cursor)?;
            Ok(DHTValue::Chunk(chunk))
        }
        x if x == DHTType::Piece as u8 => {
            let piece: PieceDHTValue = bincode::deserialize_from(&mut cursor)?;
            Ok(DHTValue::Piece(piece))
        }
        x if x == DHTType::Tracker as u8 => {
            let tracker: TrackerDHTValue = bincode::deserialize_from(&mut cursor)?;
            Ok(DHTValue::Tracker(tracker))
        }
        unknown => Err(DHTError::UnknownTag(unknown)),
    }
}
