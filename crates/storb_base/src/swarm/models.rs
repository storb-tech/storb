use bincode::Options;
use chrono::{DateTime, Utc};
use ed25519::Signature;
use libp2p::kad::RecordKey;
use serde::{Deserialize, Serialize};
use thiserror::Error;

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkDHTValue {
    pub chunk_hash: RecordKey,
    pub validator_id: u32,
    pub piece_hashes: Vec<[u8; 32]>,
    pub chunk_idx: u64,
    pub k: u64,
    pub m: u64,
    pub chunk_size: u64,
    pub padlen: u64,
    pub original_chunk_size: u64,
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackerDHTValue {
    pub infohash: RecordKey,
    pub validator_id: u32,
    pub filename: String,
    pub length: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub chunk_hashes: Vec<[u8; 32]>,
    pub creation_timestamp: DateTime<Utc>,
    pub signature: Signature,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PieceType {
    Data,
    Parity,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PieceDHTValue {
    pub piece_hash: RecordKey,
    pub validator_id: u32,
    pub chunk_idx: u64,
    pub piece_idx: u64,
    pub piece_type: PieceType,
    pub signature: Signature,
}

#[repr(u8)]
enum DHTType {
    Chunk = 1,
    Piece = 2,
    Tracker = 3,
}

#[derive(Error, Debug)]
pub enum DHTError {
    #[error("Unknown DHT type tag: {0}")]
    UnknownTag(u8),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
}

#[derive(Debug, PartialEq)]
pub enum DHTValue {
    Chunk(ChunkDHTValue),
    Piece(PieceDHTValue),
    Tracker(TrackerDHTValue),
}

pub fn serialize_dht_value(value: &DHTValue) -> Result<Vec<u8>, DHTError> {
    let mut buf = Vec::new();
    let bincode_opts = bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();
    match value {
        DHTValue::Chunk(chunk) => {
            buf.push(DHTType::Chunk as u8);
            bincode_opts.serialize_into(&mut buf, chunk)?;
        }
        DHTValue::Piece(piece) => {
            buf.push(DHTType::Piece as u8);
            bincode_opts.serialize_into(&mut buf, piece)?;
        }
        DHTValue::Tracker(tracker) => {
            buf.push(DHTType::Tracker as u8);
            bincode_opts.serialize_into(&mut buf, tracker)?;
        }
    }
    Ok(buf)
}

pub fn deserialize_dht_value(bytes: &[u8]) -> Result<DHTValue, DHTError> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();

    // Ensure there's at least one byte to read
    if bytes.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Empty bytes").into());
    }

    // The first byte is the tag
    let tag = bytes[0];
    // Everything after the first byte is for bincode to parse the DHTValue
    let rest = &bytes[1..];

    match tag {
        x if x == DHTType::Chunk as u8 => {
            let chunk: ChunkDHTValue = bincode_opts.deserialize(rest)?;
            Ok(DHTValue::Chunk(chunk))
        }
        x if x == DHTType::Piece as u8 => {
            let piece: PieceDHTValue = bincode_opts.deserialize(rest)?;
            Ok(DHTValue::Piece(piece))
        }
        x if x == DHTType::Tracker as u8 => {
            let tracker: TrackerDHTValue = bincode_opts.deserialize(rest)?;
            Ok(DHTValue::Tracker(tracker))
        }
        unknown => Err(DHTError::UnknownTag(unknown)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::Utc;

    fn sample_chunk_dht_value() -> ChunkDHTValue {
        ChunkDHTValue {
            chunk_hash: RecordKey::new(&[0xAA; 32]),
            validator_id: 1234,
            piece_hashes: vec![[0xBB; 32], [0xCC; 32]],
            chunk_idx: 42,
            k: 2,
            m: 4,
            chunk_size: 1024,
            padlen: 16,
            original_chunk_size: 1000,
            signature: Signature::from_bytes(&[0x00; 64]),
        }
    }

    fn sample_piece_dht_value() -> PieceDHTValue {
        PieceDHTValue {
            piece_hash: RecordKey::new(&[0x99; 32]),
            validator_id: 98765,
            chunk_idx: 55,
            piece_idx: 99,
            piece_type: PieceType::Data,
            signature: Signature::from_bytes(&[0x11; 64]),
        }
    }

    fn sample_tracker_dht_value() -> TrackerDHTValue {
        TrackerDHTValue {
            infohash: RecordKey::new(&[0x01; 32]),
            validator_id: 666,
            filename: "myfile.txt".to_string(),
            length: 123456,
            chunk_size: 4096,
            chunk_count: 30,
            chunk_hashes: vec![[0x10; 32], [0x20; 32], [0x30; 32]],
            creation_timestamp: Utc::now(),
            signature: Signature::from_bytes(&[0x22; 64]),
        }
    }

    #[test]
    fn test_serialize_deserialize_chunk() {
        let chunk = sample_chunk_dht_value();
        let dht_value = DHTValue::Chunk(chunk.clone());

        let serialized = serialize_dht_value(&dht_value).expect("Serialization failed");
        let deserialized = deserialize_dht_value(&serialized).expect("Deserialization failed");

        assert_eq!(deserialized, DHTValue::Chunk(chunk));
    }

    #[test]
    fn test_serialize_deserialize_piece() {
        let piece = sample_piece_dht_value();
        let dht_value = DHTValue::Piece(piece.clone());

        let serialized = serialize_dht_value(&dht_value).expect("Serialization failed");
        let deserialized = deserialize_dht_value(&serialized).expect("Deserialization failed");

        assert_eq!(deserialized, DHTValue::Piece(piece));
    }

    #[test]
    fn test_serialize_deserialize_tracker() {
        let tracker = sample_tracker_dht_value();
        let dht_value = DHTValue::Tracker(tracker.clone());

        let serialized = serialize_dht_value(&dht_value).expect("Serialization failed");
        let deserialized = deserialize_dht_value(&serialized).expect("Deserialization failed");

        assert_eq!(deserialized, DHTValue::Tracker(tracker));
    }

    #[test]
    fn test_unknown_tag_deserialization() {
        // Manually craft a buffer with a fake tag = 99
        let bytes = [99, 0, 1, 2, 3, 4]; // The rest is random filler
        let err = deserialize_dht_value(&bytes).unwrap_err();
        match err {
            DHTError::UnknownTag(tag) => assert_eq!(tag, 99),
            e => panic!("Expected UnknownTag(99), got {:?}", e),
        }
    }

    #[test]
    fn test_trailing_bytes_rejected() {
        // Serialize a valid ChunkDHTValue
        let chunk = sample_chunk_dht_value();
        let dht_value = DHTValue::Chunk(chunk);
        let mut serialized = serialize_dht_value(&dht_value).expect("Serialization failed");

        serialized.push(0xFF);

        let err = deserialize_dht_value(&serialized).unwrap_err();
        match err {
            DHTError::SerializationError(_inner) => {
                // This is expected: Bincode sees leftover data
            }
            e => panic!("Expected trailing byte error, got {:?}", e),
        }
    }
}
