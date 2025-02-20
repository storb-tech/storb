use bincode::Options;
use chrono::{DateTime, Utc};
use crabtensor::sign::KeypairSignature;
use libp2p::kad::RecordKey;
use serde::{Deserialize, Serialize};
use subxt::ext::codec::Compact;
use thiserror::Error;

use crate::piece::PieceType;

/// Represents a chunk entry in the DHT.
///
/// Contains metadata for a chunk including its hash, associated piece hashes,
/// and other relevant information along with a cryptographic signature.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ChunkDHTValue {
    pub chunk_hash: RecordKey,
    pub validator_id: Compact<u16>,
    pub piece_hashes: Vec<[u8; 32]>,
    pub chunk_idx: u64,
    pub k: u64,
    pub m: u64,
    pub chunk_size: u64,
    pub padlen: u64,
    pub original_chunk_size: u64,
    pub signature: KeypairSignature,
}

/// Represents a tracker entry in the DHT.
///
/// This struct holds the information required to track a file,
/// including its infohash, filename, size parameters, and a cryptographic signature.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TrackerDHTValue {
    pub infohash: RecordKey,
    pub validator_id: Compact<u16>,
    pub filename: String,
    pub length: u64,
    pub chunk_size: u64,
    pub chunk_count: u64,
    pub chunk_hashes: Vec<[u8; 32]>,
    pub creation_timestamp: DateTime<Utc>,
    pub signature: KeypairSignature,
}

/// Represents a piece entry in the DHT.
///
/// Contains the piece hash, indices indicating its position, its type,
/// and a signature to ensure integrity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PieceDHTValue {
    pub piece_hash: RecordKey,
    pub validator_id: Compact<u16>,
    pub chunk_idx: u64,
    pub piece_idx: u64,
    pub piece_type: PieceType,
    pub signature: KeypairSignature,
}

/// Internal enumeration for DHT value types.
///
/// Each variant is represented as a single byte tag for serialization purposes.
#[repr(u8)]
enum DHTType {
    Chunk = 1,
    Piece = 2,
    Tracker = 3,
}

/// Errors that can occur during DHT serialization or deserialization.
#[derive(Error, Debug)]
pub enum DHTError {
    #[error("Unknown DHT type tag: {0}")]
    UnknownTag(u8),

    #[error("I/O error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Serialization error: {0}")]
    SerializationError(#[from] bincode::Error),
}

/// Represents a DHT value, which can be a chunk, piece, or tracker entry.
#[derive(Debug, PartialEq)]
pub enum DHTValue {
    Chunk(ChunkDHTValue),
    Piece(PieceDHTValue),
    Tracker(TrackerDHTValue),
}

/// Serializes a DHTValue into a binary representation.
///
/// The output consists of a one-byte type tag followed by the bincode-serialized data
/// of the corresponding value. This function uses little-endian encoding with fixed integer
/// representation and rejects trailing bytes.
///
/// # Errors
///
/// Returns a `DHTError` if serialization fails.
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

/// Deserializes a byte slice into a DHTValue.
///
/// The function reads the first byte as a type tag and then uses bincode to decode
/// the remaining bytes according to the specified type. It rejects extra trailing bytes.
///
/// # Errors
///
/// Returns a `DHTError` if the input is empty, the type tag is unknown,
/// or if deserialization fails.
pub fn deserialize_dht_value(bytes: &[u8]) -> Result<DHTValue, DHTError> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();

    // Ensure there's at least one byte to read.
    if bytes.is_empty() {
        return Err(std::io::Error::new(std::io::ErrorKind::UnexpectedEof, "Empty bytes").into());
    }

    // The first byte is the type tag.
    let tag = bytes[0];
    // The remaining bytes contain the serialized data.
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

    /// Creates a sample ChunkDHTValue for testing purposes.
    fn sample_chunk_dht_value() -> ChunkDHTValue {
        ChunkDHTValue {
            chunk_hash: RecordKey::new(&[0xAA; 32]),
            validator_id: Compact(128),
            piece_hashes: vec![[0xBB; 32], [0xCC; 32]],
            chunk_idx: 42,
            k: 2,
            m: 4,
            chunk_size: 1024,
            padlen: 16,
            original_chunk_size: 1000,
            signature: KeypairSignature::from_raw([0x11; 64]),
        }
    }

    /// Creates a sample PieceDHTValue for testing purposes.
    fn sample_piece_dht_value() -> PieceDHTValue {
        PieceDHTValue {
            piece_hash: RecordKey::new(&[0x99; 32]),
            validator_id: Compact(98),
            chunk_idx: 55,
            piece_idx: 99,
            piece_type: PieceType::Data,
            signature: KeypairSignature::from_raw([0x11; 64]),
        }
    }

    /// Creates a sample TrackerDHTValue for testing purposes.
    fn sample_tracker_dht_value() -> TrackerDHTValue {
        TrackerDHTValue {
            infohash: RecordKey::new(&[0x01; 32]),
            validator_id: Compact(66),
            filename: "myfile.txt".to_string(),
            length: 123456,
            chunk_size: 4096,
            chunk_count: 30,
            chunk_hashes: vec![[0x10; 32], [0x20; 32], [0x30; 32]],
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0x11; 64]),
        }
    }

    /// Tests serialization and deserialization of a ChunkDHTValue.
    #[test]
    fn test_serialize_deserialize_chunk() {
        let chunk = sample_chunk_dht_value();
        let dht_value = DHTValue::Chunk(chunk.clone());

        let serialized = serialize_dht_value(&dht_value).expect("Serialization failed");
        let deserialized = deserialize_dht_value(&serialized).expect("Deserialization failed");

        assert_eq!(deserialized, DHTValue::Chunk(chunk));
    }

    /// Tests serialization and deserialization of a PieceDHTValue.
    #[test]
    fn test_serialize_deserialize_piece() {
        let piece = sample_piece_dht_value();
        let dht_value = DHTValue::Piece(piece.clone());

        let serialized = serialize_dht_value(&dht_value).expect("Serialization failed");
        let deserialized = deserialize_dht_value(&serialized).expect("Deserialization failed");

        assert_eq!(deserialized, DHTValue::Piece(piece));
    }

    /// Tests serialization and deserialization of a TrackerDHTValue.
    #[test]
    fn test_serialize_deserialize_tracker() {
        let tracker = sample_tracker_dht_value();
        let dht_value = DHTValue::Tracker(tracker.clone());

        let serialized = serialize_dht_value(&dht_value).expect("Serialization failed");
        let deserialized = deserialize_dht_value(&serialized).expect("Deserialization failed");

        assert_eq!(deserialized, DHTValue::Tracker(tracker));
    }

    /// Tests that deserialization fails when encountering an unknown type tag.
    #[test]
    fn test_unknown_tag_deserialization() {
        // Manually craft a buffer with a fake tag = 99.
        let bytes = [99, 0, 1, 2, 3, 4]; // The rest is random filler.
        let err = deserialize_dht_value(&bytes).unwrap_err();
        match err {
            DHTError::UnknownTag(tag) => assert_eq!(tag, 99),
            e => panic!("Expected UnknownTag(99), got {:?}", e),
        }
    }

    /// Tests that deserialization rejects buffers with trailing bytes.
    #[test]
    fn test_trailing_bytes_rejected() {
        // Serialize a valid ChunkDHTValue.
        let chunk = sample_chunk_dht_value();
        let dht_value = DHTValue::Chunk(chunk);
        let mut serialized = serialize_dht_value(&dht_value).expect("Serialization failed");

        // Append extra trailing bytes.
        serialized.push(0xFF);

        let err = deserialize_dht_value(&serialized).unwrap_err();
        match err {
            DHTError::SerializationError(_inner) => {
                // Expected: bincode rejects extra trailing data.
            }
            e => panic!("Expected trailing byte error, got {:?}", e),
        }
    }
}
