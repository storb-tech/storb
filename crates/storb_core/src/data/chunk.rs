use thiserror::Error;
use tracing::{debug, error};

use crate::data::chunk_encoding::{ChunkDecoder, ChunkEncodingError, ZfecEncoding};
use crate::data::piece::Piece;
use crate::hashes::{ChunkHash, PieceHash};

/// Represents a object chunk, which is a collection of pieces.
///
/// Chunks are optionally erasure-encoded for redundancy and fault tolerance.
#[derive(Clone, Debug)]
pub struct Chunk {
    /// The chunk hash, which acts as a unique identifier for the chunk.
    // pub chunk_hash: ChunkHash,

    /// The encoding type used for the chunk.
    pub encoding_type: ChunkEncodingType,

    /// Number of data blocks.
    pub k: u64,

    /// Total blocks (data + parity).
    pub m: u64,

    /// The index of the chunk within the object.
    pub chunk_idx: u64,

    /// The size of the chunk in bytes.
    pub chunk_size: u64,

    /// Zero-padding length in bytes.
    /// This is used to ensure that the chunk size is a multiple of the piece size.
    pub pad_length: u64,

    /// The original size of the chunk before any padding was applied.
    pub original_chunk_size: u64,

    /// The pieces that make up the chunk.
    pub pieces: Vec<Piece>,
}

/// A piece within a chunk.
pub struct ChunkPiece {
    /// The chunk hash this piece belongs to.
    pub chunk_hash: ChunkHash,

    /// The index of the piece within the chunk.
    pub piece_idx: u64,

    /// The hash of the piece, which acts as a unique identifier for the piece.
    pub piece_hash: PieceHash,
}

#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum ChunkEncodingType {
    None = 0,
    Zfec = 1,
}

#[derive(Debug, Error)]
pub enum ChunkError {
    #[error("Not enough pieces to reconstruct chunk {0}, expected k={1} but got {2} pieces")]
    ReconstructionError(u64, u64, usize),

    #[error("Failed to reconstruct chunk: {0}")]
    ChunkEncodingError(#[from] ChunkEncodingError),
}

/// Reconstructs a single chunk as raw bytes from its pieces in the encoded chunk.
pub fn reconstruct_chunk(chunk: &Chunk) -> Result<Vec<u8>, ChunkError> {
    debug!("Reconstructing single chunk {}", chunk.chunk_idx);

    let pieces = chunk.clone().pieces;

    // Filter pieces for this specific chunk and sort them
    let mut relevant_pieces: Vec<Piece> = pieces
        .iter()
        .filter(|piece| piece.chunk_idx == chunk.chunk_idx)
        .cloned()
        .collect();
    relevant_pieces.sort_by_key(|p| p.piece_idx);

    // Ensure we have enough pieces to reconstruct (at least k pieces)
    let k = chunk.k;
    debug!("k={k}, pieces available={}", relevant_pieces.len());

    if relevant_pieces.len() < k as usize {
        error!(
            "Not enough pieces to reconstruct chunk {}, expected {} but got {} pieces",
            chunk.chunk_idx,
            k,
            relevant_pieces.len(),
        );
        return Err(ChunkError::ReconstructionError(
            chunk.chunk_idx,
            k,
            relevant_pieces.len(),
        ));
    }

    // Create a new `Chunk` with just the pieces for this chunk
    let mut chunk_to_decode = chunk.clone();
    chunk_to_decode.pieces = relevant_pieces;

    let codec = ZfecEncoding::new();
    let decoded_chunk = codec.decode(&chunk_to_decode)?;

    Ok(decoded_chunk)
}

// TODO: tests
