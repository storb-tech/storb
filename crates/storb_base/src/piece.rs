use bincode::Options;
use serde::{ser::Error, Deserialize, Serialize};
use thiserror::Error;
use tracing::{debug, error};
use zfec_rs::Chunk as ZFecChunk;

use crate::constants::{
    PIECE_LENGTH_FUNC_MAX_SIZE, PIECE_LENGTH_FUNC_MIN_SIZE, PIECE_LENGTH_OFFSET,
    PIECE_LENGTH_SCALING,
};

#[derive(Debug, Clone)]
pub struct Piece {
    pub chunk_idx: u64,
    pub piece_size: u64,
    pub piece_idx: u64,
    pub piece_type: PieceType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub enum PieceType {
    Data,
    Parity,
}

impl TryFrom<u8> for PieceType {
    type Error = &'static str;

    fn try_from(value: u8) -> Result<Self, Self::Error> {
        match value {
            0 => Ok(PieceType::Data),
            1 => Ok(PieceType::Parity),
            _ => Err("Invalid PieceType value"),
        }
    }
}

#[derive(Debug, Clone)]
pub struct EncodedChunk {
    pub pieces: Vec<Piece>,
    pub chunk_idx: u64,
    /// Number of data blocks
    pub k: u64,
    /// Total blocks (data + parity)
    pub m: u64,
    pub chunk_size: u64,
    pub padlen: u64,
    pub original_chunk_size: u64,
}

/// Piece response that is sent from the miner to the validator.
///
/// - `piece_hash` is the fixed width piece signature from which we start
///   reading the piece data.
/// - `piece_data` is the data itself.
#[derive(Serialize, Deserialize)]
#[repr(C)]
pub struct PieceResponse {
    pub piece_hash: [u8; 32],
    pub piece_data: Vec<u8>,
}

#[derive(Debug, Error)]
pub enum PieceError {
    #[error("Not enough pieces to reconstruct chunk {0}, expected k={1} but got {2} pieces")]
    ReconstructionError(u64, u64, usize),
}

/// Serialise the piece response struct into a vector of bytes.
pub fn serialise_piece_response(
    piece_response: &PieceResponse,
) -> Result<Vec<u8>, Box<bincode::Error>> {
    let mut buf = Vec::new();
    let bincode_opts = bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();

    match bincode_opts.serialize_into(&mut buf, piece_response) {
        Ok(_) => Ok(buf),
        Err(e) => Err(Box::new(e)),
    }
}

/// Deserialise the piece response in bytes into a piece response struct.
/// The `piece_hash` is provided so the piece hash can be checked against the
/// serialised data to determine where the actual payload starts.
pub fn deserialise_piece_response(
    serialised_buf: &[u8],
    piece_hash: &[u8; 32],
) -> Result<Vec<u8>, Box<bincode::Error>> {
    let bincode_opts = bincode::DefaultOptions::new()
        .with_little_endian()
        .with_fixint_encoding()
        .reject_trailing_bytes();

    if let Some(pos) = serialised_buf
        .windows(32)
        .position(|window| window == piece_hash)
    {
        let piece_data = serialised_buf[pos + 32..].to_vec();
        let deserialized_piece_response = bincode_opts.deserialize(&piece_data)?;
        Ok(deserialized_piece_response)
    } else {
        Err(Box::new(bincode::Error::custom(
            "Piecehash was not matched in the buffer. Make sure it is a valid piecehash",
        )))
    }
}

pub fn get_infohash(piece_hashes: Vec<[u8; 32]>) -> [u8; 32] {
    // The infohash is a hash of the piece hashes
    let mut hasher = blake3::Hasher::new();
    for piece_hash in piece_hashes {
        hasher.update(&piece_hash);
    }
    let hash = hasher.finalize();
    let mut infohash = [0u8; 32];
    infohash.copy_from_slice(&hash.as_bytes()[..32]);
    infohash
}

pub fn piece_length(content_length: u64, min_size: Option<u64>, max_size: Option<u64>) -> u64 {
    let min_size = min_size.unwrap_or(PIECE_LENGTH_FUNC_MIN_SIZE);
    let max_size = max_size.unwrap_or(PIECE_LENGTH_FUNC_MAX_SIZE);

    // Calculate ideal length based on content size using log scaling
    let exponent =
        ((content_length as f64).log2() * PIECE_LENGTH_SCALING + PIECE_LENGTH_OFFSET) as i32;
    let length = 1_u64 << exponent;

    // Clamp between min and max bounds
    length.clamp(min_size, max_size)
}

/// parameters for erasure coding
/// TODO(k_and_m): we might change how we determined these in the future - related issue: https://github.com/storb-tech/storb/issues/66
pub fn get_k_and_m(chunk_size: u64) -> (usize, usize) {
    let piece_size = piece_length(chunk_size, None, None);
    // Calculate how many data blocks (k) and parity blocks
    let expected_data_pieces = ((chunk_size as f64) / (piece_size as f64)).ceil() as usize;
    let expected_parity_pieces = ((expected_data_pieces as f64) / 2.0).ceil() as usize;

    let k = expected_data_pieces;
    let m = k + expected_parity_pieces;

    (k, m)
}

// TODO: maybe update parts of this function in the future to adhere the fact that k and m are going to be constant
pub fn encode_chunk(chunk: &[u8], chunk_idx: u64) -> EncodedChunk {
    let chunk_size = chunk.len() as u64;
    let piece_size = piece_length(chunk_size, None, None);
    // Calculate how many data blocks (k) and parity blocks
    // TODO: see TODO(k_and_m)
    let (k, m) = get_k_and_m(chunk_size);
    debug!("[encode_chunk] chunk {chunk_idx}: {chunk_size} bytes, piece_size = {piece_size}, k = {k}, m = {m}");

    let encoder = zfec_rs::Fec::new(k, m).expect("Failed to create encoder");
    let (encoded_pieces, padlen) = encoder.encode(chunk).expect("Failed to encode chunk");

    // Calculate how zfec splits/pads under the hood
    let zfec_chunk_size = chunk_size.div_ceil(k as u64);

    let mut pieces: Vec<Piece> = Vec::new();
    for (i, piece) in encoded_pieces.into_iter().enumerate() {
        let piece_data = piece.data;
        let piece_type = if i < k {
            PieceType::Data
        } else {
            PieceType::Parity
        };

        pieces.push(Piece {
            piece_type,
            piece_size,
            data: piece_data,
            chunk_idx,
            piece_idx: i.try_into().expect("Failed to convert i"),
        });
    }

    EncodedChunk {
        pieces,
        chunk_idx,
        k: k.try_into().expect("Failed to convert k"),
        m: m.try_into().expect("Failed to convert m"),
        chunk_size: zfec_chunk_size,
        padlen: padlen.try_into().expect("Failed to convert padlen"),
        original_chunk_size: chunk_size,
    }
}

pub fn decode_chunk(encoded_chunk: &EncodedChunk) -> Vec<u8> {
    let k: usize = encoded_chunk.k as usize;
    let m: usize = encoded_chunk.m as usize;
    let padlen = encoded_chunk.padlen;
    let mut pieces = encoded_chunk.pieces.clone();
    pieces.sort_unstable_by_key(|p| p.piece_idx);

    let mut pieces_to_decode: Vec<ZFecChunk> = Vec::new();

    // zfec decode requires exactly k blocks
    if pieces.len() > k {
        for p in pieces.iter().take(k) {
            pieces_to_decode.push(ZFecChunk::new(p.data.clone(), p.piece_idx as usize));
        }
    } else {
        for p in pieces.iter() {
            pieces_to_decode.push(ZFecChunk::new(p.data.clone(), p.piece_idx as usize));
        }
    }

    let decoder = zfec_rs::Fec::new(k, m).expect("Failed to create decoder");
    decoder
        .decode(&pieces_to_decode, padlen as usize)
        .expect("Failed to decode chunk")
}

pub fn reconstruct_data(pieces: &[Piece], chunks: &[EncodedChunk]) -> Vec<u8> {
    debug!("reconstructing data");
    debug!(
        "pieces: {:#?}",
        pieces
            .iter()
            .map(|p| Piece {
                chunk_idx: p.chunk_idx,
                piece_size: p.piece_size,
                piece_idx: p.piece_idx,
                piece_type: p.piece_type.clone(),
                data: vec![] // Exclude data from debug output
            })
            .collect::<Vec<_>>()
    );
    let mut reconstructed_chunks: Vec<Vec<u8>> = Vec::new();

    for chunk in chunks {
        let chunk_idx = chunk.chunk_idx;

        // Collect all pieces for this chunk
        let mut relevant_pieces: Vec<Piece> = pieces
            .iter()
            .filter(|piece| piece.chunk_idx == chunk_idx)
            .cloned()
            .collect();
        relevant_pieces.sort_by_key(|p| p.piece_idx);

        // Ensure at least k pieces are available for decoding
        let k = chunk.k;
        debug!("[reconstruct_data]: k={k}");
        if (relevant_pieces.len() as u64) < k {
            tracing::error!(
                "Not enough pieces to reconstruct chunk {}, expected {} but got {} pieces | # pieces to reconstruct from: {}",
                chunk_idx,
                k,
                relevant_pieces.len(),
                pieces.len()
            );
            return Vec::new(); // Return empty vec to indicate error
        }

        let mut chunk_to_decode = chunk.clone();
        chunk_to_decode.pieces = relevant_pieces;
        let reconstructed_chunk = decode_chunk(&chunk_to_decode);
        reconstructed_chunks.push(reconstructed_chunk);
    }

    reconstructed_chunks.concat()
}

/// Reconstructs a single chunk (byte stream) from its pieces in the encoded chunk.
pub fn reconstruct_chunk(chunk: &EncodedChunk) -> Result<Vec<u8>, PieceError> {
    debug!("reconstructing single chunk {}", chunk.chunk_idx);

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
    debug!(
        "[reconstruct_chunk]: k={k}, pieces available={}",
        relevant_pieces.len()
    );

    if relevant_pieces.len() < k as usize {
        error!(
            "Not enough pieces to reconstruct chunk {}, expected {} but got {} pieces",
            chunk.chunk_idx,
            k,
            relevant_pieces.len(),
        );
        return Err(PieceError::ReconstructionError(
            chunk.chunk_idx,
            k,
            relevant_pieces.len(),
        ));
    }

    // Create a new EncodedChunk with just the pieces for this chunk
    let mut chunk_to_decode = chunk.clone();
    chunk_to_decode.pieces = relevant_pieces;

    // Decode and return the chunk
    Ok(decode_chunk(&chunk_to_decode))
}

#[cfg(test)]
mod tests {
    use std::sync::Once;

    use rand;
    use rand::seq::SliceRandom;
    use rand::RngCore;

    use super::*;

    // This runs before any tests
    static INIT: Once = Once::new();

    fn setup_logging() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_test_writer() // This ensures output goes to the test console
                .init();
        });
    }

    #[test]
    fn test_piece_length() {
        setup_logging();
        assert!(piece_length(1000, None, None) >= PIECE_LENGTH_FUNC_MIN_SIZE);
        assert!(piece_length(1000000, None, None) <= PIECE_LENGTH_FUNC_MAX_SIZE);
    }

    #[test]
    fn test_encode_decode_chunk() {
        setup_logging();
        let test_data = b"Hello, World!".to_vec();
        let encoded = encode_chunk(&test_data, 0);
        let decoded = decode_chunk(&encoded);
        assert_eq!(decoded, test_data);
    }

    #[test]
    fn test_encode_chunk_pieces() {
        setup_logging();
        let test_data = b"Test data".to_vec();
        let encoded = encode_chunk(&test_data, 0);
        let pieces = encoded.pieces;

        // Verify we have both data and parity pieces
        let data_pieces: Vec<_> = pieces
            .iter()
            .filter(|p| matches!(p.piece_type, PieceType::Data))
            .collect();
        let parity_pieces: Vec<_> = pieces
            .iter()
            .filter(|p| matches!(p.piece_type, PieceType::Parity))
            .collect();

        assert!(!data_pieces.is_empty());
        assert!(!parity_pieces.is_empty());
    }

    #[test]
    fn test_reconstruct_data() {
        setup_logging();
        let test_data = b"Test reconstruction".to_vec();
        let encoded = encode_chunk(&test_data, 0);
        let pieces = encoded.pieces.to_vec();
        let reconstructed = reconstruct_data(&pieces, &[encoded]);
        assert_eq!(reconstructed, test_data);
    }

    #[test]
    fn test_split_data() {
        setup_logging();
        const TEST_FILE_SIZE: u64 = 1024 * 1024;
        let mut test_data = vec![0u8; TEST_FILE_SIZE.try_into().unwrap()];
        rand::thread_rng().fill_bytes(&mut test_data);

        let chunk_size = piece_length(TEST_FILE_SIZE, None, None);
        let num_chunks = ((TEST_FILE_SIZE as f64) / (chunk_size as f64)).ceil() as usize;

        let mut chunks: Vec<EncodedChunk> = Vec::new();
        let mut pieces: Vec<Piece> = Vec::new();
        let mut expected_pieces: usize = 0;

        for (chunk_idx, chunk) in test_data.chunks(chunk_size.try_into().unwrap()).enumerate() {
            let chunk_info = encode_chunk(chunk, chunk_idx.try_into().unwrap());
            chunks.push(chunk_info.clone());

            let piece_size = piece_length(chunk_info.original_chunk_size, None, None);
            let pieces_per_block =
                ((chunk_info.chunk_size as f64) / (piece_size as f64)).ceil() as usize;
            let num_pieces = chunk_info.m as usize * pieces_per_block;
            expected_pieces += num_pieces;

            pieces.extend(chunk_info.pieces);
        }

        assert_eq!(
            chunks.len(),
            num_chunks,
            "Mismatch in chunk counts! Expected {}, got {}",
            num_chunks,
            chunks.len()
        );

        assert_eq!(
            pieces.len(),
            expected_pieces,
            "Mismatch in piece counts! Expected {}, got {}",
            expected_pieces,
            pieces.len()
        );
    }

    #[test]
    fn test_reconstruct_data_large() {
        setup_logging();
        const TEST_FILE_SIZE: u64 = 1024 * 1024;
        let mut test_data = vec![0u8; TEST_FILE_SIZE.try_into().unwrap()];
        rand::thread_rng().fill_bytes(&mut test_data);

        let chunk_size = piece_length(TEST_FILE_SIZE, None, None);
        let mut chunks: Vec<EncodedChunk> = Vec::new();
        let mut pieces: Vec<Piece> = Vec::new();

        for (chunk_idx, chunk) in test_data.chunks(chunk_size.try_into().unwrap()).enumerate() {
            let chunk_info = encode_chunk(chunk, chunk_idx.try_into().unwrap());
            chunks.push(chunk_info.clone());
            pieces.extend(chunk_info.pieces);
        }

        let mut rng = rand::rngs::ThreadRng::default();
        pieces.shuffle(&mut rng);

        let reconstructed_data = reconstruct_data(&pieces, &chunks);
        assert_eq!(test_data, reconstructed_data, "Data mismatch!");
    }

    #[test]
    fn test_reconstruct_data_corrupted() {
        setup_logging();
        const TEST_FILE_SIZE: u64 = 1024 * 1024;
        let mut test_data = vec![0u8; TEST_FILE_SIZE.try_into().unwrap()];
        rand::thread_rng().fill_bytes(&mut test_data);

        let chunk_size = piece_length(TEST_FILE_SIZE, None, None);
        let mut chunks: Vec<EncodedChunk> = Vec::new();
        let mut pieces: Vec<Piece> = Vec::new();

        for (chunk_idx, chunk) in test_data.chunks(chunk_size.try_into().unwrap()).enumerate() {
            let chunk_info = encode_chunk(chunk, chunk_idx.try_into().unwrap());
            chunks.push(chunk_info.clone());
            pieces.extend(chunk_info.pieces);
        }

        let mut rng = rand::rngs::ThreadRng::default();

        for chunk in &mut chunks {
            let mut pieces_vec = chunk.pieces.clone();
            let num_pieces_to_keep = ((pieces_vec.len() as f64 * 0.7).ceil()) as u64;
            pieces_vec.shuffle(&mut rng);
            pieces_vec.truncate(num_pieces_to_keep.try_into().unwrap());
            pieces_vec.shuffle(&mut rng);
        }

        let reconstructed_data = reconstruct_data(&pieces, &chunks);
        assert_eq!(test_data, reconstructed_data, "Data mismatch!");
    }

    #[test]
    fn test_reconstruct_single_chunk() {
        setup_logging();

        // Create test data for a single chunk
        let test_data = vec![0u8; 1024];
        let chunk_idx = 0;

        // Encode the chunk
        let mut encoded_chunk = encode_chunk(&test_data, chunk_idx);

        // Try to reconstruct the chunk
        let reconstructed = reconstruct_chunk(&encoded_chunk).expect("Failed to reconstruct chunk");

        assert_eq!(
            reconstructed, test_data,
            "Reconstructed chunk doesn't match original"
        );

        // Test with missing pieces (but still enough to reconstruct)
        let mut reduced_pieces = encoded_chunk.pieces.clone();
        reduced_pieces.truncate((encoded_chunk.k + 1) as usize); // Keep just enough pieces

        let reconstructed_reduced = reconstruct_chunk(&encoded_chunk)
            .expect("Failed to reconstruct chunk with reduced pieces");

        assert_eq!(
            reconstructed_reduced, test_data,
            "Reconstructed chunk from reduced pieces doesn't match original"
        );

        // Test with too few pieces
        let too_few_pieces = encoded_chunk.pieces[0..((encoded_chunk.k - 1) as usize)].to_vec();
        encoded_chunk.pieces = too_few_pieces;
        assert!(
            reconstruct_chunk(&encoded_chunk).is_err(),
            "Should return Err when there are too few pieces"
        );
    }
}
