//! Encoding and decoding for erasure-coded chunks.

use std::num::TryFromIntError;

use thiserror::Error;
use tracing::debug;
use zfec_rs::{Chunk as ZFecChunk, Error as ZfecError, Fec};

use crate::data::block::{block_length, get_k_and_m};
use crate::data::chunk::{Chunk, ChunkEncodingType};
use crate::data::piece::{Piece, PieceType};

#[derive(Debug, Error)]
pub enum ChunkEncodingError {
    #[error("Zfec error: {0}")]
    ZfecError(#[from] ZfecError),

    #[error("Integer conversion error: {0}")]
    IntConversionError(#[from] TryFromIntError),
}

pub trait ChunkEncoder {
    /// Encodes the given chunk (in raw bytes) into pieces.
    fn encode(&self, raw_chunk: &[u8], chunk_idx: u64) -> Result<Chunk, ChunkEncodingError>;
}

pub trait ChunkDecoder {
    /// Decodes pieces back into the original chunk (in raw bytes).
    fn decode(&self, encoded_chunk: &Chunk) -> Result<Vec<u8>, ChunkEncodingError>;
}

// TODO: Implementation for no encoding
/// No encoding for chunks.
pub struct NoEncoding {}

/// Zfec encoding for chunks.
pub struct ZfecEncoding {}

impl ZfecEncoding {
    /// Creates a new Zfec encoding instance.
    pub fn new() -> Self {
        ZfecEncoding {}
    }
}

impl ChunkEncoder for ZfecEncoding {
    fn encode(&self, raw_chunk: &[u8], chunk_idx: u64) -> Result<Chunk, ChunkEncodingError> {
        let chunk_size = raw_chunk.len() as u64;
        let piece_size = block_length(chunk_size, None, None);

        let (k, m) = get_k_and_m(chunk_size);
        debug!("[encode/zfec] chunk {chunk_idx}: {chunk_size} bytes, piece_size = {piece_size}, k = {k}, m = {m}");

        let encoder = Fec::new(k, m)?;
        let (encoded_pieces, pad_len) = encoder.encode(raw_chunk)?;

        // Calculate how Zfec splits/pads under the hood
        let zfec_chunk_size = chunk_size.div_ceil(k as u64);

        let mut pieces: Vec<Piece> = Vec::new();
        pieces.reserve(encoded_pieces.len());
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
                piece_idx: i.try_into()?,
            });
        }

        Ok(Chunk {
            encoding_type: ChunkEncodingType::Zfec,
            k: k.try_into()?,
            m: m.try_into()?,
            chunk_idx,
            chunk_size: zfec_chunk_size,
            pad_length: pad_len.try_into()?,
            original_chunk_size: chunk_size,
            pieces,
        })
    }
}

impl ChunkDecoder for ZfecEncoding {
    fn decode(&self, encoded_chunk: &Chunk) -> Result<Vec<u8>, ChunkEncodingError> {
        let k = encoded_chunk.k as usize;
        let m = encoded_chunk.m as usize;
        let pad_len = encoded_chunk.pad_length;
        let mut pieces = encoded_chunk.pieces.clone();
        pieces.sort_unstable_by_key(|p| p.piece_idx);

        let mut pieces_to_decode: Vec<ZFecChunk> = Vec::new();
        pieces_to_decode.reserve(pieces.len());

        // Zfec decode requires exactly k blocks
        if pieces.len() > k {
            for p in pieces.iter().take(k) {
                pieces_to_decode.push(ZFecChunk::new(p.data.clone(), p.piece_idx as usize));
            }
        } else {
            for p in pieces.iter() {
                pieces_to_decode.push(ZFecChunk::new(p.data.clone(), p.piece_idx as usize));
            }
        }

        let decoder = Fec::new(k, m)?;
        let decoded_chunk = decoder.decode(&pieces_to_decode, pad_len as usize)?;

        Ok(decoded_chunk)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::data::piece::PieceType;

    // Test data constants
    const TEST_CHUNK_DATA: &[u8] = b"Hello, World! This is test data for chunk encoding and decoding tests. It needs to be long enough to test various scenarios.";
    const SHORT_CHUNK_DATA: &[u8] = b"Short";
    const EMPTY_CHUNK_DATA: &[u8] = b"";

    fn create_test_data(size: usize) -> Vec<u8> {
        (0..size).map(|i| (i % 256) as u8).collect()
    }

    #[test]
    fn test_zfec_encoding_basic() {
        let encoder = ZfecEncoding::new();
        let chunk_idx = 0;

        let result = encoder.encode(TEST_CHUNK_DATA, chunk_idx);
        assert!(result.is_ok());

        let encoded_chunk = result.unwrap();
        assert_eq!(encoded_chunk.encoding_type, ChunkEncodingType::Zfec);
        assert_eq!(encoded_chunk.chunk_idx, chunk_idx);
        assert_eq!(
            encoded_chunk.original_chunk_size,
            TEST_CHUNK_DATA.len() as u64
        );
        assert!(encoded_chunk.k > 0);
        assert!(encoded_chunk.m > encoded_chunk.k);
        assert_eq!(encoded_chunk.pieces.len(), encoded_chunk.m as usize);
    }

    #[test]
    fn test_zfec_encoding_piece_types() {
        let encoder = ZfecEncoding::new();
        let chunk_idx = 1;

        let encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Check that first k pieces are data pieces
        let data_pieces: Vec<_> = encoded_chunk
            .pieces
            .iter()
            .filter(|p| p.piece_type == PieceType::Data)
            .collect();
        let parity_pieces: Vec<_> = encoded_chunk
            .pieces
            .iter()
            .filter(|p| p.piece_type == PieceType::Parity)
            .collect();

        assert_eq!(data_pieces.len(), encoded_chunk.k as usize);
        assert_eq!(
            parity_pieces.len(),
            (encoded_chunk.m - encoded_chunk.k) as usize
        );

        // Verify piece indices
        for (i, piece) in encoded_chunk.pieces.iter().enumerate() {
            assert_eq!(piece.piece_idx, i as u64);
            assert_eq!(piece.chunk_idx, chunk_idx);
            assert!(piece.piece_size > 0);
            assert!(!piece.data.is_empty());
        }
    }

    #[test]
    fn test_zfec_encoding_different_sizes() {
        let encoder = ZfecEncoding::new();
        let test_cases = vec![
            (create_test_data(100), 0),
            (create_test_data(1024), 1),
            (create_test_data(4096), 2),
            (create_test_data(16384), 3),
        ];

        for (data, chunk_idx) in test_cases {
            let result = encoder.encode(&data, chunk_idx);
            assert!(
                result.is_ok(),
                "Failed to encode chunk with size {}",
                data.len()
            );

            let encoded_chunk = result.unwrap();
            assert_eq!(encoded_chunk.original_chunk_size, data.len() as u64);
            assert_eq!(encoded_chunk.chunk_idx, chunk_idx);
            assert!(encoded_chunk.pieces.len() > 0);
        }
    }

    #[test]
    fn test_zfec_encoding_short_data() {
        let encoder = ZfecEncoding::new();
        let chunk_idx = 0;

        let result = encoder.encode(SHORT_CHUNK_DATA, chunk_idx);
        assert!(result.is_ok());

        let encoded_chunk = result.unwrap();
        assert_eq!(
            encoded_chunk.original_chunk_size,
            SHORT_CHUNK_DATA.len() as u64
        );
        assert!(encoded_chunk.k >= 1);
        assert!(encoded_chunk.m > encoded_chunk.k);
    }

    #[test]
    fn test_zfec_encoding_empty_data() {
        let encoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Empty data might be handled differently by zfec
        let result = encoder.encode(EMPTY_CHUNK_DATA, chunk_idx);
        // This might fail or succeed depending on zfec implementation
        // If it succeeds, verify basic properties
        if let Ok(encoded_chunk) = result {
            assert_eq!(encoded_chunk.original_chunk_size, 0);
            assert_eq!(encoded_chunk.chunk_idx, chunk_idx);
        }
    }

    #[test]
    fn test_zfec_decoding_basic() {
        let encoder = ZfecEncoding::new();
        let decoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Encode first
        let encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Then decode
        let result = decoder.decode(&encoded_chunk);
        assert!(result.is_ok());

        let decoded_data = result.unwrap();
        assert_eq!(decoded_data, TEST_CHUNK_DATA);
    }

    #[test]
    fn test_zfec_encode_decode_roundtrip() {
        let encoder = ZfecEncoding::new();
        let decoder = ZfecEncoding::new();

        let test_cases = vec![
            (create_test_data(100), 0),
            (create_test_data(1024), 1),
            (create_test_data(4096), 2),
            (TEST_CHUNK_DATA.to_vec(), 3),
            (SHORT_CHUNK_DATA.to_vec(), 4),
        ];

        for (original_data, chunk_idx) in test_cases {
            // Encode
            let encoded_chunk = encoder.encode(&original_data, chunk_idx).unwrap();

            // Decode
            let decoded_data = decoder.decode(&encoded_chunk).unwrap();

            // Verify roundtrip
            assert_eq!(
                decoded_data,
                original_data,
                "Roundtrip failed for chunk {} with size {}",
                chunk_idx,
                original_data.len()
            );
        }
    }

    #[test]
    fn test_zfec_decoding_with_missing_pieces() {
        let encoder = ZfecEncoding::new();
        let decoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Encode first
        let mut encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Remove some parity pieces (should still be decodable with k data pieces)
        let k = encoded_chunk.k as usize;
        encoded_chunk.pieces.truncate(k);

        // Should still be able to decode with exactly k pieces
        let result = decoder.decode(&encoded_chunk);
        assert!(result.is_ok());

        let decoded_data = result.unwrap();
        assert_eq!(decoded_data, TEST_CHUNK_DATA);
    }

    #[test]
    fn test_zfec_decoding_with_insufficient_pieces() {
        let encoder = ZfecEncoding::new();
        let decoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Encode first
        let mut encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Remove too many pieces (less than k)
        let k = encoded_chunk.k as usize;
        if k > 1 {
            encoded_chunk.pieces.truncate(k - 1);

            // Should fail to decode
            let result = decoder.decode(&encoded_chunk);
            assert!(result.is_err());
        }
    }

    #[test]
    fn test_zfec_decoding_with_mixed_pieces() {
        let encoder = ZfecEncoding::new();
        let decoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Encode first
        let mut encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Keep some data pieces and some parity pieces
        let k = encoded_chunk.k as usize;
        let m = encoded_chunk.m as usize;

        if k > 2 && m > k + 1 {
            let mut selected_pieces = Vec::new();

            // Take first k-1 data pieces
            selected_pieces.extend(encoded_chunk.pieces.iter().take(k - 1).cloned());

            // Take one parity piece
            selected_pieces.push(encoded_chunk.pieces[k].clone());

            encoded_chunk.pieces = selected_pieces;

            // Should still be able to decode
            let result = decoder.decode(&encoded_chunk);
            assert!(result.is_ok());

            let decoded_data = result.unwrap();
            assert_eq!(decoded_data, TEST_CHUNK_DATA);
        }
    }

    #[test]
    fn test_zfec_encoding_chunk_properties() {
        let encoder = ZfecEncoding::new();
        let chunk_idx = 42;

        let encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Test chunk properties
        assert_eq!(encoded_chunk.encoding_type, ChunkEncodingType::Zfec);
        assert_eq!(encoded_chunk.chunk_idx, chunk_idx);
        assert_eq!(
            encoded_chunk.original_chunk_size,
            TEST_CHUNK_DATA.len() as u64
        );
        assert!(encoded_chunk.chunk_size > 0);
        // pad_length is u64, so it's always >= 0
        assert!(encoded_chunk.k > 0);
        assert!(encoded_chunk.m > encoded_chunk.k);

        // Test piece properties
        for (i, piece) in encoded_chunk.pieces.iter().enumerate() {
            assert_eq!(piece.chunk_idx, chunk_idx);
            assert_eq!(piece.piece_idx, i as u64);
            assert!(piece.piece_size > 0);
            assert!(!piece.data.is_empty());

            if i < encoded_chunk.k as usize {
                assert_eq!(piece.piece_type, PieceType::Data);
            } else {
                assert_eq!(piece.piece_type, PieceType::Parity);
            }
        }
    }

    #[test]
    fn test_zfec_encoding_deterministic() {
        let encoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Encode the same data multiple times
        let encoded1 = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();
        let encoded2 = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Should produce identical results
        assert_eq!(encoded1.k, encoded2.k);
        assert_eq!(encoded1.m, encoded2.m);
        assert_eq!(encoded1.chunk_size, encoded2.chunk_size);
        assert_eq!(encoded1.pad_length, encoded2.pad_length);
        assert_eq!(encoded1.original_chunk_size, encoded2.original_chunk_size);
        assert_eq!(encoded1.pieces.len(), encoded2.pieces.len());

        // Compare piece data
        for (piece1, piece2) in encoded1.pieces.iter().zip(encoded2.pieces.iter()) {
            assert_eq!(piece1.data, piece2.data);
            assert_eq!(piece1.piece_type, piece2.piece_type);
            assert_eq!(piece1.piece_idx, piece2.piece_idx);
        }
    }

    #[test]
    fn test_zfec_encoding_large_data() {
        let encoder = ZfecEncoding::new();
        let decoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Test with larger data
        let large_data = create_test_data(1024 * 1024); // 1MB

        let encoded_chunk = encoder.encode(&large_data, chunk_idx).unwrap();
        assert_eq!(encoded_chunk.original_chunk_size, large_data.len() as u64);
        assert!(encoded_chunk.pieces.len() > 0);

        // Verify it can be decoded
        let decoded_data = decoder.decode(&encoded_chunk).unwrap();
        assert_eq!(decoded_data, large_data);
    }

    #[test]
    fn test_zfec_encoding_error_handling() {
        let encoder = ZfecEncoding::new();

        // Test with extreme values that might cause issues
        let result = encoder.encode(&[0u8; 0], 0);
        // This might succeed or fail depending on zfec implementation
        // Just verify it doesn't panic
        let _ = result;
    }

    #[test]
    fn test_chunk_encoding_error_display() {
        // Test error formatting - using TryFromIntError as an example
        let result: Result<u32, TryFromIntError> = u64::MAX.try_into();
        let int_error = result.unwrap_err();
        let chunk_error = ChunkEncodingError::IntConversionError(int_error);

        let error_string = format!("{}", chunk_error);
        assert!(error_string.contains("Integer conversion error"));
    }

    #[test]
    fn test_zfec_piece_ordering_after_encoding() {
        let encoder = ZfecEncoding::new();
        let chunk_idx = 0;

        let encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Verify pieces are in order
        for (i, piece) in encoded_chunk.pieces.iter().enumerate() {
            assert_eq!(piece.piece_idx, i as u64);
        }

        // Verify data pieces come before parity pieces
        let k = encoded_chunk.k as usize;
        for i in 0..k {
            assert_eq!(encoded_chunk.pieces[i].piece_type, PieceType::Data);
        }
        for i in k..encoded_chunk.pieces.len() {
            assert_eq!(encoded_chunk.pieces[i].piece_type, PieceType::Parity);
        }
    }

    #[test]
    fn test_zfec_decoding_piece_sorting() {
        let encoder = ZfecEncoding::new();
        let decoder = ZfecEncoding::new();
        let chunk_idx = 0;

        // Encode first
        let mut encoded_chunk = encoder.encode(TEST_CHUNK_DATA, chunk_idx).unwrap();

        // Shuffle pieces to test sorting in decode
        encoded_chunk.pieces.reverse();

        // Should still decode correctly due to internal sorting
        let decoded_data = decoder.decode(&encoded_chunk).unwrap();
        assert_eq!(decoded_data, TEST_CHUNK_DATA);
    }

    #[test]
    fn test_zfec_new_instance() {
        let encoder1 = ZfecEncoding::new();
        let encoder2 = ZfecEncoding::new();

        // Both should work independently
        let chunk1 = encoder1.encode(TEST_CHUNK_DATA, 0).unwrap();
        let chunk2 = encoder2.encode(TEST_CHUNK_DATA, 1).unwrap();

        assert_eq!(chunk1.k, chunk2.k);
        assert_eq!(chunk1.m, chunk2.m);
        assert_ne!(chunk1.chunk_idx, chunk2.chunk_idx);
    }
}
