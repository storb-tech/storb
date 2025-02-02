use crate::constants::{MAX_PIECE_SIZE, MIN_PIECE_SIZE, PIECE_LENGTH_OFFSET, PIECE_LENGTH_SCALING};
use tracing::debug;
use zfec_rs::Chunk as ZFecChunk;

#[derive(Debug, Clone)]
pub struct Piece {
    pub chunk_idx: i32,
    pub piece_idx: i32,
    pub piece_type: PieceType,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone, PartialEq)]
pub enum PieceType {
    Data,
    Parity,
}

#[derive(Debug, Clone)]
pub struct EncodedChunk {
    pub pieces: Option<Vec<Piece>>,
    pub chunk_idx: i32,
    pub k: i32, // Number of data blocks
    pub m: i32, // Total blocks (data + parity)
    pub chunk_size: i32,
    pub padlen: i32,
    pub original_chunk_size: i32,
}

pub fn piece_length(
    content_length: usize,
    min_size: Option<usize>,
    max_size: Option<usize>,
) -> i32 {
    let min_size: i32 = min_size.unwrap_or(MIN_PIECE_SIZE).try_into().unwrap();
    let max_size: i32 = max_size.unwrap_or(MAX_PIECE_SIZE).try_into().unwrap();
    let exponent =
        ((content_length as f64).log2() * PIECE_LENGTH_SCALING + PIECE_LENGTH_OFFSET) as i32;
    let length: i32 = (1_usize << exponent).try_into().unwrap();

    if length < min_size {
        min_size
    } else if length > max_size {
        max_size
    } else {
        length
    }
}

pub fn encode_chunk(chunk: &[u8], chunk_idx: u32) -> EncodedChunk {
    let chunk_size = chunk.len();
    let piece_size = piece_length(chunk_size, None, None);
    debug!("[encode_chunk] chunk {chunk_idx}: {chunk_size} bytes, piece_size = {piece_size}");
    // Calculate how many data blocks (k) and parity blocks
    let expected_data_pieces = ((chunk_size as f64) / (piece_size as f64)).ceil() as usize;
    let expected_parity_pieces = ((expected_data_pieces as f64) / 2.0).ceil() as usize;

    let k = expected_data_pieces;
    let m = k + expected_parity_pieces;

    let encoder = zfec_rs::Fec::new(k, m).expect("Failed to create encoder");
    let (encoded_pieces, padlen) = encoder.encode(chunk).expect("Failed to encode chunk");

    // Calculate how zfec splits/pads under the hood
    let zfec_chunk_size = chunk_size + (k - 1);

    let mut pieces: Vec<Piece> = Vec::new();
    for (i, piece) in encoded_pieces.into_iter().enumerate() {
        let piece_data = piece.data;
        let piece_type = if i < k {
            PieceType::Data
        } else {
            PieceType::Parity
        };
        debug!("Encoding piece {} with length {}", i, piece_data.len());
        pieces.push(Piece {
            piece_type,
            data: piece_data,
            chunk_idx: chunk_idx as i32,
            piece_idx: i as i32,
        });
    }

    for piece in &pieces {
        debug!("[encode] Piece length: {}", piece.data.len());
    }

    let encoded_chunk = EncodedChunk {
        pieces: Some(pieces),
        chunk_idx: chunk_idx as i32,
        k: k as i32,
        m: m as i32,
        chunk_size: zfec_chunk_size as i32,
        padlen: padlen as i32,
        original_chunk_size: chunk_size as i32,
    };

    debug!(
        "[encode_chunk] chunk {}: k={}, m={}, encoded {} blocks",
        chunk_idx,
        k,
        m,
        encoded_chunk.pieces.as_ref().unwrap().len()
    );
    encoded_chunk
}

pub fn decode_chunk(encoded_chunk: &EncodedChunk) -> Vec<u8> {
    let k = encoded_chunk.k;
    let m = encoded_chunk.m;
    let padlen = encoded_chunk.padlen;
    let pieces = encoded_chunk
        .pieces
        .as_ref()
        .expect("Pieces must be present in EncodedChunk");

    let mut pieces_to_decode: Vec<ZFecChunk> = Vec::new();
    let mut sharenums: Vec<usize> = Vec::new();

    // zfec decode requires exactly k blocks
    if pieces.len() > k as usize {
        for (i, p) in pieces.iter().take(k as usize).enumerate() {
            pieces_to_decode.push(ZFecChunk::new(p.data.clone(), i));
            sharenums.push(i);
        }
    } else {
        for (i, p) in pieces.iter().enumerate() {
            pieces_to_decode.push(ZFecChunk::new(p.data.clone(), i));
            sharenums.push(i);
        }
    }

    let decoder = zfec_rs::Fec::new(k as usize, m as usize).expect("Failed to create decoder");
    decoder
        .decode(&pieces_to_decode, padlen as usize)
        .expect("Failed to decode chunk")
}

pub fn reconstruct_data(pieces: &[Piece], chunks: &[EncodedChunk]) -> Vec<u8> {
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
        if relevant_pieces.len() < k as usize {
            panic!("Not enough pieces to reconstruct chunk {}", chunk_idx);
        }

        let mut chunk_to_decode = chunk.clone();
        chunk_to_decode.pieces = Some(relevant_pieces);
        let reconstructed_chunk = decode_chunk(&chunk_to_decode);
        reconstructed_chunks.push(reconstructed_chunk);
    }

    reconstructed_chunks.concat()
}

#[cfg(test)]
mod tests {
    use super::*;
    use rand;
    use rand::seq::SliceRandom;

    #[test]
    fn test_piece_length() {
        assert!(piece_length(1000, None, None) >= MIN_PIECE_SIZE as i32);
        assert!(piece_length(1000000, None, None) <= MAX_PIECE_SIZE as i32);
    }

    #[test]
    fn test_encode_decode_chunk() {
        let test_data = b"Hello, World!".to_vec();
        let encoded = encode_chunk(&test_data, 0);
        let decoded = decode_chunk(&encoded);
        assert_eq!(decoded, test_data);
    }

    #[test]
    fn test_encode_chunk_pieces() {
        let test_data = b"Test data".to_vec();
        let encoded = encode_chunk(&test_data, 0);
        let pieces = encoded.pieces.unwrap();

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
        let test_data = b"Test reconstruction".to_vec();
        let encoded = encode_chunk(&test_data, 0);
        let pieces = encoded.pieces.as_ref().unwrap().to_vec();
        let reconstructed = reconstruct_data(&pieces, &[encoded]);
        assert_eq!(reconstructed, test_data);
    }

    #[test]
    #[should_panic]
    fn test_reconstruct_data_insufficient_pieces() {
        let test_data = b"Test insufficient pieces".to_vec();
        let encoded = encode_chunk(&test_data, 0);
        let pieces = encoded.pieces.as_ref().unwrap().clone();
        let truncated_pieces = pieces.into_iter().take(1).collect::<Vec<_>>();
        reconstruct_data(&truncated_pieces, &[encoded]);
    }

    #[test]
    fn test_split_data() {
        const TEST_FILE_SIZE: usize = 1024 * 1024;
        let test_data = vec![0u8; TEST_FILE_SIZE];

        let chunk_size = piece_length(TEST_FILE_SIZE, None, None) as usize;
        let num_chunks = ((TEST_FILE_SIZE as f64) / (chunk_size as f64)).ceil() as usize;

        let mut chunks: Vec<EncodedChunk> = Vec::new();
        let mut pieces: Vec<Piece> = Vec::new();
        let mut expected_pieces = 0;

        for (chunk_idx, chunk) in test_data.chunks(chunk_size).enumerate() {
            let chunk_info = encode_chunk(chunk, chunk_idx as u32);
            chunks.push(chunk_info.clone());

            let piece_size = piece_length(chunk_info.original_chunk_size as usize, None, None);
            let pieces_per_block =
                ((chunk_info.chunk_size as f64) / (piece_size as f64)).ceil() as i32;
            let num_pieces = chunk_info.m * pieces_per_block;
            expected_pieces += num_pieces;

            pieces.extend(chunk_info.pieces.unwrap());
        }

        assert_eq!(
            chunks.len(),
            num_chunks,
            "Mismatch in chunk counts! Expected {}, got {}",
            num_chunks,
            chunks.len()
        );

        assert_eq!(
            pieces.len() as i32,
            expected_pieces,
            "Mismatch in piece counts! Expected {}, got {}",
            expected_pieces,
            pieces.len()
        );
    }

    #[test]
    fn test_reconstruct_data_large() {
        const TEST_FILE_SIZE: usize = 1024 * 1024;
        let test_data = vec![0u8; TEST_FILE_SIZE];

        let chunk_size = piece_length(TEST_FILE_SIZE, None, None) as usize;
        let mut chunks: Vec<EncodedChunk> = Vec::new();
        let mut pieces: Vec<Piece> = Vec::new();

        for (chunk_idx, chunk) in test_data.chunks(chunk_size).enumerate() {
            let chunk_info = encode_chunk(chunk, chunk_idx as u32);
            chunks.push(chunk_info.clone());
            pieces.extend(chunk_info.pieces.unwrap());
        }

        let mut rng = rand::rngs::ThreadRng::default();
        pieces.shuffle(&mut rng);

        let reconstructed_data = reconstruct_data(&pieces, &chunks);
        assert_eq!(test_data, reconstructed_data, "Data mismatch!");
    }

    #[test]
    fn test_reconstruct_data_corrupted() {
        const TEST_FILE_SIZE: usize = 1024 * 1024;
        let test_data = vec![0u8; TEST_FILE_SIZE];

        let chunk_size = piece_length(TEST_FILE_SIZE, None, None) as usize;
        let mut chunks: Vec<EncodedChunk> = Vec::new();
        let mut pieces: Vec<Piece> = Vec::new();

        for (chunk_idx, chunk) in test_data.chunks(chunk_size).enumerate() {
            let chunk_info = encode_chunk(chunk, chunk_idx as u32);
            chunks.push(chunk_info.clone());
            pieces.extend(chunk_info.pieces.unwrap());
        }

        let mut rng = rand::rngs::ThreadRng::default();

        // Keep 70% of pieces
        let num_pieces_to_keep = (pieces.len() as f64 * 0.7) as usize;
        pieces.shuffle(&mut rng);
        pieces.truncate(num_pieces_to_keep);

        pieces.shuffle(&mut rng);

        let reconstructed_data = reconstruct_data(&pieces, &chunks);
        assert_eq!(test_data, reconstructed_data, "Data mismatch!");
    }
}
