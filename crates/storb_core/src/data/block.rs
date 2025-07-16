use crate::constants::{
    BLOCK_LENGTH_FUNC_MAX_SIZE, BLOCK_LENGTH_FUNC_MIN_SIZE, BLOCK_LENGTH_OFFSET,
    BLOCK_LENGTH_SCALING,
};

/// Calculate the block length based on content size.
///
/// Optional parameters allow clamping the result to a specified minimum and maximum size,
/// otherwise defaulting to predefined constants.
pub fn block_length(content_length: u64, min_size: Option<u64>, max_size: Option<u64>) -> u64 {
    let min_size = min_size.unwrap_or(BLOCK_LENGTH_FUNC_MIN_SIZE);
    let max_size = max_size.unwrap_or(BLOCK_LENGTH_FUNC_MAX_SIZE);

    // Calculate ideal length based on content size using log scaling
    if content_length == 0 {
        return min_size;
    }
    let exponent =
        ((content_length as f64).log2() * BLOCK_LENGTH_SCALING + BLOCK_LENGTH_OFFSET) as i32;
    let length = 1u64 << exponent;

    length.clamp(min_size, max_size)
}

/// Calculate the `k` and `m` parameters for erasure coding.
///
/// TODO: we might change how we determined these in the future - related issue:
/// https://github.com/storb-tech/storb/issues/66
pub fn get_k_and_m(chunk_size: u64) -> (usize, usize) {
    let piece_size = block_length(chunk_size, None, None);

    // Calculate how many data blocks (k) and parity blocks
    let expected_data_pieces = ((chunk_size as f64) / (piece_size as f64)).ceil() as usize;
    let expected_parity_pieces = ((expected_data_pieces as f64) / 2.0).ceil() as usize;

    let k = expected_data_pieces;
    let m = k + expected_parity_pieces;

    (k, m)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::constants::{BLOCK_LENGTH_FUNC_MAX_SIZE, BLOCK_LENGTH_FUNC_MIN_SIZE};

    #[test]
    fn test_block_length_with_defaults() {
        // Test with various content sizes using default min/max
        let test_cases = vec![
            (1024, None, None),
            (1024 * 1024, None, None),
            (1024 * 1024 * 1024, None, None),
            (1, None, None),
            (u64::MAX, None, None),
        ];

        for (content_length, min_size, max_size) in test_cases {
            let result = block_length(content_length, min_size, max_size);

            // Should be within default bounds
            assert!(result >= BLOCK_LENGTH_FUNC_MIN_SIZE);
            assert!(result <= BLOCK_LENGTH_FUNC_MAX_SIZE);

            // Should be a power of 2
            assert!(result.is_power_of_two());
        }
    }

    #[test]
    fn test_block_length_with_custom_bounds() {
        let content_length = 1024 * 1024; // 1 MiB
        let min_size = Some(512);
        let max_size = Some(8192);

        let result = block_length(content_length, min_size, max_size);

        assert!(result >= 512);
        assert!(result <= 8192);
        assert!(result.is_power_of_two());
    }

    #[test]
    fn test_block_length_clamping_to_minimum() {
        let content_length = 1; // Very small content
        let min_size = Some(1024);
        let max_size = Some(1024 * 1024);

        let result = block_length(content_length, min_size, max_size);

        assert_eq!(result, 1024); // Should be clamped to minimum
    }

    #[test]
    fn test_block_length_clamping_to_maximum() {
        let content_length = u64::MAX; // Very large content
        let min_size = Some(1024);
        let max_size = Some(1024 * 1024);

        let result = block_length(content_length, min_size, max_size);

        assert_eq!(result, 1024 * 1024); // Should be clamped to maximum
    }

    #[test]
    fn test_block_length_power_of_two_property() {
        // Test various sizes to ensure result is always a power of 2
        let test_sizes = vec![
            100, 1000, 10000, 100000, 1000000, 1024, 2048, 4096, 8192, 16384,
        ];

        for size in test_sizes {
            let result = block_length(size, None, None);
            assert!(
                result.is_power_of_two(),
                "Result {} should be power of 2 for input {}",
                result,
                size
            );
        }
    }

    #[test]
    fn test_block_length_zero_content() {
        let result = block_length(0, None, None);
        assert!(result >= BLOCK_LENGTH_FUNC_MIN_SIZE);
        assert!(result <= BLOCK_LENGTH_FUNC_MAX_SIZE);
        assert!(result.is_power_of_two());
    }

    #[test]
    fn test_block_length_edge_cases() {
        // Test edge cases with unusual min/max combinations
        let content_length = 1024;

        // Min equals max
        let result = block_length(content_length, Some(2048), Some(2048));
        assert_eq!(result, 2048);

        // Min larger than calculated value
        let result = block_length(1, Some(1024), Some(2048));
        assert_eq!(result, 1024);

        // Max smaller than calculated value
        let result = block_length(u64::MAX, Some(1024), Some(2048));
        assert_eq!(result, 2048);
    }

    #[test]
    fn test_get_k_and_m_basic() {
        let chunk_size = 1024 * 1024; // 1 MiB
        let (k, m) = get_k_and_m(chunk_size);

        // k should be positive
        assert!(k > 0);

        // m should be greater than k (includes parity blocks)
        assert!(m > k);

        // Parity blocks should be roughly half of data blocks
        let parity_blocks = m - k;
        let expected_parity = ((k as f64) / 2.0).ceil() as usize;
        assert_eq!(parity_blocks, expected_parity);
    }

    #[test]
    fn test_get_k_and_m_small_chunk() {
        let chunk_size = 1024; // 1 KiB
        let (k, m) = get_k_and_m(chunk_size);

        assert!(k > 0);
        assert!(m > k);

        // For small chunks, should still have at least one data block
        assert!(k >= 1);

        // Should have appropriate parity ratio
        let parity_blocks = m - k;
        let expected_parity = ((k as f64) / 2.0).ceil() as usize;
        assert_eq!(parity_blocks, expected_parity);
    }

    #[test]
    fn test_get_k_and_m_large_chunk() {
        let chunk_size = 1024 * 1024 * 1024; // 1 GiB
        let (k, m) = get_k_and_m(chunk_size);

        assert!(k > 0);
        assert!(m > k);

        // For large chunks, should have more blocks
        assert!(k > 1);

        // Verify parity calculation
        let parity_blocks = m - k;
        let expected_parity = ((k as f64) / 2.0).ceil() as usize;
        assert_eq!(parity_blocks, expected_parity);
    }

    #[test]
    fn test_get_k_and_m_zero_chunk_size() {
        let (k, m) = get_k_and_m(0);

        // Should handle zero gracefully
        assert_eq!(k, 0);
        assert_eq!(m, 0);
    }

    #[test]
    fn test_get_k_and_m_consistency() {
        // Test that the same chunk size always produces the same k and m
        let chunk_size = 1024 * 1024;

        let (k1, m1) = get_k_and_m(chunk_size);
        let (k2, m2) = get_k_and_m(chunk_size);

        assert_eq!(k1, k2);
        assert_eq!(m1, m2);
    }
}
