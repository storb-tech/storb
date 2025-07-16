//! In-memory storage, mostly for testing purposes

use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

use storb_core::hashes::PieceHash;

use crate::{Storage, StorageError};

/// In-memory storage implementation.
pub struct MemoryStorage {
    data: Arc<RwLock<HashMap<PieceHash, Vec<u8>>>>,
}

impl MemoryStorage {
    /// Create a new empty memory storage.
    pub fn new() -> Self {
        Self {
            data: Arc::new(RwLock::new(HashMap::new())),
        }
    }

    /// Get the number of pieces stored.
    pub fn len(&self) -> usize {
        self.data.read().unwrap().len()
    }

    /// Check if storage is empty.
    pub fn is_empty(&self) -> bool {
        self.data.read().unwrap().is_empty()
    }

    /// Clear all stored data.
    pub fn clear(&self) {
        self.data.write().unwrap().clear();
    }

    /// Check if a piece exists.
    pub fn contains(&self, piece_hash: &PieceHash) -> bool {
        self.data.read().unwrap().contains_key(piece_hash)
    }
}

impl Default for MemoryStorage {
    fn default() -> Self {
        Self::new()
    }
}

impl Storage for MemoryStorage {
    fn read(&self, piece_hash: &PieceHash) -> Result<Vec<u8>, StorageError> {
        let data = self.data.read().unwrap();
        match data.get(piece_hash) {
            Some(piece_data) => Ok(piece_data.clone()),
            None => Err(StorageError::ReadError(format!(
                "Piece not found: {piece_hash}"
            ))),
        }
    }

    fn write(&self, piece_hash: &PieceHash, data: &Vec<u8>) -> Result<PathBuf, StorageError> {
        let mut storage = self.data.write().unwrap();
        storage.insert(*piece_hash, data.clone());

        // Return a fake path since we're in memory
        Ok(PathBuf::from(format!(
            "memory://{}",
            piece_hash.to_string()
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_memory_storage_basic_operations() {
        let storage = MemoryStorage::new();
        assert!(storage.is_empty());
        assert_eq!(storage.len(), 0);

        // Create test data
        let test_data = b"Hello, World!".to_vec();
        let hash = blake3::hash(&test_data);
        let piece_hash = PieceHash::new(*hash.as_bytes());

        // Write data
        let result = storage.write(&piece_hash, &test_data);
        assert!(result.is_ok());
        assert_eq!(storage.len(), 1);
        assert!(storage.contains(&piece_hash));

        // Read data back
        let read_result = storage.read(&piece_hash);
        assert!(read_result.is_ok());
        assert_eq!(read_result.unwrap(), test_data);

        // Clear storage
        storage.clear();
        assert!(storage.is_empty());
        assert_eq!(storage.len(), 0);
    }

    #[test]
    fn test_memory_storage_missing_piece() {
        let storage = MemoryStorage::new();
        let test_hash = PieceHash::new([42u8; 32]);

        // Try to read non-existent piece (should result in an error)
        let result = storage.read(&test_hash);
        assert!(result.is_err());
    }
}
