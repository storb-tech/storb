use std::path::PathBuf;

use storb_core::hashes::PieceHash;
use thiserror::Error;

pub mod local;
pub mod memory;

#[derive(Debug, Error)]
pub enum StorageError {
    #[error("IO error: {0}")]
    IoError(#[from] std::io::Error),

    #[error("Storage read error: {0}")]
    ReadError(String),

    #[error("Storage write error: {0}")]
    WriteError(String),
}

/// Object storage interface.
pub trait Storage {
    fn read(&self, piece_hash: &PieceHash) -> Result<Vec<u8>, StorageError>;

    fn write(&self, piece_hash: &PieceHash, data: &Vec<u8>) -> Result<PathBuf, StorageError>;
}
