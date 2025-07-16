//! Local storage implementation

use std::fs;
use std::fs::File;
use std::io::{Read, Write};
use std::path::{Path, PathBuf};

use storb_core::hashes::PieceHash;
use tracing::debug;

use crate::{Storage, StorageError};

pub struct LocalStorage {
    path: PathBuf,
}

impl LocalStorage {
    /// Create a new local object store.
    pub fn new<P: AsRef<Path>>(store_dir: P) -> Result<Self, StorageError> {
        let mut path = PathBuf::new();
        path = path.join(store_dir);

        let object_store = Self { path: path.clone() };
        if fs::exists(&path)? {
            return Ok(object_store);
        }

        fs::create_dir(&path)?;
        for i in 0..=0xFF {
            fs::create_dir(path.join(format!("{i:02x}")))?;
        }

        Ok(object_store)
    }

    /// Get a reference to the path of the object store.
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

impl Storage for LocalStorage {
    /// Read piece data in bytes from the store.
    fn read(&self, piece_hash: &PieceHash) -> Result<Vec<u8>, StorageError> {
        let hash = piece_hash.to_string();
        let path = self.path.join(&hash[0..2]).join(&hash[2..]);

        let mut buffer = Vec::new();
        let mut f = File::open(path)?;
        f.read_to_end(&mut buffer)?;

        Ok(buffer)
    }

    /// Write piece data to the store.
    fn write(&self, piece_hash: &PieceHash, data: &Vec<u8>) -> Result<PathBuf, StorageError> {
        debug!("Writing piece {piece_hash} to store");
        let hash = piece_hash.to_string();

        let folder = self.path.join(&hash[0..2]);
        if !fs::exists(&folder)? {
            fs::create_dir(&folder)?;
        }

        let path = folder.join(&hash[2..]);

        let mut file = File::create(&path)?;
        file.write_all(data.as_slice())?;

        Ok(path)
    }
}
