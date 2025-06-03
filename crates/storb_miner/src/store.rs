use std::fs;
use std::io::Error;
use std::path::{Path, PathBuf};

use base::piece_hash::PieceHashStr;
use tokio::fs::File;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tracing::info;

/// Object store to store the piece data
#[derive(Clone)]
pub struct ObjectStore {
    path: PathBuf,
}

impl ObjectStore {
    /// Create a new object store
    pub fn new<P: AsRef<Path>>(store_dir: P) -> Result<Self, Error> {
        let mut path = PathBuf::new();
        path = path.join(store_dir);

        let object_store = ObjectStore { path: path.clone() };

        if fs::exists(&path).expect("Could not check if file exists or not") {
            return Ok(object_store);
        }

        fs::create_dir(&path)?;
        for i in 0..=0xFF {
            fs::create_dir(path.join(format!("{i:02x}")))?;
        }

        Ok(object_store)
    }

    /// Read piece data in bytes from the store
    pub async fn read(&self, piece_hash: &PieceHashStr) -> Result<Vec<u8>, Error> {
        let path = self.path.join(&piece_hash[0..2]).join(&piece_hash[2..]);

        let mut buffer = vec![];
        let mut f = File::open(path).await?;
        f.read_to_end(&mut buffer).await?;

        Ok(buffer)
    }

    /// Write piece data to the store
    pub async fn write(&self, piece_hash: &PieceHashStr, data: &Vec<u8>) -> Result<PathBuf, Error> {
        info!("Writing piece {piece_hash} to store");

        let folder = self.path.join(&piece_hash[0..2]);
        if !fs::exists(&folder).expect("Could not check if file exists or not") {
            fs::create_dir(&folder)?;
        }

        let path = folder.join(&piece_hash[2..]);

        let mut file = File::create(&path).await?;
        file.write_all(data.as_slice()).await?;

        Ok(path)
    }

    /// Get a reference to the path of the object store
    pub fn path(&self) -> &PathBuf {
        &self.path
    }
}

#[cfg(test)]
mod tests {
    use std::fs;
    use std::io::Error;

    use base::piece_hash::PieceHashStr;
    use rand::Rng;

    use crate::store::ObjectStore;

    fn generate_rand_string(len: usize) -> String {
        let charset = b"0123456789abcdef";
        let mut rng = rand::thread_rng();
        let random_string: String = (0..len)
            .map(|_| charset[rng.gen_range(0..charset.len())] as char)
            .collect();
        random_string
    }

    fn generate_rand_bytes(len: usize) -> Vec<u8> {
        let mut rng = rand::thread_rng();
        let random_bytes: Vec<u8> = (0..len).map(|_| rng.gen()).collect();
        random_bytes
    }

    fn generate_rand_hash() -> String {
        blake3::hash(generate_rand_string(64).as_bytes())
            .to_hex()
            .to_string()
    }

    fn create_test_dir_str() -> String {
        format!("test_dir_{}", generate_rand_string(10))
    }

    fn cleanup_test_dir(path: &str) -> Result<(), Error> {
        fs::remove_dir_all(path)
    }

    #[test]
    fn test_creating_object_store() {
        let test_dir = create_test_dir_str();
        let store = ObjectStore::new(&test_dir).unwrap();

        assert!(fs::exists(&store.path).unwrap());
        cleanup_test_dir(&test_dir).unwrap();
    }

    #[test]
    fn test_creating_object_store_existing_dir() {
        let test_dir = create_test_dir_str();
        let _ = ObjectStore::new(&test_dir).unwrap();
        // Ensure creating an object store where directory already exists works
        let store = ObjectStore::new(&test_dir).unwrap();

        assert!(fs::exists(&store.path).unwrap());
        cleanup_test_dir(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn test_create_file() {
        let test_dir = create_test_dir_str();
        let store = ObjectStore::new(&test_dir).unwrap();
        let piece_hash = PieceHashStr::new(generate_rand_hash()).unwrap();
        let data = generate_rand_bytes(1024);

        let piece_path = store.write(&piece_hash, &data).await.unwrap();

        assert!(fs::exists(store.path()).unwrap());
        assert!(fs::exists(&piece_path).unwrap());
        cleanup_test_dir(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn test_read_file() {
        let test_dir = create_test_dir_str();
        let store = ObjectStore::new(&test_dir).unwrap();
        let piece_hash = PieceHashStr::new(generate_rand_hash()).unwrap();
        let data = generate_rand_bytes(1024);

        let piece_path = store.write(&piece_hash, &data).await.unwrap();

        assert!(fs::exists(store.path()).unwrap());
        assert!(fs::exists(&piece_path).unwrap());

        let read_data = store.read(&piece_hash).await.unwrap();
        assert!(data == read_data);

        cleanup_test_dir(&test_dir).unwrap();
    }

    #[tokio::test]
    async fn test_overwrite_existing_file() {
        let test_dir = create_test_dir_str();
        let store = ObjectStore::new(&test_dir).unwrap();
        let piece_hash = PieceHashStr::new(generate_rand_hash()).unwrap();
        let data = generate_rand_bytes(1024);
        let data2 = generate_rand_bytes(1024);

        let _ = store.write(&piece_hash, &data).await.unwrap();
        // Call write once again to overwrite the file
        let piece_path = store.write(&piece_hash, &data2).await.unwrap();

        assert!(fs::exists(store.path()).unwrap());
        assert!(fs::exists(&piece_path).unwrap());

        let read_data = store.read(&piece_hash).await.unwrap();
        assert!(data != read_data);
        assert!(data2 == read_data);

        cleanup_test_dir(&test_dir).unwrap();
    }
}
