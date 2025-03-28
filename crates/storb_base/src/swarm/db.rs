use std::io::{Error, ErrorKind};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use rocksdb::{ColumnFamily, ColumnFamilyDescriptor, Options, WriteBatch, DB};
use tokio::sync::mpsc;
use tokio::time::{timeout, Duration, Instant};
use tracing::{debug, error, info, trace, warn};

use crate::constants::DB_MPSC_BUFFER_SIZE;
use crate::memory_db::{insert_chunk_dht_value, MemoryDb};

use super::models;
use super::record::StorbRecord;

/// Configuration for the RocksDBStore.
///
/// This configuration includes the path to the database,
/// the maximum number of operations in a batch, and the maximum delay before
/// flushing a batch.
#[derive(Debug, Clone)]
pub struct RocksDBConfig {
    pub path: PathBuf,
    pub max_batch_size: usize,
    pub max_batch_delay: Duration,
}

/// An enum indicating which column family to use in the database.
#[derive(Debug, Clone, Copy)]
pub enum CfType {
    Default,
    Provider,
}

impl CfType {
    /// Returns the name of the column family as a string slice.
    fn name(&self) -> &str {
        match self {
            CfType::Default => "default",
            CfType::Provider => "provider",
        }
    }

    /// Retrieves the handle for the column family from the given database.
    ///
    /// Returns an error if the column family is not found.
    fn handle<'a>(&self, db: &'a DB) -> Result<&'a ColumnFamily, Error> {
        let cf_name = self.name();
        db.cf_handle(cf_name).ok_or_else(|| {
            Error::new(
                ErrorKind::NotFound,
                format!("Column family '{}' not found", cf_name),
            )
        })
    }
}

/// Represents a database operation to be performed.
///
/// This enum supports both put and delete operations for a specified column family.
pub enum DbOp {
    Put {
        cf: CfType,
        key: Vec<u8>,
        val: Vec<u8>,
    },
    Del {
        cf: CfType,
        key: Vec<u8>,
    },
}

/// An asynchronous wrapper around a RocksDB instance.
///
/// This store encapsulates a RocksDB database and uses a background worker to process
/// write operations in batches.
#[derive(Debug, Clone)]
pub struct RocksDBStore {
    pub db: Arc<DB>,
    sender: mpsc::Sender<DbOp>,
}

impl RocksDBStore {
    /// Creates a new RocksDBStore with the provided configuration.
    ///
    /// This function initializes the database, creates any missing column families,
    /// and spawns a background worker to process write operations in batches.
    pub fn new(config: RocksDBConfig, mem_db: Option<Arc<MemoryDb>>) -> Result<Self, Error> {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        let db_path = config.path;
        debug!("Opening RocksDBStore at {}", &db_path.to_string_lossy());

        // Define the expected column families.
        let expected_cfs = vec!["default", "provider"];
        let list = DB::list_cf(&opts, &db_path).unwrap_or_else(|_| vec!["default".to_string()]);
        debug!("Existing column families: {:?}", list);
        // If the DB already exists, check for missing CFs.
        if Path::new(&db_path).exists() {
            let existing =
                DB::list_cf(&opts, &db_path).unwrap_or_else(|_| vec!["default".to_string()]);
            for cf in &expected_cfs {
                if !existing.contains(&cf.to_string()) {
                    debug!("Column family '{}' missing, creating...", cf);
                    // Open the DB with default CF only.
                    let mut db_temp = DB::open_default(&db_path).map_err(std::io::Error::other)?;
                    // Create the missing column family.
                    db_temp
                        .create_cf(cf, &Options::default())
                        .map_err(std::io::Error::other)?;
                }
            }
        }

        // Define CF descriptors.
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("default", Options::default()),
            ColumnFamilyDescriptor::new("provider", Options::default()),
        ];

        // Open DB with all desired column families.
        let db = Arc::new(
            DB::open_cf_descriptors(&opts, &db_path, cf_descriptors)
                .map_err(std::io::Error::other)?,
        );
        debug!("Successfully opened DB with all column families.");

        // Create an mpsc channel for scheduling operations.
        let (sender, receiver) = mpsc::channel(DB_MPSC_BUFFER_SIZE);
        let db_clone = db.clone();
        tokio::spawn(async move {
            Self::bg_worker(
                db_clone,
                mem_db,
                receiver,
                config.max_batch_delay,
                config.max_batch_size,
            )
            .await;
        });
        debug!("Started RocksDBStore background worker");
        Ok(Self { db, sender })
    }

    /// Schedules a put operation in the default column family.
    ///
    /// The provided key and value are queued for an asynchronous write.
    pub async fn schedule_put(&self, key: &[u8], val: &[u8]) {
        let op = DbOp::Put {
            cf: CfType::Default,
            key: key.to_vec(),
            val: val.to_vec(),
        };
        if let Err(e) = self.sender.send(op).await {
            error!("schedule_put error: {}", e);
        }
    }

    /// Schedules a delete operation in the default column family.
    ///
    /// The key provided will be queued for deletion.
    pub async fn schedule_delete(&self, key: &[u8]) {
        let op = DbOp::Del {
            cf: CfType::Default,
            key: key.to_vec(),
        };
        if let Err(e) = self.sender.send(op).await {
            error!("schedule_delete error: {}", e);
        }
    }

    /// Schedules a put operation in the provider column family.
    ///
    /// The provided key and value are queued for an asynchronous write in the provider CF.
    pub async fn schedule_put_provider(&self, key: &[u8], val: &[u8]) {
        info!("schedule_put_provider for key: {:?}", &key);
        let op = DbOp::Put {
            cf: CfType::Provider,
            key: key.to_vec(),
            val: val.to_vec(),
        };
        if let Err(e) = self.sender.send(op).await {
            error!("schedule_put_provider error: {}", e);
        }
    }

    /// Schedules a delete operation in the provider column family.
    ///
    /// The key provided will be queued for deletion in the provider CF.
    pub async fn schedule_delete_provider(&self, key: &[u8]) {
        let op = DbOp::Del {
            cf: CfType::Provider,
            key: key.to_vec(),
        };
        if let Err(e) = self.sender.send(op).await {
            error!("schedule_delete_provider error: {}", e);
        }
    }

    /// Retrieves a value from the specified column family using the provided key.
    ///
    /// This function spawns a blocking task to perform the database read operation.
    pub async fn get(&self, cf: CfType, key: &[u8]) -> Result<Option<Vec<u8>>, Error> {
        let db = self.db.clone();
        let key = key.to_vec();
        info!("get: cf={:?}, key={:?}", &cf, &key);
        tokio::task::spawn_blocking(move || {
            let cf_handle = cf.handle(&db).map_err(std::io::Error::other)?;
            db.get_cf(cf_handle, &key).map_err(std::io::Error::other)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    pub async fn iter(&self, cf: CfType) -> Result<Vec<(Box<[u8]>, Box<[u8]>)>, Error> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let cf_handle = cf.handle(&db).map_err(std::io::Error::other)?;
            let iter = db.iterator_cf(cf_handle, rocksdb::IteratorMode::Start);
            let mut results = Vec::new();
            for item in iter {
                match item {
                    Ok((key, val)) => {
                        results.push((
                            key.to_vec().into_boxed_slice(),
                            val.to_vec().into_boxed_slice(),
                        ));
                    }
                    Err(e) => {
                        return Err(Error::other(e.to_string()));
                    }
                }
            }
            Ok(results)
        })
        .await
        .map_err(std::io::Error::other)?
    }

    /// Background worker that processes queued database operations in batches.
    ///
    /// This internal asynchronous function collects operations into a WriteBatch,
    /// waits for a maximum delay or batch size to be reached, and then writes them atomically.
    async fn bg_worker(
        db: Arc<DB>,
        mem_db: Option<Arc<MemoryDb>>,
        mut rx: mpsc::Receiver<DbOp>,
        max_batch_delay: Duration,
        max_batch_size: usize,
    ) {
        loop {
            let op = match rx.recv().await {
                Some(op) => op,
                None => {
                    info!("DB channel closed, worker exiting");
                    break;
                }
            };
            let mut batch = WriteBatch::default();
            let mut batch_size: usize = 0;
            if let Err(e) = Self::apply_op(&db, mem_db.clone(), &mut batch, op) {
                error!("Failed to apply operation to batch: {}", e);
                continue;
            }
            batch_size += 1;
            let deadline = Instant::now() + max_batch_delay;

            loop {
                let remaining = deadline.saturating_duration_since(Instant::now());
                if remaining.is_zero() {
                    // Timeout reached.
                    break;
                }

                if batch_size >= max_batch_size {
                    // Batch size reached.
                    break;
                }

                match timeout(remaining, rx.recv()).await {
                    Ok(Some(op)) => {
                        if let Err(e) = Self::apply_op(&db, mem_db.clone(), &mut batch, op) {
                            error!("Failed to apply operation to batch: {}", e);
                            continue;
                        }
                        batch_size += 1;
                    }
                    Ok(None) => {
                        debug!("DB channel closed, worker exiting, flushing batch");
                        break;
                    }
                    Err(_) => {
                        // Timeout reached.
                        break;
                    }
                }
            }

            let dbc = db.clone();
            let res = tokio::task::spawn_blocking(move || dbc.write(batch)).await;
            match res {
                Ok(Ok(())) => debug!("Atomic batch write successful ({} ops)", batch_size),
                Ok(Err(e)) => error!("Batch write error ({} ops): {}", batch_size, e),
                Err(e) => error!("spawn_blocking failed ({} ops): {}", batch_size, e),
            }
        }
    }

    /// Applies a database operation to the given write batch.
    ///
    /// This internal function updates the WriteBatch with either a put or delete operation.
    fn apply_op(
        db: &DB,
        mem_db: Option<Arc<MemoryDb>>,
        batch: &mut WriteBatch,
        op: DbOp,
    ) -> Result<(), Error> {
        match op {
            DbOp::Put { cf, key, val } => {
                let cf_handle = cf.handle(db).map_err(|e| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!("Column family not found: {}", e),
                    )
                })?;

                match bincode::deserialize::<StorbRecord>(&val) {
                    Ok(storb_record) => {
                        // Step 2: Try deserializing the inner Record's value into a DHTValue
                        match models::deserialize_dht_value(&storb_record.value) {
                            Ok(models::DHTValue::Chunk(chunk_data)) => {
                                // Success! Log the human-readable Chunk.
                                debug!(
                                    "Applying Put for Chunk [CF: {}, Key: '{}']:\n{:#?}",
                                    cf.name(),
                                    String::from_utf8_lossy(&key),
                                    chunk_data
                                );
                                // insert the chunk into the memory db
                                if let Some(mem_db) = mem_db {
                                    let mem_db = mem_db.clone();
                                    tokio::spawn(async move {
                                        let conn = mem_db.conn.clone();
                                        if let Err(e) =
                                            insert_chunk_dht_value(chunk_data, conn).await
                                        {
                                            error!("Failed to insert chunk into memory db: {}", e);
                                        }
                                    });
                                }
                            }
                            Ok(_) => {
                                // Inner value deserialized, but wasn't a chunk.
                                trace!(
                                    "Applying Put for non-Chunk DHTValue [CF: {}, Key: '{}'] (contained in StorbRecord)",
                                    cf.name(),
                                    String::from_utf8_lossy(&key)
                                );
                            }
                            Err(inner_err) => {
                                // Failed to deserialize the inner Record's value.
                                warn!(
                                    "Failed to deserialize inner Record value [CF: {}, Key: '{}', InnerError: {}] (contained in StorbRecord). Storing raw StorbRecord.",
                                    cf.name(),
                                    String::from_utf8_lossy(&key),
                                    inner_err
                                );
                            }
                        }
                    }
                    Err(outer_err) => {
                        // Failed to deserialize the outer StorbRecord wrapper.
                        warn!(
                            "Failed to deserialize StorbRecord wrapper during Put apply_op [CF: {}, Key: '{}', OuterError: {}]. Storing raw bytes.",
                            cf.name(),
                            String::from_utf8_lossy(&key),
                            outer_err
                        );
                    }
                }

                batch.put_cf(cf_handle, &key, &val);
            }
            DbOp::Del { cf, key } => {
                let cf_handle = cf.handle(db).map_err(|e| {
                    Error::new(
                        ErrorKind::NotFound,
                        format!("Column family not found: {}", e),
                    )
                })?;
                batch.delete_cf(cf_handle, &key);
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Once;
    use std::time::Duration;

    use crate::swarm::db::{CfType, RocksDBConfig, RocksDBStore};
    use crate::swarm::models::{
        deserialize_dht_value, serialize_dht_value, ChunkDHTValue, DHTValue, PieceDHTValue,
        TrackerDHTValue,
    };
    use chrono::Utc;
    use crabtensor::sign::KeypairSignature;
    use libp2p::kad::RecordKey;
    use subxt::ext::codec::Compact;
    use tempfile::tempdir;
    use tracing::error;

    use crate::piece::PieceType;

    // This runs before any tests
    static INIT: Once = Once::new();

    fn setup_logging() {
        INIT.call_once(|| {
            let _ = tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_test_writer() // This ensures output goes to the test console
                .try_init();
        });
    }

    #[tokio::test]
    async fn test_db() {
        setup_logging();
        let tmp_dir = tempdir().expect("Failed to create a temporary directory");
        let db_path = tmp_dir.path().join("test_db");

        let db = RocksDBStore::new(
            RocksDBConfig {
                path: db_path,
                max_batch_delay: Duration::from_millis(10),
                max_batch_size: 100,
            },
            None,
        )
        .expect("Failed to create RocksDBStore");

        let chunk_val = ChunkDHTValue {
            chunk_hash: RecordKey::new(&[1u8; 32]),
            validator_id: Compact(42u16),
            piece_hashes: vec![[2u8; 32], [3u8; 32]],
            chunk_idx: 123,
            k: 5,
            m: 8,
            chunk_size: 1024,
            padlen: 16,
            original_chunk_size: 1000,
            signature: KeypairSignature::from_raw([0x11; 64]),
        };
        let piece_val = PieceDHTValue {
            piece_hash: RecordKey::new(&[4u8; 32]),
            validator_id: Compact(77u16),
            chunk_idx: 456,
            piece_idx: 2,
            piece_type: PieceType::Data,
            signature: KeypairSignature::from_raw([0x11; 64]),
        };
        let tracker_val = TrackerDHTValue {
            infohash: RecordKey::new(&[5u8; 32]),
            validator_id: Compact(99),
            filename: "example.txt".to_string(),
            length: 4096,
            chunk_size: 512,
            chunk_count: 8,
            chunk_hashes: vec![[6u8; 32], [7u8; 32]],
            creation_timestamp: Utc::now(),
            signature: KeypairSignature::from_raw([0x11; 64]),
        };

        let chunk_enum = DHTValue::Chunk(chunk_val.clone());
        let piece_enum = DHTValue::Piece(piece_val.clone());
        let tracker_enum = DHTValue::Tracker(tracker_val.clone());

        let chunk_bytes = serialize_dht_value(&chunk_enum).expect("Failed to serialize chunk");
        let piece_bytes = serialize_dht_value(&piece_enum).expect("Failed to serialize piece");
        let tracker_bytes =
            serialize_dht_value(&tracker_enum).expect("Failed to serialize tracker");

        db.schedule_put(b"chunk1", &chunk_bytes).await;
        db.schedule_put(b"piece1", &piece_bytes).await;
        db.schedule_put(b"tracker1", &tracker_bytes).await;

        tokio::time::sleep(std::time::Duration::from_millis(150)).await;

        let c_bytes = db
            .get(CfType::Default, b"chunk1")
            .await
            .unwrap_or_else(|e| {
                error!("get chunk1 failed: {}", e);
                panic!("get chunk1 failed");
            });

        let p_bytes = db
            .get(crate::swarm::db::CfType::Default, b"piece1")
            .await
            .expect("get piece1 failed");
        let t_bytes = db
            .get(crate::swarm::db::CfType::Default, b"tracker1")
            .await
            .expect("get tracker1 failed");

        assert!(c_bytes.is_some(), "No bytes found for chunk1");
        assert!(p_bytes.is_some(), "No bytes found for piece1");
        assert!(t_bytes.is_some(), "No bytes found for tracker1");

        let c_val = deserialize_dht_value(&c_bytes.unwrap()).expect("Failed to deserialize chunk");
        let p_val = deserialize_dht_value(&p_bytes.unwrap()).expect("Failed to deserialize piece");
        let t_val =
            deserialize_dht_value(&t_bytes.unwrap()).expect("Failed to deserialize tracker");

        if let DHTValue::Chunk(c) = c_val {
            assert_eq!(c, chunk_val, "Chunk mismatch");
        } else {
            panic!("Expected DHTValue::Chunk");
        }
        if let DHTValue::Piece(p) = p_val {
            assert_eq!(p, piece_val, "Piece mismatch");
        } else {
            panic!("Expected DHTValue::Piece");
        }
        if let DHTValue::Tracker(t) = t_val {
            assert_eq!(t, tracker_val, "Tracker mismatch");
        } else {
            panic!("Expected DHTValue::Tracker");
        }
    }
}
