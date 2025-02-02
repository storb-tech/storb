/// Commands sent to the background flush task.
enum FlushCommand {
    /// Flush or update a regular record on disk.
    FlushRecord(Record),
    /// Remove a regular record from disk.
    RemoveRecord(RecordKey),
    /// Flush or update a provider record on disk.
    FlushProviderRecord(ProviderRecord),
    /// Remove a provider record from disk.
    RemoveProviderRecord(RecordKey, PeerId),
}

/// A production-ready RocksDB-backed RecordStore that uses MemoryStore for immediate operations,
/// while offloading disk writes/removals asynchronously.
pub struct RocksDBStore {
    /// Shared handle to the RocksDB instance.
    db: Arc<DB>,
    /// Handle to the column family for regular records.
    records_cf: rocksdb::ColumnFamily,
    /// Handle to the column family for provider records.
    provider_cf: rocksdb::ColumnFamily,
    /// Local peer ID.
    local_peer_id: PeerId,
    /// In-memory cache for immediate record operations.
    cache: MemoryStore,
    /// Channel sender used to schedule flush operations.
    flush_sender: mpsc::Sender<FlushCommand>,
}

impl RocksDBStore {
    /// Opens (or creates) a RocksDBStore at the specified path.
    pub fn open(path: &str, peer_id: PeerId) -> Result<Self, rocksdb::Error> {
        // Configure RocksDB to create the database if missing.
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);

        // Define two column families: one for records and one for provider records.
        let cf_descriptors = vec![
            ColumnFamilyDescriptor::new("records_cf", Options::default()),
            ColumnFamilyDescriptor::new("provider_cf", Options::default()),
        ];

        // Open the database with the column families.
        let db = Arc::new(DB::open_cf_descriptors(&opts, path, cf_descriptors)?);

        // Retrieve handles to each column family.
        let records_cf = db.cf_handle("records_cf").unwrap();
        let provider_cf = db.cf_handle("provider_cf").unwrap();

        let cache = MemoryStore::new(peer_id);

        // Create a channel to queue flush commands.
        let (tx, mut rx) = mpsc::channel::<FlushCommand>(100);
        let db_clone = db.clone();
        let records_cf_clone = records_cf.clone();
        let provider_cf_clone = provider_cf.clone();

        // Spawn the background flush task.
        tokio::spawn(async move {
            while let Some(command) = rx.recv().await {
                match command {
                    FlushCommand::FlushRecord(record) => {
                        if let Err(e) = db_clone.put_cf(
                            &records_cf_clone,
                            record.key.to_vec(),
                            record.value.clone(),
                        ) {
                            error!(
                                "Failed to flush record {:?} to RocksDB: {:?}",
                                record.key, e
                            );
                        } else {
                            info!("Flushed record {:?} to RocksDB", record.key);
                        }
                    }
                    FlushCommand::RemoveRecord(key) => {
                        if let Err(e) = db_clone.delete_cf(&records_cf_clone, key.to_vec()) {
                            error!("Failed to remove record {:?} from RocksDB: {:?}", key, e);
                        } else {
                            info!("Removed record {:?} from RocksDB", key);
                        }
                    }
                    FlushCommand::FlushProviderRecord(provider_record) => {
                        // Serialize the provider record (using bincode, for example).
                        let serialized =
                            bincode::serialize(&provider_record).unwrap_or_else(|_| Vec::new());
                        if let Err(e) = db_clone.put_cf(
                            &provider_cf_clone,
                            provider_record.key.to_vec(),
                            serialized,
                        ) {
                            error!(
                                "Failed to flush provider record {:?} to RocksDB: {:?}",
                                provider_record.key, e
                            );
                        } else {
                            info!(
                                "Flushed provider record {:?} to RocksDB",
                                provider_record.key
                            );
                        }
                    }
                    FlushCommand::RemoveProviderRecord(key, _peer) => {
                        if let Err(e) = db_clone.delete_cf(&provider_cf_clone, key.to_vec()) {
                            error!(
                                "Failed to remove provider record {:?} from RocksDB: {:?}",
                                key, e
                            );
                        } else {
                            info!("Removed provider record {:?} from RocksDB", key);
                        }
                    }
                }
            }
        });

        Ok(Self {
            db,
            records_cf: records_cf.clone(),
            provider_cf: provider_cf.clone(),
            local_peer_id: peer_id,
            cache,
            flush_sender: tx,
        })
    }

    /// Schedule an asynchronous flush of a record to disk.
    async fn flush_record(&self, record: Record) {
        if let Err(e) = self
            .flush_sender
            .send(FlushCommand::FlushRecord(record))
            .await
        {
            error!("Failed to send flush command: {:?}", e);
        }
    }

    /// Schedule an asynchronous removal of a record from disk.
    async fn remove_record(&self, key: RecordKey) {
        if let Err(e) = self
            .flush_sender
            .send(FlushCommand::RemoveRecord(key))
            .await
        {
            error!("Failed to send remove command: {:?}", e);
        }
    }
}

/// Production-level implementation of the RecordStore trait.
/// All operations update the in-memory cache immediately, while disk persistence is handled asynchronously.
impl RecordStore for RocksDBStore {
    fn put(&mut self, record: Record) -> Result<(), libp2p::kad::store::Error> {
        // Insert/update the record in the in-memory cache.
        self.cache.put(record.clone()).map_err(|e| {
            libp2p::kad::store::Error::new(format!("MemoryStore put error: {:?}", e))
        })?;
        // Schedule an asynchronous flush to disk.
        let flush_future = self.flush_record(record);
        tokio::spawn(flush_future);
        Ok(())
    }

    fn get(&self, key: &RecordKey) -> Option<Record> {
        // Try to get from the in-memory cache first.
        if let Some(record) = self.cache.get(key) {
            return Some(record);
        }
        // Fall back to RocksDB.
        if let Ok(Some(value)) = self.db.get_cf(&self.records_cf, key.to_vec()) {
            Some(Record {
                key: key.clone(),
                value,
                publisher: None,
                expires: None,
            })
        } else {
            None
        }
    }

    fn remove(&mut self, key: &RecordKey) -> Option<Record> {
        // Remove from in-memory cache.
        let record = self.cache.remove(key);
        let key_clone = key.clone();
        tokio::spawn(self.remove_record(key_clone));
        // Optionally, attempt synchronous removal.
        if let Err(e) = self.db.delete_cf(&self.records_cf, key.to_vec()) {
            error!(
                "Synchronous removal from RocksDB failed for key {:?}: {:?}",
                key, e
            );
        }
        record
    }

    fn records(&self) -> Vec<Record> {
        self.cache.records()
    }

    // Define associated iterator types for records and provider records.
    type RecordsIter<'a>
        = std::vec::IntoIter<Record>
    where
        Self: 'a;
    type ProvidedIter<'a>
        = std::vec::IntoIter<ProviderRecord>
    where
        Self: 'a;

    fn add_provider(&mut self, record: ProviderRecord) -> kad::store::Result<()> {
        self.cache.add_provider(record.clone())?;
        let flush_sender = self.flush_sender.clone();
        tokio::spawn(async move {
            if let Err(e) = flush_sender
                .send(FlushCommand::FlushProviderRecord(record))
                .await
            {
                error!("Failed to send flush provider command: {:?}", e);
            }
        });
        Ok(())
    }

    fn providers(&self, key: &RecordKey) -> Vec<ProviderRecord> {
        self.cache.providers(key)
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        // Collect the provider records from the in-memory cache.
        self.cache.provided().collect::<Vec<_>>().into_iter()
    }

    fn remove_provider(&mut self, key: &RecordKey, peer: &PeerId) {
        self.cache.remove_provider(key, peer);
        let key_clone = key.clone();
        let peer_clone = peer.clone();
        let flush_sender = self.flush_sender.clone();
        tokio::spawn(async move {
            if let Err(e) = flush_sender
                .send(FlushCommand::RemoveProviderRecord(key_clone, peer_clone))
                .await
            {
                error!("Failed to send remove provider command: {:?}", e);
            }
        });
    }
}
