use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};

use libp2p::kad::store::{RecordStore, Result as StoreRes};
use libp2p::kad::{ProviderRecord, Record, RecordKey};
use libp2p::PeerId;
use lru::LruCache;
use tokio::runtime::Handle;
use tracing::info;

use super::db::{CfType, RocksDBStore};
use super::record::{StorbProviderRecord, StorbRecord};

/// A store that implements libp2p's RecordStore trait using RocksDB as
/// persistent storage and LRU caches for in-memory caching of records and
/// provider records.
///
/// This store enables asynchronous operations via tokio and supports both
/// record and provider record management.
pub struct StorbStore {
    rec: Mutex<LruCache<RecordKey, Record>>,
    prv: Mutex<LruCache<RecordKey, Vec<ProviderRecord>>>,
    db: Arc<RocksDBStore>,
}

impl StorbStore {
    /// Creates a new `StorbStore` with specified cache capacities.
    ///
    /// The store maintains separate caches for records and provider records,
    /// and uses the provided RocksDBStore for persistent storage.
    pub fn new(db: Arc<RocksDBStore>, cap_r: NonZeroUsize, cap_p: NonZeroUsize) -> Self {
        Self {
            rec: Mutex::new(LruCache::new(cap_r)),
            prv: Mutex::new(LruCache::new(cap_p)),
            db,
        }
    }
}

impl RecordStore for StorbStore {
    type RecordsIter<'a>
        = std::vec::IntoIter<Cow<'a, Record>>
    where
        Self: 'a;

    type ProvidedIter<'a>
        = std::vec::IntoIter<Cow<'a, ProviderRecord>>
    where
        Self: 'a;

    /// Retrieves a record by its key.
    ///
    /// First checks the in-memory cache. If not found, performs a blocking call to retrieve
    /// the record from the RocksDB store. If found in the DB, the record is deserialized,
    /// cached, and returned.
    fn get(&self, k: &RecordKey) -> Option<Cow<'_, Record>> {
        // Check in-memory cache first.
        if let Some(rec) = self.rec.lock().unwrap_or_else(|e| e.into_inner()).get(k) {
            return Some(Cow::Owned(rec.clone()));
        }

        let kb = k.as_ref();

        // Perform a blocking call to retrieve the record from RocksDB.
        let db_result = tokio::task::block_in_place(|| {
            // Create a temporary runtime to await the async get operation.
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("Failed to build runtime");
            rt.block_on(self.db.get(CfType::Default, kb))
        });

        if let Ok(Some(bytes)) = db_result {
            if let Ok(wrapper) = bincode::deserialize::<StorbRecord>(&bytes) {
                let rec: Record = wrapper.into();
                // Cache the deserialized record.
                self.rec
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .put(k.clone(), rec.clone());
                return Some(Cow::Owned(rec));
            }
        }
        None
    }

    /// Inserts or updates a record in the store.
    ///
    /// The record is first cached in-memory, then wrapped for serialization and stored
    /// persistently in the RocksDB store using an asynchronous put operation.
    ///
    /// # Panics
    ///
    /// This function may panic if serialization fails. The caller should catch such panics.
    fn put(&mut self, r: Record) -> StoreRes<()> {
        // Cache the record in-memory.
        self.rec
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(r.key.clone(), r.clone());
        let kb = r.key.as_ref().to_vec();
        // Wrap the record for serialization.
        let wrapper = StorbRecord::from(r.clone());
        // Serialize the record; panics if serialization fails.
        let vb = bincode::serialize(&wrapper).expect("serialization failed");

        // Schedule the put operation on the RocksDB store.
        // TODO: is there a better way to do this instead of having to clone the db?
        let db = self.db.clone();
        tokio::spawn(async move {
            db.schedule_put(&kb, &vb).await;
        });

        Ok(())
    }

    /// Removes a record from the store by its key.
    ///
    /// The record is removed from the in-memory cache and a delete operation is scheduled
    /// on the RocksDB store.
    fn remove(&mut self, k: &RecordKey) {
        self.rec.lock().unwrap_or_else(|e| e.into_inner()).pop(k);
        Handle::current().block_on(self.db.schedule_delete(k.as_ref()));
    }

    /// Returns an iterator over all records stored in the in-memory cache.
    ///
    /// This function iterates over the cached records and returns them as owned copies.
    fn records(&self) -> Self::RecordsIter<'_> {
        let mut out = Vec::new();
        for (_, r) in self.rec.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            out.push(Cow::Owned(r.clone()));
        }
        out.into_iter()
    }

    /// Adds a provider record for a given key.
    ///
    /// The provider record is added to the in-memory cache and then serialized and stored
    /// persistently in the RocksDB store using a provider-specific put operation.
    ///
    /// # Panics
    ///
    /// This function may panic if serialization fails. The caller should handle such cases.
    fn add_provider(&mut self, p: ProviderRecord) -> StoreRes<()> {
        let k = p.key.clone();
        let mut lock = self.prv.lock().unwrap_or_else(|e| e.into_inner());
        let mut vec = lock.get(&k).cloned().unwrap_or_default();
        vec.push(p.clone());
        lock.put(k.clone(), vec);
        let kb = p.key.as_ref().to_vec();

        // Wrap the provider record for serialization.
        let wrapper = StorbProviderRecord::from(p.clone());
        let pb = bincode::serialize(&wrapper).expect("serialization failed");
        let db = self.db.clone();
        info!("Adding provider record for key {:?}", k);
        tokio::spawn(async move {
            db.schedule_put_provider(&kb, &pb).await;
        });

        Ok(())
    }

    /// Retrieves provider records associated with a given key.
    ///
    /// If no provider records are found in the in-memory cache, returns an empty vector.
    fn providers(&self, k: &RecordKey) -> Vec<ProviderRecord> {
        // Check in-memory cache first
        if let Some(providers) = self
            .prv
            .lock()
            .unwrap_or_else(|e| {
                tracing::error!("Cache lock poisoned in providers(): {}", e);
                e.into_inner()
            })
            .get(k)
        {
            tracing::info!(
                "Cache hit for key {:?}: found {} providers",
                k,
                providers.len()
            );
            return providers.clone();
        }
        tracing::info!("Cache miss for key {:?}, checking database", k);

        let kb = k.as_ref();

        // Perform a blocking call to retrieve the provider record from RocksDB
        let db_result = tokio::task::block_in_place(|| {
            tracing::info!("Creating runtime for database access");
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .map_err(|e| {
                    tracing::error!("Failed to build runtime in providers(): {}", e);
                    e
                })
                .expect("Failed to build runtime");

            rt.block_on(async {
                    // Get the serialized provider record from the database
                    tracing::info!("Querying database for key {:?}", k);
                    match self.db.get(CfType::Provider, kb).await {
                        Ok(Some(bytes)) => {
                            tracing::info!("Found record in database for key {:?}", k);
                            match bincode::deserialize::<StorbProviderRecord>(&bytes) {
                                Ok(wrapper) => {
                                    let provider = ProviderRecord::from(wrapper);
                                    tracing::info!(
                                        "Successfully deserialized provider record for key {:?}, provider: {:?}",
                                        k,
                                        provider.provider
                                    );

                                    // Update the in-memory cache with a new vector containing the provider
                                    if let Err(e) = self.prv.lock().map(|mut lock| {
                                        let mut providers = lock.get(k).cloned().unwrap_or_default();
                                        providers.push(provider.clone());
                                        lock.put(k.clone(), providers);
                                        tracing::info!("Updated cache with provider for key {:?}", k);
                                    }) {
                                        tracing::error!("Failed to acquire cache lock for update in providers(): {}", e);
                                    }

                                    vec![provider]
                                }
                                Err(e) => {
                                    tracing::error!(
                                        "Failed to deserialize provider record in providers(): {} for key: {:?}",
                                        e,
                                        k
                                    );
                                    Vec::new()
                                }
                            }
                        }
                        Ok(None) => {
                            tracing::debug!("No provider record found in database for key: {:?}", k);
                            Vec::new()
                        }
                        Err(e) => {
                            tracing::error!(
                                "Database error in providers() while fetching key {:?}: {}",
                                k,
                                e
                            );
                            Vec::new()
                        }
                    }
                })
        });

        db_result
    }

    /// Returns an iterator over all provider records stored in the in-memory cache.
    ///
    /// Each provider record is returned as an owned copy.
    fn provided(&self) -> Self::ProvidedIter<'_> {
        let mut out = Vec::new();
        for (_, vec) in self.prv.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            for p in vec {
                out.push(Cow::Owned(p.clone()));
            }
        }
        out.into_iter()
    }

    /// Removes a provider record for a given key and peer ID.
    ///
    /// The provider record is removed from the in-memory cache. If no provider
    /// records remain, a delete operation is scheduled on the RocksDB store.
    /// Otherwise, the updated provider list is serialized and stored persistently.
    fn remove_provider(&mut self, k: &RecordKey, pid: &PeerId) {
        let mut lock = self.prv.lock().unwrap_or_else(|e| e.into_inner());
        if let Some(mut vec) = lock.pop(k) {
            vec.retain(|p| &p.provider != pid);
            if vec.is_empty() {
                Handle::current().block_on(self.db.schedule_delete_provider(k.as_ref()));
            } else {
                lock.put(k.clone(), vec.clone());
                let wrappers: Vec<StorbProviderRecord> =
                    vec.into_iter().map(StorbProviderRecord::from).collect();
                let pb =
                    bincode::serialize(&wrappers).expect("Failed to serialize provider records");
                Handle::current().block_on(self.db.schedule_put_provider(k.as_ref(), &pb));
            }
        }
    }
}
