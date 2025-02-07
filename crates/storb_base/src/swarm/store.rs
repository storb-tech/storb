use super::db::{CfType, RocksDBStore};
use super::record::{StorbProviderRecord, StorbRecord};
use libp2p::kad::store::{RecordStore, Result as StoreRes};
use libp2p::kad::{ProviderRecord, Record, RecordKey};
use libp2p::PeerId;
use lru::LruCache;
use std::borrow::Cow;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use tokio::runtime::Handle;

pub struct StorbStore {
    rec: Mutex<LruCache<RecordKey, Record>>,
    prv: Mutex<LruCache<RecordKey, Vec<ProviderRecord>>>,
    db: Arc<RocksDBStore>,
}

impl StorbStore {
    /// Create a new store with cache capacities.
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

    fn get(&self, k: &RecordKey) -> Option<Cow<'_, Record>> {
        // First, check the in-memory cache.
        if let Some(rec) = self.rec.lock().unwrap_or_else(|e| e.into_inner()).get(k) {
            return Some(Cow::Owned(rec.clone()));
        }

        let kb = k.as_ref();

        // Use block_in_place to safely perform a blocking wait.
        let db_result = tokio::task::block_in_place(|| {
            // Create a small single-threaded runtime to await our async call.
            // (Note: Creating a runtime per call can be expensive. If this path is hit frequently,
            // consider caching the runtime or, preferably, converting the function to async.)
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

    fn put(&mut self, r: Record) -> StoreRes<()> {
        // Cache the record in-memory.
        self.rec
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .put(r.key.clone(), r.clone());
        let kb = r.key.as_ref();
        // Wrap the record for serialization.
        let wrapper = StorbRecord::from(r.clone());
        // MUST CATCH THIS PANIC IN THE CONTEXT OF THE CALLER
        let vb = bincode::serialize(&wrapper).expect("serialization failed");
        tokio::task::block_in_place(|| {
            tokio::runtime::Handle::current().block_on(self.db.schedule_put(kb, &vb))
        });
        Ok(())
    }

    fn remove(&mut self, k: &RecordKey) {
        self.rec.lock().unwrap_or_else(|e| e.into_inner()).pop(k);
        Handle::current().block_on(self.db.schedule_delete(k.as_ref()));
    }

    fn records(&self) -> Self::RecordsIter<'_> {
        let mut out = Vec::new();
        for (_, r) in self.rec.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            out.push(Cow::Owned(r.clone()));
        }
        out.into_iter()
    }

    fn add_provider(&mut self, p: ProviderRecord) -> StoreRes<()> {
        let k = p.key.clone();
        let mut lock = self.prv.lock().unwrap_or_else(|e| e.into_inner());
        let mut vec = lock.get(&k).cloned().unwrap_or_default();
        vec.push(p.clone());
        lock.put(k.clone(), vec);
        let kb = k.as_ref();
        // Wrap the provider record for serialization. MUST CATCH THIS PANIC IN THE CONTEXT OF THE CALLER
        let wrapper = StorbProviderRecord::from(p.clone());
        let pb = bincode::serialize(&wrapper).expect("serialization failed");
        Handle::current().block_on(self.db.schedule_put_provider(kb, &pb));
        Ok(())
    }

    fn providers(&self, k: &RecordKey) -> Vec<ProviderRecord> {
        self.prv
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(k)
            .cloned()
            .unwrap_or_default()
    }

    fn provided(&self) -> Self::ProvidedIter<'_> {
        let mut out = Vec::new();
        for (_, vec) in self.prv.lock().unwrap_or_else(|e| e.into_inner()).iter() {
            for p in vec {
                out.push(Cow::Owned(p.clone()));
            }
        }
        out.into_iter()
    }

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
