use libp2p::kad::{ProviderRecord, Record, RecordKey};
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorbRecord {
    pub key: RecordKey,
    pub value: Vec<u8>,
    pub publisher: Option<PeerId>,
    #[serde(skip)]
    pub expires: Option<u64>,
}

impl From<Record> for StorbRecord {
    fn from(r: Record) -> Self {
        StorbRecord {
            key: r.key,
            value: r.value,
            publisher: r.publisher,
            expires: None,
        }
    }
}

impl Into<Record> for StorbRecord {
    fn into(self) -> Record {
        Record {
            key: self.key,
            value: self.value,
            publisher: self.publisher,
            expires: None,
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorbProviderRecord {
    pub key: RecordKey,
    pub provider: PeerId,
    #[serde(skip)]
    pub expires: Option<u64>,
    pub addresses: Vec<Multiaddr>,
}

impl From<ProviderRecord> for StorbProviderRecord {
    fn from(p: ProviderRecord) -> Self {
        StorbProviderRecord {
            key: p.key,
            provider: p.provider,
            expires: None,
            addresses: p.addresses,
        }
    }
}

impl Into<ProviderRecord> for StorbProviderRecord {
    fn into(self) -> ProviderRecord {
        ProviderRecord {
            key: self.key.into(),
            provider: self.provider,
            expires: None,
            addresses: self.addresses,
        }
    }
}
