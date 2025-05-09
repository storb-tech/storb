use libp2p::kad::{ProviderRecord, Record, RecordKey};
use libp2p::{Multiaddr, PeerId};
use serde::{Deserialize, Serialize};

/// A record used by Storb, wrapping a libp2p `Record`.
///
/// This struct adds serialization support via Serde and omits the `expires` field
/// during serialization, as Instant type is not serializable.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorbRecord {
    /// The key associated with the record.
    pub key: RecordKey,
    /// The value stored in the record.
    pub value: Vec<u8>,
    /// Optional publisher (peer) of the record.
    pub publisher: Option<PeerId>,
    /// The expiration time of the record, skipped during serialization.
    #[serde(skip)]
    pub expires: Option<u64>,
}

impl From<Record> for StorbRecord {
    /// Converts a libp2p `Record` into a `StorbRecord`.
    ///
    /// The conversion sets `expires` to `None`.
    fn from(r: Record) -> Self {
        StorbRecord {
            key: r.key,
            value: r.value,
            publisher: r.publisher,
            expires: None,
        }
    }
}

impl From<StorbRecord> for Record {
    /// Converts a `StorbRecord` into a libp2p `Record`.
    ///
    /// The resulting record will have `expires` set to `None`.
    fn from(record: StorbRecord) -> Self {
        Record {
            key: record.key,
            value: record.value,
            publisher: record.publisher,
            expires: None,
        }
    }
}

/// A provider record used by Storb, wrapping a libp2p `ProviderRecord`.
///
/// This struct supports serialization via Serde while skipping the `expires` field.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct StorbProviderRecord {
    /// The key associated with the provider record.
    pub key: RecordKey,
    /// The peer that provides the record.
    pub provider: PeerId,
    /// A list of network addresses associated with the provider.
    pub addresses: Vec<Multiaddr>,
    /// The expiration time of the record, skipped during serialization.
    #[serde(skip)]
    pub expires: Option<u64>,
}

impl From<ProviderRecord> for StorbProviderRecord {
    /// Converts a libp2p `ProviderRecord` into a `StorbProviderRecord`.
    ///
    /// The conversion sets `expires` to `None`.
    fn from(p: ProviderRecord) -> Self {
        StorbProviderRecord {
            key: p.key,
            provider: p.provider,
            addresses: p.addresses,
            expires: None,
        }
    }
}

impl From<StorbProviderRecord> for ProviderRecord {
    /// Converts a `StorbProviderRecord` into a libp2p `ProviderRecord`.
    ///
    /// The resulting provider record will have `expires` set to `None`.
    fn from(record: StorbProviderRecord) -> Self {
        ProviderRecord {
            key: record.key,
            provider: record.provider,
            addresses: record.addresses,
            expires: None,
        }
    }
}
