use chrono::{DateTime, Utc};

use crate::hashes::{AccountId, ChunkHash, InfoHash};

/// Object metadata.
///
/// NOTE: Objects may occasionally be referred to as "files".
pub struct Object {
    pub spec_version: u16,

    /// Info hash of the object, which acts as a unique identifier.
    pub info_hash: InfoHash,

    /// The name of the object.
    pub name: String,

    /// The length of the object in bytes.
    pub length: u64,

    /// The size of each chunk in bytes.
    pub chunk_size: u64,

    /// The number of chunks in the object.
    pub chunk_count: u64,

    /// The owner account ID of the object.
    pub owner_account_id: AccountId,

    /// The signature of the object entry, used for verification.
    pub signature: String,

    /// The timestamp when the object was created.
    pub created_at: DateTime<Utc>,
}

/// Mapping of chunks to their objects.
pub struct ObjectChunk {
    pub spec_version: u16,

    /// The info hash of the object this chunk belongs to.
    pub info_hash: InfoHash,

    /// The chunk index within the object.
    pub chunk_idx: u64,

    /// The hash of the chunk, which acts as a unique identifier for the chunk.
    pub chunk_hash: ChunkHash,
}
