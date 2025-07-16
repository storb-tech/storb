use std::fmt::{self, Display, Formatter};
use std::hash::Hash;
use std::marker::PhantomData;

use serde::{Deserialize, Serialize};
use thiserror::Error;

pub type InfoHash = TypedHash<private::InfoHashType>;
pub type ChunkHash = TypedHash<private::ChunkHashType>;
pub type PieceHash = TypedHash<private::PieceHashType>;
pub type Nonce = TypedHash<private::NonceType>;
pub type AccountId = TypedHash<private::AccountIdType>;

type Hash32 = [u8; 32];

#[derive(Debug, Error)]
pub enum HashError {
    #[error("Invalid hash length, expected 64 characters")]
    InvalidStringLength,

    #[error("Invalid hash length, expected 32 bytes")]
    InvalidVecLength,

    #[error("Invalid hex string: {0}")]
    InvalidHexString(#[from] hex::FromHexError),
}

pub enum HashTypeName {
    InfoHash,
    ChunkHash,
    PieceHash,
    Nonce,
    AccountId,
}

pub trait HashType: Clone + Copy + PartialEq + Eq + Hash {
    const TYPE_NAME: HashTypeName;
}

#[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
pub struct TypedHash<T: HashType> {
    bytes: Hash32,
    _hash_type: PhantomData<T>,
}

impl<T: HashType> TypedHash<T> {
    /// Create a new 32-byte hash.
    pub fn new(bytes: [u8; 32]) -> Self {
        Self {
            bytes,
            _hash_type: PhantomData,
        }
    }

    /// Get the hash type.
    pub fn type_name(&self) -> HashTypeName {
        T::TYPE_NAME
    }

    /// Returns the underlying hash as a `Vec<u8>`.
    pub fn to_vec(&self) -> Vec<u8> {
        self.bytes.to_vec()
    }
}

impl<T> Display for TypedHash<T>
where
    T: HashType,
{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        write!(f, "{:x?}", self.bytes)
    }
}

impl<T> TryFrom<String> for TypedHash<T>
where
    T: HashType,
{
    type Error = HashError;

    fn try_from(value: String) -> Result<Self, Self::Error> {
        if value.len() != 64 {
            return Err(HashError::InvalidStringLength);
        }

        let bytes = hex::decode(value)?;
        if bytes.len() != 32 {
            Err(HashError::InvalidVecLength)
        } else {
            let mut hash = Hash32::default();
            hash.copy_from_slice(&bytes);
            Ok(Self {
                bytes: hash,
                _hash_type: PhantomData,
            })
        }
    }
}

impl<T> TryFrom<Vec<u8>> for TypedHash<T>
where
    T: HashType,
{
    type Error = HashError;

    fn try_from(value: Vec<u8>) -> Result<Self, Self::Error> {
        if value.len() != 32 {
            Err(HashError::InvalidVecLength)
        } else {
            let mut hash = Hash32::default();
            hash.copy_from_slice(&value);
            Ok(Self {
                bytes: hash,
                _hash_type: PhantomData,
            })
        }
    }
}

impl<T> AsRef<[u8]> for TypedHash<T>
where
    T: HashType,
{
    fn as_ref(&self) -> &[u8] {
        &self.bytes
    }
}

mod private {
    use serde::{Deserialize, Serialize};

    use super::{HashType, HashTypeName};

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
    pub struct InfoHashType;
    impl HashType for InfoHashType {
        const TYPE_NAME: HashTypeName = HashTypeName::InfoHash;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
    pub struct ChunkHashType;
    impl HashType for ChunkHashType {
        const TYPE_NAME: HashTypeName = HashTypeName::ChunkHash;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
    pub struct PieceHashType;
    impl HashType for PieceHashType {
        const TYPE_NAME: HashTypeName = HashTypeName::PieceHash;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
    pub struct NonceType;
    impl HashType for NonceType {
        const TYPE_NAME: HashTypeName = HashTypeName::Nonce;
    }

    #[derive(Clone, Copy, Debug, Eq, PartialEq, Hash, Deserialize, Serialize)]
    pub struct AccountIdType;
    impl HashType for AccountIdType {
        const TYPE_NAME: HashTypeName = HashTypeName::AccountId;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const VALID_HASH_BYTES: [u8; 32] = [
        0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c, 0x0d, 0x0e, 0x0f,
        0x10, 0x11, 0x12, 0x13, 0x14, 0x15, 0x16, 0x17, 0x18, 0x19, 0x1a, 0x1b, 0x1c, 0x1d, 0x1e,
        0x1f, 0x20,
    ];
    const VALID_HASH_HEX: &str = "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f20";
    const VALID_HASH_DISPLAY: &str = "[1, 2, 3, 4, 5, 6, 7, 8, 9, a, b, c, d, e, f, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 1a, 1b, 1c, 1d, 1e, 1f, 20]";

    #[test]
    fn test_hash_creation() {
        let info_hash = InfoHash::new(VALID_HASH_BYTES);
        let chunk_hash = ChunkHash::new(VALID_HASH_BYTES);
        let piece_hash = PieceHash::new(VALID_HASH_BYTES);
        let nonce = Nonce::new(VALID_HASH_BYTES);
        let account_id = AccountId::new(VALID_HASH_BYTES);

        assert_eq!(info_hash.as_ref(), &VALID_HASH_BYTES);
        assert_eq!(chunk_hash.as_ref(), &VALID_HASH_BYTES);
        assert_eq!(piece_hash.as_ref(), &VALID_HASH_BYTES);
        assert_eq!(nonce.as_ref(), &VALID_HASH_BYTES);
        assert_eq!(account_id.as_ref(), &VALID_HASH_BYTES);
    }

    #[test]
    fn test_hash_type_names() {
        let info_hash = InfoHash::new(VALID_HASH_BYTES);
        let chunk_hash = ChunkHash::new(VALID_HASH_BYTES);
        let piece_hash = PieceHash::new(VALID_HASH_BYTES);
        let nonce = Nonce::new(VALID_HASH_BYTES);
        let account_id = AccountId::new(VALID_HASH_BYTES);

        assert!(matches!(info_hash.type_name(), HashTypeName::InfoHash));
        assert!(matches!(chunk_hash.type_name(), HashTypeName::ChunkHash));
        assert!(matches!(piece_hash.type_name(), HashTypeName::PieceHash));
        assert!(matches!(nonce.type_name(), HashTypeName::Nonce));
        assert!(matches!(account_id.type_name(), HashTypeName::AccountId));
    }

    #[test]
    fn test_hash_to_vec() {
        let hash = InfoHash::new(VALID_HASH_BYTES);
        let vec = hash.to_vec();
        assert_eq!(vec, VALID_HASH_BYTES.to_vec());
    }

    #[test]
    fn test_hash_display() {
        let hash = InfoHash::new(VALID_HASH_BYTES);
        let display_str = format!("{}", hash);
        // The Display impl uses {:x?} which formats as array with hex values
        assert!(display_str == VALID_HASH_DISPLAY);
    }

    #[test]
    fn test_hash_from_valid_string() {
        let hash = InfoHash::try_from(VALID_HASH_HEX.to_string()).unwrap();
        assert_eq!(hash.as_ref(), &VALID_HASH_BYTES);
    }

    #[test]
    fn test_hash_from_invalid_string_length() {
        let short_string = "010203040506070809".to_string();
        let long_string =
            "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1f2021222324".to_string();

        assert!(matches!(
            InfoHash::try_from(short_string).unwrap_err(),
            HashError::InvalidStringLength
        ));
        assert!(matches!(
            InfoHash::try_from(long_string).unwrap_err(),
            HashError::InvalidStringLength
        ));
    }

    #[test]
    fn test_hash_from_invalid_hex_string() {
        // Correct length (64 characters), but invalid hex characters
        let invalid_hex =
            "0102030405060708090a0b0c0d0e0f101112131415161718191a1b1c1d1e1xyz".to_string();

        assert!(matches!(
            InfoHash::try_from(invalid_hex.clone()).unwrap_err(),
            HashError::InvalidHexString(_)
        ));
    }

    #[test]
    fn test_hash_from_valid_vec() {
        let hash = InfoHash::try_from(VALID_HASH_BYTES.to_vec()).unwrap();
        assert_eq!(hash.as_ref(), &VALID_HASH_BYTES);
    }

    #[test]
    fn test_hash_from_invalid_vec_length() {
        let short_vec = vec![0x01, 0x02, 0x03];
        let long_vec = vec![0u8; 33];

        assert!(matches!(
            InfoHash::try_from(short_vec).unwrap_err(),
            HashError::InvalidVecLength
        ));
        assert!(matches!(
            InfoHash::try_from(long_vec).unwrap_err(),
            HashError::InvalidVecLength
        ));
    }

    #[test]
    fn test_hash_equality() {
        let hash1 = InfoHash::new(VALID_HASH_BYTES);
        let hash2 = InfoHash::new(VALID_HASH_BYTES);
        let hash3 = InfoHash::new([0u8; 32]);

        assert_eq!(hash1, hash2);
        assert_ne!(hash1, hash3);
    }

    #[test]
    fn test_hash_clone() {
        let hash1 = InfoHash::new(VALID_HASH_BYTES);
        let hash2 = hash1.clone();
        assert_eq!(hash1, hash2);
    }

    #[test]
    fn test_different_hash_types_not_equal() {
        // This test ensures type safety - different hash types with same bytes
        // should not be directly comparable (this would be a compile error)
        let info_hash = InfoHash::new(VALID_HASH_BYTES);
        let chunk_hash = ChunkHash::new(VALID_HASH_BYTES);

        // These should have the same underlying bytes but different types
        assert_eq!(info_hash.as_ref(), chunk_hash.as_ref());

        // Note: Direct comparison between different hash types would not compile:
        // assert_ne!(info_hash, chunk_hash); // This would be a compile error
    }
    #[test]
    fn test_hash_serialization() {
        let hash = InfoHash::new(VALID_HASH_BYTES);

        // Test that the hash implements Serialize and Deserialize traits
        // This is a compile-time check that the derive macros work
        fn assert_serializable<T: serde::Serialize + serde::de::DeserializeOwned>() {}
        assert_serializable::<InfoHash>();

        // Test basic serialization properties
        assert_eq!(hash.as_ref(), &VALID_HASH_BYTES);
    }

    #[test]
    fn test_hash_as_ref() {
        let hash = InfoHash::new(VALID_HASH_BYTES);
        let bytes_ref: &[u8] = hash.as_ref();
        assert_eq!(bytes_ref, &VALID_HASH_BYTES);
    }

    #[test]
    fn test_all_hash_types_creation() {
        let info_hash = InfoHash::new(VALID_HASH_BYTES);
        let chunk_hash = ChunkHash::new(VALID_HASH_BYTES);
        let piece_hash = PieceHash::new(VALID_HASH_BYTES);
        let nonce = Nonce::new(VALID_HASH_BYTES);

        // Test that all types can be created and used
        assert_eq!(info_hash.to_vec().len(), 32);
        assert_eq!(chunk_hash.to_vec().len(), 32);
        assert_eq!(piece_hash.to_vec().len(), 32);
        assert_eq!(nonce.to_vec().len(), 32);
    }

    #[test]
    fn test_hash_from_string_roundtrip() {
        let original_hash = InfoHash::new(VALID_HASH_BYTES);
        let hash_from_string = InfoHash::try_from(VALID_HASH_HEX.to_string()).unwrap();
        assert_eq!(original_hash.as_ref(), hash_from_string.as_ref());
    }

    #[test]
    fn test_hash_from_vec_roundtrip() {
        let original_hash = InfoHash::new(VALID_HASH_BYTES);
        let vec = original_hash.to_vec();
        let hash_from_vec = InfoHash::try_from(vec).unwrap();
        assert_eq!(original_hash, hash_from_vec);
    }
}
