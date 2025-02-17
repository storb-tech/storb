use hmac::{Hmac, Mac};
use num_bigint::BigUint;
use rand;
use rand::rngs::OsRng;
use rand::RngCore;
use rsa::RsaPrivateKey;
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::debug;

const DEFAULT_RSA_KEY_SIZE: usize = 2048;
const G_CANDIDATE_RETRY: u32 = 1000; // Number of times to retry g (RSA Generator) candidate generation
const S_CANDIDATE_RETRY: u32 = 1000; // Number of times to retry s (RSA Secret) candidate generation

#[derive(Debug, Clone, Serialize, Deserialize)]
struct APDPTag {
    index: u64,
    tag_value: BigUint,
    prf_value: [u8; 16],
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Challenge {
    tag: APDPTag,
    prp_key: [u8; 32],
    prf_key: [u8; 32],
    s: BigUint,
    g_s: BigUint,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
struct Proof {
    tag_value: BigUint,
    block_value: BigUint,
    hashed_result: String,
}

struct APDPKey {
    rsa: Option<RsaPrivateKey>,
    g: Option<BigUint>,
    prf_key: [u8; 32],
}

struct ChallengeSystem {
    key: APDPKey,
}

#[derive(Error, Debug)]
pub enum APDPError {
    #[error("Invalid parameters")]
    InvalidParameters,
    #[error("Failed to generate RSA key")]
    RsaKeyGeneration,
    #[error("Failed to find suitable generator g")]
    GeneratorFinding,
    #[error("Failed to generate PRF key")]
    PrfKeyGeneration,
}

pub struct CryptoUtils;

impl CryptoUtils {
    pub fn generate_rsa_private_key(key_size: usize) -> Result<RsaPrivateKey, APDPError> {
        RsaPrivateKey::new(&mut rand::thread_rng(), key_size)
            .map_err(|_| APDPError::RsaKeyGeneration)
    }

    pub fn full_domain_hash(rsa_key: &RsaPrivateKey, data: &[u8]) -> Result<BigUint, APDPError> {
        if data.is_empty() {
            return Err(APDPError::InvalidParameters);
        }

        let mut hasher = Sha256::new();
        hasher.update(data);
        let hashed = hasher.finalize();

        use rsa::traits::PublicKeyParts;
        let n = rsa_key.n();
        let n_biguint = BigUint::from_bytes_be(&n.to_bytes_be());
        let hash_biguint = BigUint::from_bytes_be(&hashed);
        Ok(hash_biguint % n_biguint)
    }

    pub fn prf(key: &[u8; 32], input_int: u64) -> Result<[u8; 16], APDPError> {
        if key.is_empty() {
            return Err(APDPError::InvalidParameters);
        }

        let mut mac =
            Hmac::<Sha256>::new_from_slice(key).map_err(|_| APDPError::InvalidParameters)?;

        mac.update(&input_int.to_be_bytes());
        let result = mac.finalize().into_bytes();
        let mut output = [0u8; 16];
        output.copy_from_slice(&result[..16]);
        Ok(output)
    }
}

impl APDPKey {
    pub fn generate(rsa_bits: Option<usize>) -> Result<Self, APDPError> {
        let key_size = match rsa_bits {
            Some(0) => return Err(APDPError::InvalidParameters),
            Some(bits) => bits,
            None => DEFAULT_RSA_KEY_SIZE,
        };

        let mut os_rng = OsRng;
        let rsa =
            RsaPrivateKey::new(&mut os_rng, key_size).map_err(|_| APDPError::RsaKeyGeneration)?;

        use rsa::traits::PublicKeyParts;
        let n = rsa.n();
        let n_biguint = BigUint::from_bytes_be(&n.to_bytes_be());

        // Generate g by finding first valid candidate that meets requirements
        let g = (0..G_CANDIDATE_RETRY)
            .find_map(|_| {
                let candidate = BigUint::from(rand::thread_rng().next_u64());
                let temp_val = candidate.modpow(&BigUint::from(2u32), &n_biguint);

                if temp_val != BigUint::from(0u32) && temp_val != BigUint::from(1u32) {
                    Some(temp_val)
                } else {
                    None
                }
            })
            .ok_or(APDPError::GeneratorFinding)?;

        let mut prf_key = [0u8; 32];
        rand::thread_rng()
            .try_fill_bytes(&mut prf_key)
            .map_err(|_| APDPError::PrfKeyGeneration)?;

        Ok(APDPKey {
            rsa: Some(rsa),
            g: Some(g),
            prf_key,
        })
    }
}

impl ChallengeSystem {
    pub fn new() -> Result<Self, APDPError> {
        Ok(ChallengeSystem {
            key: APDPKey::generate(None)?,
        })
    }

    pub fn generate_tag(&self, data: &[u8]) -> Result<APDPTag, APDPError> {
        let rsa = self.key.rsa.as_ref().ok_or(APDPError::InvalidParameters)?;
        use rsa::traits::PrivateKeyParts;
        use rsa::traits::PublicKeyParts;

        // RSA parameters
        let n = rsa.n().to_bytes_be(); // Modulus: product of prime numbers `p` and `q`

        let block_int =
            BigUint::from_bytes_be(data).modpow(&BigUint::from(1u32), &BigUint::from_bytes_be(&n));

        let prf_value = CryptoUtils::prf(&self.key.prf_key, 0)?;

        let fdh_hash = CryptoUtils::full_domain_hash(rsa, &prf_value)?;

        debug!("FDH hash: {}, PRF value: {:?}", fdh_hash, prf_value);
        let g = self.key.g.as_ref().ok_or(APDPError::InvalidParameters)?;
        let n_big = BigUint::from_bytes_be(&n);

        let g_pow = g.modpow(&block_int, &n_big);
        let base = (&fdh_hash * &g_pow) % &n_big;

        let d = rsa.d().to_bytes_be();
        let d_big = BigUint::from_bytes_be(&d);

        let tag_value = base.modpow(&d_big, &n_big);

        Ok(APDPTag {
            index: 0,
            tag_value,
            prf_value,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_challenge_system_new() {
        let cs = ChallengeSystem::new().unwrap();
        assert!(cs.key.rsa.is_some());
        assert!(cs.key.g.is_some());
        assert!(!cs.key.prf_key.is_empty());
    }

    #[test]
    fn test_generate_tag() {
        let cs = ChallengeSystem::new().unwrap();
        let data = b"test data";
        let tag = cs.generate_tag(data).unwrap();
        assert_eq!(tag.index, 0);
        assert!(tag.tag_value > BigUint::from(0u32));
        assert!(!tag.prf_value.is_empty());
    }

    #[test]
    fn test_prf() {
        let key = [0u8; 32];
        let result = CryptoUtils::prf(&key, 123).unwrap();
        assert_eq!(result.len(), 16);
    }

    #[test]
    fn test_full_domain_hash() {
        let rsa = CryptoUtils::generate_rsa_private_key(1024).unwrap();
        let data = b"test data";
        let hash = CryptoUtils::full_domain_hash(&rsa, data).unwrap();
        assert!(hash > BigUint::from(0u32));
    }

    #[test]
    fn test_prf_empty_key() {
        let key = [0u8; 32];
        let result = CryptoUtils::prf(&key, 0);
        assert!(result.is_ok());
    }

    #[test]
    fn test_full_domain_hash_empty_data() {
        let rsa = CryptoUtils::generate_rsa_private_key(1024).unwrap();
        let result = CryptoUtils::full_domain_hash(&rsa, &[]);
        assert!(result.is_err());
    }
}
