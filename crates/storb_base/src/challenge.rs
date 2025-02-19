//! Proof of data possession challenge system based on the paper by Ateniese et al.
//! which can be found here: https://dl.acm.org/doi/10.1145/1315245.1315318

use hmac::{Hmac, Mac};
use num_bigint::BigUint;
use rand;
use rand::rngs::OsRng;
use rand::RngCore;
use rsa::{traits::PublicKeyParts, RsaPrivateKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use thiserror::Error;
use tracing::debug;

const DEFAULT_RSA_KEY_SIZE: usize = 4096;
const G_CANDIDATE_RETRY: u32 = 1000; // Number of times to retry g (RSA Generator) candidate generation
const S_CANDIDATE_RETRY: u32 = 1000; // Number of times to retry s (RSA Secret) candidate generation

/// 32 byte key
type Key32 = [u8; 32];

/// Represents an APDP tag which represents a stored piece to be
/// verified as being stored by a miner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct APDPTag {
    index: u64,
    tag_value: BigUint,
    prf_value: Key32,
}

/// Represents a challenge that is sent from a validator to a miner.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Challenge {
    /// Tag of that refers to the piece to verify as being stored
    tag: APDPTag,
    /// Pseudo-random permutation (PRP) key
    prp_key: Key32, // k_1
    /// Pseudo-random function (PRF) key
    prf_key: Key32, // k_2
}

/// Represents a challenge proof that is sent from a miner to a validator.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Proof {
    tag_value: BigUint, // T
    big_m: BigUint,     // M
                        // block_value: BigUint,  // rho_temp in zachary peterson's code - TODO: is this even used?
                        // hashed_result: String, // rho
}

/// Represents the PDP challenge key.
pub struct ChallengeKey {
    /// RSA private key
    rsa: Option<RsaPrivateKey>,
    /// Generator seed
    g: Option<BigUint>,
    /// Pseudo-random function (PRF) key, also referred to as `v` in papers
    prf_key: Key32,
}

/// Used by miners and validators to generate and prove challenges.
pub struct ChallengeSystem {
    key: ChallengeKey,
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

#[derive(Debug, Error)]
pub enum ChallengeSystemError {
    #[error("Uninitialised keys")]
    UninitialisedKeys,
}

#[derive(Debug, Error)]
pub enum ChallengeError {
    #[error(transparent)]
    Apdp(#[from] APDPError),
    #[error(transparent)]
    ChallengeSystem(#[from] ChallengeSystemError),
}

/// Useful cryptographic utilities
pub struct CryptoUtils;

impl CryptoUtils {
    pub fn generate_rsa_private_key(key_size: usize) -> Result<RsaPrivateKey, ChallengeError> {
        RsaPrivateKey::new(&mut rand::thread_rng(), key_size)
            .map_err(|_| APDPError::RsaKeyGeneration.into())
    }

    /// Generate the full domain hash (FDH)
    pub fn full_domain_hash(
        rsa_key: &RsaPrivateKey,
        data: &[u8],
    ) -> Result<BigUint, ChallengeError> {
        if data.is_empty() {
            return Err(APDPError::InvalidParameters.into());
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

    /// PRF generator
    /// TODO: is this supposed 16 bytes (as it is in python implementation)
    /// or 32 bytes?
    pub fn prf(key: &Key32, input_int: u64) -> Result<[u8; 32], ChallengeError> {
        if key.is_empty() {
            return Err(APDPError::InvalidParameters.into());
        }

        let mut mac =
            Hmac::<Sha256>::new_from_slice(key).map_err(|_| APDPError::InvalidParameters)?;

        mac.update(&input_int.to_be_bytes());
        let result = mac.finalize().into_bytes();
        let mut output = [0u8; 32];
        output.copy_from_slice(&result[..32]);
        Ok(output)
    }
}

// TODO: in the public schema we can make the private key's "e",
// N, and g parameters public :)
impl ChallengeKey {
    /// Generate a new APDP key, given RSA bits.
    pub fn generate(rsa_bits: Option<usize>) -> Result<Self, ChallengeError> {
        let key_size = match rsa_bits {
            Some(0) => return Err(APDPError::InvalidParameters.into()),
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

        Ok(ChallengeKey {
            rsa: Some(rsa),
            g: Some(g),
            prf_key,
        })
    }
}

impl ChallengeSystem {
    /// Create a new challenge system.
    pub fn new() -> Result<Self, ChallengeError> {
        Ok(ChallengeSystem {
            key: ChallengeKey::generate(None)?,
        })
    }

    /// Generate a new PDP challenge tag for the given data.
    pub fn generate_tag(&self, data: &[u8], index: u64) -> Result<APDPTag, ChallengeError> {
        let rsa = self.key.rsa.as_ref().ok_or(APDPError::InvalidParameters)?;
        use rsa::traits::PrivateKeyParts;
        use rsa::traits::PublicKeyParts;

        // RSA parameters
        let n = rsa.n().to_bytes_be(); // Modulus: product of prime numbers `p` and `q`

        let block_int =
            BigUint::from_bytes_be(data).modpow(&BigUint::from(1u32), &BigUint::from_bytes_be(&n));

        // w is PRF value - this is referred to as W = w_vi in the paper
        let w = CryptoUtils::prf(&self.key.prf_key, index)?;
        let fdh_hash = CryptoUtils::full_domain_hash(rsa, &w)?;
        debug!("FDH hash: {}, w : {:?}", fdh_hash, w);

        let g = self.key.g.as_ref().ok_or(APDPError::InvalidParameters)?;
        let n_big = BigUint::from_bytes_be(&n);

        let g_pow = g.modpow(&block_int, &n_big);
        let base = (&fdh_hash * &g_pow) % &n_big;

        let d = rsa.d().to_bytes_be();
        let d_big = BigUint::from_bytes_be(&d);

        let tag_value = base.modpow(&d_big, &n_big);

        Ok(APDPTag {
            index,
            tag_value,
            prf_value: w,
        })
    }

    /// TODO: is this it?
    /// Create a new challenge to send to a miner.
    pub fn issue_challenge(&self, tag: APDPTag) -> Challenge {
        use rsa::traits::PrivateKeyParts;
        use rsa::traits::PublicKeyParts;

        let mut prp_key = [0u8; 32];
        let _ = rand::thread_rng().try_fill_bytes(&mut prp_key);

        let mut prf_key = [0u8; 32];
        let _ = rand::thread_rng().try_fill_bytes(&mut prf_key);

        Challenge {
            tag,
            prp_key,
            prf_key,
        }
    }

    /// Generate a new proof to send back to the validator.
    fn generate_proof(&self, data: &[u8], tag: APDPTag, challenge: Challenge) -> Proof {
        let rsa_key = match &self.key.rsa {
            Some(key) => key,
            None => {
                panic!("No RSA key"); // TODO: better error handling
            }
        };

        let n = BigUint::from_bytes_be(&(&rsa_key.n()).to_bytes_be());

        debug!("Generating proof for data...");
        debug!("RSA modulus: {n}");
        let message = BigUint::from_bytes_be(data) % n.clone();

        // use prf_key (a.ka. k_2) to generate prf_result
        let prf_result =
            CryptoUtils::prf(&challenge.prf_key, 0).expect("Could not generate prf result");

        let coefficient = BigUint::from_bytes_be(&prf_result) % n.clone();

        debug!(
            "Message: {:?}, coefficient: {:?}, prf: {:?}",
            message, coefficient, prf_result
        );

        // referred to as "r0" in Zachary Peterson's code
        let aggregated_tag = tag.tag_value.modpow(&coefficient, &n);
        let aggregated_blocks = coefficient * message;
        //

        Proof {
            tag_value: aggregated_tag,
            big_m: aggregated_blocks,
        }
    }

    ///
    /// Verify the proof sent from the miner and determine whether it is valid or not.
    ///
    /// - `proof` is the proof from the miner to verify
    /// - `challenge` is the challenge for the proof that was given to the miner
    /// - `tag` is the APDP tag for the proof
    /// - `n` is the RSA modulus
    /// - `e` is the RSA public exponent
    fn verify_proof(
        &self,
        proof: Proof,
        challenge: Challenge,
        tag: APDPTag,
    ) -> Result<bool, ChallengeError> {
        let rsa_key = match &self.key.rsa {
            Some(key) => key,
            None => return Err(ChallengeSystemError::UninitialisedKeys.into()),
        };

        // TODO: is this OK?
        let e = BigUint::from_bytes_be(&(&rsa_key.e()).to_bytes_be());
        let n = BigUint::from_bytes_be(&(&rsa_key.n()).to_bytes_be());

        let mut tau = proof.tag_value.modpow(&e, &n);
        debug!("Computed tau: {tau}");

        // Generate the coefficient for block index a = f_k2(tag.index)
        let prf_result = CryptoUtils::prf(&challenge.prf_key, tag.index)?;
        let coefficient = BigUint::from_bytes_be(&prf_result) % &n;

        // Calculate the full-domain hash h(W_i)
        let fdh_hash = CryptoUtils::full_domain_hash(&rsa_key, &prf_result)?;

        // Calculate h(W_i)^a
        let mut denom = fdh_hash.modpow(&coefficient, &n);
        // Inverse h(W_i)^a to create 1/h(W_i)^a
        denom = denom.modinv(&n).expect("Could not perform mod inverse");
        // tao = tao * 1/h(W_i)^a mod N
        tau = (tau * denom) % n.clone();

        let g = self.key.g.clone().unwrap(); // TODO: handle error
                                             // g^M
                                             // let to_check = g.pow(
                                             //     proof
                                             //         .big_m
                                             //         .try_into()
                                             //         .expect("exponent too large for pow()"),
                                             // );

        // TODO: is the following right?
        // Use modpow instead of pow for g^M calculation
        debug!("g: {:?}", g);
        debug!("M: {:?}", proof.big_m);
        debug!("n: {:?}", n);
        debug!("τ: {:?}", tau);
        let to_check = g.modpow(&proof.big_m, &n);
        debug!("to_check: {:?}", to_check);
        // let t = g.pow

        let valid_proof = to_check == tau; // TODO: also check that gcd(e, 2(M^* − M )) = 1

        Ok(valid_proof)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use num_bigint::BigUint;
    use rand::RngCore;

    use std::sync::Once;

    // This runs before any tests
    static INIT: Once = Once::new();

    fn setup_logging() {
        INIT.call_once(|| {
            tracing_subscriber::fmt()
                .with_max_level(tracing::Level::DEBUG)
                .with_test_writer() // This ensures output goes to the test console
                .init();
        });
    }

    // --- Unit Tests ---

    #[test]
    fn test_prf_determinism() {
        // Arrange: a fixed key and input
        let key: Key32 = [42u8; 32];
        let input: u64 = 12345;

        // Act: call the PRF twice
        let output1 = CryptoUtils::prf(&key, input).expect("PRF should succeed");
        let output2 = CryptoUtils::prf(&key, input).expect("PRF should succeed");

        // Assert: outputs are 32 bytes and are equal
        assert_eq!(output1.len(), 32);
        assert_eq!(output1, output2);
    }

    #[test]
    fn test_full_domain_hash_success() {
        // Arrange: Generate an RSA key for testing and some non-empty data.
        let rsa_key =
            CryptoUtils::generate_rsa_private_key(2048).expect("RSA key generation should succeed");
        let data = b"Test data for FDH";

        // Act: Compute the full domain hash.
        let hash = CryptoUtils::full_domain_hash(&rsa_key, data)
            .expect("Full domain hash computation should succeed");

        // Assert: hash is less than the RSA modulus.
        let n = BigUint::from_bytes_be(&rsa_key.n().to_bytes_be());
        assert!(hash < n);
    }

    #[test]
    fn test_full_domain_hash_error_on_empty_data() {
        // Arrange: Generate an RSA key and pass empty data.
        let rsa_key =
            CryptoUtils::generate_rsa_private_key(2048).expect("RSA key generation should succeed");
        let empty_data: &[u8] = b"";

        // Act: Calling full_domain_hash with empty data should return an error.
        let result = CryptoUtils::full_domain_hash(&rsa_key, empty_data);

        // Assert: The error variant should be APDPError::InvalidParameters.
        assert!(result.is_err());
    }

    #[test]
    fn test_challenge_key_generate() {
        // Act: Generate a challenge key with a specific RSA size.
        let key = ChallengeKey::generate(Some(2048)).expect("Key generation should succeed");

        // Assert: RSA key and generator are set, and prf_key is 32 bytes.
        assert!(key.rsa.is_some());
        assert!(key.g.is_some());
        assert_eq!(key.prf_key.len(), 32);
    }

    #[test]
    fn test_generate_tag() {
        // Arrange: Create a challenge system and some test data.
        let system = ChallengeSystem::new().expect("Challenge system creation should succeed");
        let data = b"Some piece of data";
        let index = 7;

        // Act: Generate an APDP tag.
        let tag = system
            .generate_tag(data, index)
            .expect("Tag generation should succeed");

        // Assert: The tag's index is correct and prf_value is 32 bytes.
        assert_eq!(tag.index, index);
        assert_eq!(tag.prf_value.len(), 32);
        // We expect tag_value to be non-zero for non-trivial data.
        assert_ne!(tag.tag_value, BigUint::from(0u32));
    }

    // --- “Integration” Test ---
    //
    // This test simulates a roundtrip: a validator creates a challenge tag,
    // issues a challenge, a miner uses that challenge to generate a proof,
    // and finally the validator verifies the proof.
    //
    // Note: The functions generate_proof and verify_proof are not public;
    // however, because the tests are in the same module, they have access.
    #[test]
    fn test_challenge_proof_roundtrip() {
        setup_logging();
        // Arrange: Create a challenge system and sample data.
        let system = ChallengeSystem::new().expect("Challenge system creation should succeed");
        let data = b"Miner stored piece of data";
        let index = 42;

        // The validator generates a tag for the data.
        let tag = system
            .generate_tag(data, index)
            .expect("Tag generation should succeed");

        // The validator then issues a challenge to the miner.
        let challenge = system.issue_challenge(tag.clone());

        // Simulate the miner generating a proof using the challenge.
        // (Note: generate_proof is a private method; we can call it from our test module.)
        let proof = system.generate_proof(data, tag.clone(), challenge.clone());

        // Act: The validator verifies the proof.
        let verification_result = system
            .verify_proof(proof, challenge, tag)
            .expect("Verification should succeed");

        // Assert: The proof should be valid.
        assert!(verification_result, "The proof should verify correctly");
    }

    // Additional test to check that the challenge system fails gracefully if keys are not initialised.
    #[test]
    fn test_verify_proof_fails_without_rsa() {
        // Arrange: Create a challenge system and then manually remove the RSA key.
        let mut system = ChallengeSystem::new().expect("Challenge system creation should succeed");
        // For testing purposes, set the RSA key to None.
        system.key.rsa = None;

        // Create dummy values for tag, challenge, and proof.
        let dummy_tag = APDPTag {
            index: 0,
            tag_value: BigUint::from(123u32),
            prf_value: [0u8; 32],
        };
        let dummy_challenge = Challenge {
            tag: dummy_tag.clone(),
            prp_key: [0u8; 32],
            prf_key: [0u8; 32],
        };
        let dummy_proof = Proof {
            tag_value: BigUint::from(456u32),
            big_m: BigUint::from(789u32),
        };

        // Act: Attempt to verify the proof.
        let result = system.verify_proof(dummy_proof, dummy_challenge, dummy_tag);

        // Assert: It should return an error because RSA keys are uninitialised.
        assert!(result.is_err());
    }
}
