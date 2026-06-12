// Hashing Functions

use borsh::{BorshDeserialize, BorshSerialize};
use error::VerificationError;
use indextreemap::IndexTreeMap;
use rand::Rng;
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::fmt;
use std::hash::Hash;
use utoipa::ToSchema;

/// Transaction Hashes (TxHash) are 36 byte sized arrays, which concatenates
/// all information relevant to the hashed transactions. The segments of a
/// txhash are as follows:
///
/// (Tx SHA256 Hash) + (TxRequest Enum Discriminant) + (Tx Enum Discriminant) + (BOOL .is_valid())
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord, Copy, BorshSerialize, BorshDeserialize)]
/// Transaction hash (36 bytes).
pub struct TxHash([u8; 36]);

impl TxHash {
    /// get the original transaction hash embedded in the transaction
    pub fn get_hash(&self) -> HashType {
        HashType::from(&self.0[0..32])
    }

    /// get the discriminant value associated with the tx request
    pub fn get_env_type(&self) -> u8 {
        self.0[32]
    }

    /// get the discriminant value associated with the tx type
    pub fn get_tx_type(&self) -> u16 {
        u16::from_le_bytes([self.0[33], self.0[34]])
    }

    /// get the transaction implementation (bool)
    pub fn get_is_valid(&self) -> u8 {
        self.0[35]
    }

    pub fn from_tx(env_type: u8, tx_type: u16, hash: [u8; 32], is_valid: u8) -> TxHash {
        TxHash::from([&[env_type], tx_type.to_le_bytes().as_slice(), hash.as_slice(), &[is_valid]].concat().as_slice())
    }

    pub fn to_vec(self) -> Vec<u8> {
        self.as_ref().to_vec()
    }

    pub fn from_byte_hash(hash: [u8; 36]) -> Self {
        Self(hash)
    }
}

impl AsRef<[u8]> for TxHash {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for TxHash {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> fmt::Result {
        let _ = write!(f, "{}", hex::encode(self));
        Ok(())
    }
}

impl From<String> for TxHash {
    fn from(hex_string: String) -> Self {
        TxHash::from(&hex_string)
    }
}

impl From<&String> for TxHash {
    fn from(hex_string: &String) -> Self {
        match hex::decode(hex_string) {
            Ok(bytes) => match bytes.try_into() {
                Ok(arr) => TxHash(arr),
                Err(_) => TxHash([0u8; 36]),
            },
            Err(_) => TxHash([0u8; 36]),
        }
    }
}

impl TryFrom<&str> for TxHash {
    type Error = VerificationError;
    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let h = hex::decode(value).map_err(|_| VerificationError::InvalidHex)?;
        let bytes: [u8; 36] = h.try_into().map_err(|_| VerificationError::InvaliDbtxHash)?;
        Ok(Self(bytes))
    }
}

impl From<&[u8]> for TxHash {
    fn from(bytes: &[u8]) -> Self {
        match bytes.try_into() {
            Ok(arr) => TxHash(arr),
            Err(_) => TxHash([0u8; 36]),
        }
    }
}

impl From<&TxHash> for String {
    fn from(pk: &TxHash) -> String {
        pk.to_string()
    }
}

impl Default for TxHash {
    fn default() -> Self {
        TxHash([0_u8; 36])
    }
}

impl Serialize for TxHash {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for TxHash {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <String as Deserialize>::deserialize(d)?;
        Ok(TxHash::from(s))
    }
}

#[derive(Clone, PartialEq, Default, Eq, Hash, PartialOrd, Ord, Copy, ToSchema, BorshSerialize, BorshDeserialize)]
/// Generic hash type (32 bytes).
pub struct HashType(pub [u8; 32]);

impl HashType {
    pub fn new() -> Self {
        let mut rng = rand::rng();
        let mut bytes = [0u8; 32];
        rng.fill(&mut bytes);
        HashType(bytes)
    }

    pub fn hash(s: &[u8]) -> Self {
        let mut sha256 = Sha256::default();
        sha256.update(s);
        HashType(sha256.finalize().into())
    }

    pub fn to_bytes(self) -> Vec<u8> {
        self.as_ref().to_vec()
    }

    pub fn from_byte_hash(hash: [u8; 32]) -> Self {
        Self(hash)
    }

    /// Normalizes the hash value to a float between 0.0 and 1.0.
    /// Used for percentage-based routing decisions.
    ///
    /// This uses the first 8 bytes of the hash to create a u64,
    /// then normalizes it to 0.0-1.0 range.
    pub fn normalized_value(&self) -> f64 {
        // Take the first 8 bytes and convert to u64
        let mut bytes = [0u8; 8];
        bytes.copy_from_slice(&self.0[0..8]);
        let value = u64::from_le_bytes(bytes);

        // Normalize to 0.0-1.0
        value as f64 / u64::MAX as f64
    }

    /// Checks if this hash falls within a given range (inclusive).
    /// Used for range-based routing in User migration strategy.
    pub fn in_range(&self, start: &HashType, end: &HashType) -> bool {
        self >= start && self <= end
    }

    /// Creates a hash representing a percentage threshold.
    ///
    /// # Examples
    /// ```
    /// use format::hashtype::HashType;
    ///
    /// // Create a threshold for 25% (0.25)
    /// let threshold = HashType::from_percentage(0.25);
    /// ```
    pub fn from_percentage(percentage: f64) -> Self {
        let percentage = percentage.clamp(0.0, 1.0);
        let value = (percentage * u64::MAX as f64) as u64;
        let bytes = value.to_le_bytes();

        let mut hash = [0u8; 32];
        hash[0..8].copy_from_slice(&bytes);
        HashType(hash)
    }

    /// Creates the minimum possible hash value (all zeros).
    pub fn min() -> Self {
        HashType([0u8; 32])
    }

    /// Creates the maximum possible hash value (all 0xFF).
    pub fn max() -> Self {
        HashType([0xFFu8; 32])
    }

    /// Creates a hash range that covers approximately the given percentage.
    /// Returns (start, end) where the range is [start, end] inclusive.
    ///
    /// # Examples
    /// ```
    /// use format::hashtype::HashType;
    ///
    /// // Create a range covering the first 25% of hash space
    /// let (start, end) = HashType::percentage_range(0.0, 0.25);
    /// ```
    pub fn percentage_range(start_pct: f64, end_pct: f64) -> (Self, Self) {
        let start_pct = start_pct.clamp(0.0, 1.0);
        let end_pct = end_pct.clamp(0.0, 1.0);

        let start_value = (start_pct * u64::MAX as f64) as u64;
        let end_value = (end_pct * u64::MAX as f64) as u64;

        let start_bytes = start_value.to_le_bytes();
        let end_bytes = end_value.to_le_bytes();

        let mut start_hash = [0u8; 32];
        let mut end_hash = [0u8; 32];

        start_hash[0..8].copy_from_slice(&start_bytes);
        end_hash[0..8].copy_from_slice(&end_bytes);

        (HashType(start_hash), HashType(end_hash))
    }
}

impl AsRef<[u8]> for HashType {
    #[inline]
    fn as_ref(&self) -> &[u8] {
        &self.0
    }
}

impl fmt::Display for HashType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}

// Add this new implementation
impl fmt::Debug for HashType {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}

impl From<String> for HashType {
    fn from(hex_string: String) -> Self {
        HashType::from(&hex_string)
    }
}

impl From<&String> for HashType {
    fn from(hex_string: &String) -> Self {
        HashType::from(hex_string.as_str())
    }
}

impl From<&str> for HashType {
    fn from(hex_string: &str) -> Self {
        match hex::decode(hex_string) {
            Ok(bytes) => match bytes.try_into() {
                Ok(arr) => HashType(arr),
                Err(_) => HashType([0u8; 32]),
            },
            Err(_) => HashType([0u8; 32]),
        }
    }
}

impl From<&[u8]> for HashType {
    fn from(bytes: &[u8]) -> Self {
        match bytes.try_into() {
            Ok(arr) => HashType(arr),
            Err(_) => HashType([0u8; 32]),
        }
    }
}

impl From<&HashType> for String {
    fn from(pk: &HashType) -> String {
        pk.to_string()
    }
}

impl Serialize for HashType {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for HashType {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <String as Deserialize>::deserialize(d)?;
        Ok(HashType::from(s))
    }
}

pub trait DoHash {
    fn hash(&self) -> HashType;
    // fn tx_hash(&self)s -> TxHash;
}

impl<K, V> DoHash for BTreeMap<K, V>
where
    K: Sized + AsRef<[u8]>,
    V: Sized,
{
    fn hash(&self) -> HashType {
        let mut bytes: Vec<u8> = Vec::with_capacity(self.len() * 32);
        for k in self.keys() {
            bytes.extend(k.as_ref())
        }
        HashType::hash(bytes.as_ref())
    }

    // fn tx_hash(&self) -> TxHash {
    //     let mut bytes: Vec<u8> = Vec::with_capacity(self.len() * 36);
    //     for (k, _) in self {
    //         bytes.extend(k.as_ref())
    //     }
    //     TxHash::hash(bytes.as_ref())
    // }
}

impl<K, V> DoHash for IndexTreeMap<K, V>
where
    K: Sized + Default + Ord + Clone + Hash + AsRef<[u8]>,
    V: Sized + Default + Clone,
{
    fn hash(&self) -> HashType {
        let mut bytes: Vec<u8> = Vec::with_capacity(self.len() * 32);
        for k in self.keys() {
            bytes.extend(k.as_ref())
        }
        HashType::hash(bytes.as_ref())
    }

    // fn tx_hash(&self) -> TxHash {
    //     let mut bytes: Vec<u8> = Vec::with_capacity(self.len() * 36);
    //     for (k, _) in self.iter() {
    //         bytes.extend(k.as_ref())
    //     }
    //     TxHash::hash(bytes.as_ref())
    // }
}
