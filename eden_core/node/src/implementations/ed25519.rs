use std::array::TryFromSliceError;
use std::fmt;
use std::ops::Deref;

use borsh::{BorshDeserialize, BorshSerialize};
use ed25519_dalek::{
    PUBLIC_KEY_LENGTH, SECRET_KEY_LENGTH, SIGNATURE_LENGTH, Signature as CryptoSignature, Signer, SigningKey, Verifier, VerifyingKey,
};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_debug, log_error, log_trace};
use error::VerificationError;
use function_name::named;
use serde::{Deserialize, Serialize, de, de::Deserializer, ser::Serializer};

#[derive(BorshSerialize, BorshDeserialize, Debug, Clone, PartialEq, PartialOrd, Eq, Hash)]
pub struct Signature([u8; SIGNATURE_LENGTH]);

impl Deref for Signature {
    type Target = [u8; SIGNATURE_LENGTH];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Default for Signature {
    fn default() -> Self {
        Self([0; SIGNATURE_LENGTH])
    }
}

impl fmt::Display for Signature {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}

impl AsRef<[u8]> for Signature {
    fn as_ref(&self) -> &[u8] {
        &**self
    }
}

impl TryFrom<&[u8]> for Signature {
    type Error = error::VerificationError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|_e: TryFromSliceError| error::VerificationError::SignatureError)?))
    }
}

impl TryFrom<&str> for Signature {
    type Error = error::VerificationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = hex::decode(value).map_err(|_e: hex::FromHexError| error::VerificationError::PubKeyError)?;
        Ok(Self(
            bytes.as_slice().try_into().map_err(|_e: TryFromSliceError| error::VerificationError::PubKeyError)?,
        ))
    }
}

// Serialize & Deserialize has to be rewritten to implement serialization of 64 bytes as a tuple
// only up to 32 bytes work out of the box, see https://docs.rs/serde/latest/src/serde/ser/impls.rs.html#1-998 for implementation
// implementation with hex string is ~30% slower than tuples
impl Serialize for Signature {
    fn serialize<S: Serializer>(&self, s: S) -> Result<S::Ok, S::Error> {
        s.serialize_str(&self.to_string())
    }
}

impl<'de> Deserialize<'de> for Signature {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let s = <String as serde::Deserialize>::deserialize(d)?;
        let b = hex::decode(s).map_err(|e| de::Error::invalid_value(de::Unexpected::Str(&e.to_string()), &"64-byte hex signature"))?;
        Signature::try_from(b.as_slice())
            .map_err(|e| de::Error::invalid_value(de::Unexpected::Str(&e.to_string()), &"64-byte hex signature"))
    }
}

// impl BorshSerialize for Signature {
//     fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
//         BorshSerialize::serialize(self.as_ref(), writer)
//     }
// }

// impl BorshDeserialize for Signature {
//     fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
//         Self::deserialize_reader(&mut *buf)
//     }

//     fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
//         let bytes = <Vec<u8>>::deserialize_reader(reader)?;
//         Ok(Box::new(
//             Signature::try_from(bytes.as_ref()).map_err(|_e| std::io::ErrorKind::InvalidData)?,
//         ))
//     }
// }

impl Signature {
    #[named]
    #[allow(unused_variables)] // bytes/e used in log macros when log features enabled
    pub fn verify(&self, bytes: &[u8], pubkey: &PubKey) -> Result<(), VerificationError> {
        let _ctx = ctx_with_trace!().with_feature("node");

        log_trace!(
            _ctx.clone(),
            "Verifying bytes",
            audience = LogAudience::Internal,
            bytes_len = bytes.len(),
            bytes = format!("{:?}", bytes)
        );
        log_trace!(_ctx.clone(), "PubKey", audience = LogAudience::Internal, pubkey = pubkey.to_string());
        log_trace!(_ctx.clone(), "Signature", audience = LogAudience::Internal, signature = self.to_string());
        let bytes: &[u8; 32] = match pubkey.as_ref().try_into() {
            Ok(b) => b,
            Err(_) => return Err(VerificationError::InvalidHex),
        };
        let pk = match VerifyingKey::from_bytes(bytes) {
            Ok(k) => k,
            Err(e) => {
                log_error!(
                    _ctx,
                    "Can't use pubkey",
                    audience = LogAudience::Internal,
                    pubkey = pubkey.to_string(),
                    error = e.to_string()
                );
                return Err(VerificationError::SignatureError);
            }
        };
        let sig = CryptoSignature::from_bytes(self);
        pk.verify(bytes, &sig).map_err(|e| {
            log_debug!(_ctx, "Signature error", audience = LogAudience::Internal, error = e.to_string());
            VerificationError::SignatureError
        })
    }

    pub fn sign(bytes: &[u8], signing_key: &[u8; SECRET_KEY_LENGTH]) -> Self {
        let sk = SigningKey::from_bytes(signing_key);
        Self(sk.sign(bytes).to_bytes())
    }
}
impl From<[u8; SIGNATURE_LENGTH]> for Signature {
    fn from(value: [u8; SIGNATURE_LENGTH]) -> Self {
        Self(value)
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct PubKey([u8; PUBLIC_KEY_LENGTH]);

impl Deref for PubKey {
    type Target = [u8; PUBLIC_KEY_LENGTH];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for PubKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}
impl AsRef<[u8]> for PubKey {
    fn as_ref(&self) -> &[u8] {
        &**self
    }
}
impl TryFrom<&[u8]> for PubKey {
    type Error = error::VerificationError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|_e: TryFromSliceError| error::VerificationError::PubKeyError)?))
    }
}
impl From<&[u8; PUBLIC_KEY_LENGTH]> for PubKey {
    fn from(value: &[u8; PUBLIC_KEY_LENGTH]) -> Self {
        Self(*value)
    }
}
impl TryFrom<&str> for PubKey {
    type Error = error::VerificationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = hex::decode(value).map_err(|_e: hex::FromHexError| error::VerificationError::PubKeyError)?;
        Ok(Self(
            bytes.as_slice().try_into().map_err(|_e: TryFromSliceError| error::VerificationError::PubKeyError)?,
        ))
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct SecKey([u8; SECRET_KEY_LENGTH]);

impl Deref for SecKey {
    type Target = [u8; SECRET_KEY_LENGTH];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SecKey {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}
impl AsRef<[u8]> for SecKey {
    fn as_ref(&self) -> &[u8] {
        &**self
    }
}
impl TryFrom<&[u8]> for SecKey {
    type Error = error::VerificationError;

    fn try_from(value: &[u8]) -> Result<Self, Self::Error> {
        Ok(Self(value.try_into().map_err(|_e: TryFromSliceError| error::VerificationError::SecretKeyError)?))
    }
}
impl From<&[u8; SECRET_KEY_LENGTH]> for SecKey {
    fn from(value: &[u8; SECRET_KEY_LENGTH]) -> Self {
        Self(*value)
    }
}
impl TryFrom<&str> for SecKey {
    type Error = error::VerificationError;

    fn try_from(value: &str) -> Result<Self, Self::Error> {
        let bytes = hex::decode(value).map_err(|_e: hex::FromHexError| error::VerificationError::PubKeyError)?;
        Ok(Self(
            bytes.as_slice().try_into().map_err(|_e: TryFromSliceError| error::VerificationError::PubKeyError)?,
        ))
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd, Default)]
pub struct Token([u8; 32]);

impl Deref for Token {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}

impl AsRef<[u8]> for Token {
    fn as_ref(&self) -> &[u8] {
        &**self
    }
}

#[derive(Serialize, Deserialize, BorshSerialize, BorshDeserialize, Debug, Clone, Hash, Eq, PartialEq, Ord, PartialOrd)]
pub struct SubNetAddress([u8; 32]);

impl Deref for SubNetAddress {
    type Target = [u8; 32];

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl fmt::Display for SubNetAddress {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", hex::encode(self))
    }
}

impl AsRef<[u8]> for SubNetAddress {
    fn as_ref(&self) -> &[u8] {
        &**self
    }
}
