use core::hash::{Hash, Hasher};
use std::{fmt, io::Read};

use borsh::{BorshDeserialize, BorshSerialize};
// use erased_serde::{Deserializer, Serializer};
use serde::{de::Error, Deserialize, Serialize};

use crate::ed25519::PubKeyImpl;

pub trait Identifier:
    // BorshSerialize
    // + BorshDeserialize
    Send
    + Sync
    // + From<String>
    + ToString
    + AsRef<[u8]>
    + fmt::Debug
    // + Default
    // + Clone
    + Unpin
    // + PartialEq
    // + Eq
    // + Hash
    // + PartialOrd
    // + Ord
    + fmt::Display
{
}

impl Hash for dyn Identifier {
    fn hash<H>(&self, state: &mut H)
    where
        H: Hasher,
    {
        self.as_ref().hash(state)
    }
}

impl PartialEq for dyn Identifier {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

pub trait PubKey: Identifier {}

impl Serialize for dyn PubKey {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        serializer.serialize_str(&hex::encode(self.as_ref()))
    }
}

impl<'de> Deserialize<'de> for Box<dyn PubKey> {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s: String = Deserialize::deserialize(deserializer)?;
        Ok(Box::new(
            crate::ed25519::PubKeyImpl::try_from(
                hex::decode(s).map_err(D::Error::custom)?.as_ref(),
            )
            .map_err(D::Error::custom)?,
        ))
    }
}

impl BorshSerialize for dyn PubKey {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(self.as_ref(), writer)
    }
}

impl BorshDeserialize for Box<dyn PubKey> {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
        Self::deserialize_reader(&mut *buf)
    }

    fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let bytes = <Vec<u8>>::deserialize_reader(reader)?;
        Ok(Box::new(
            PubKeyImpl::try_from(bytes.as_ref()).map_err(|_e| std::io::ErrorKind::InvalidData)?,
        ))
    }
}

pub trait Token: Identifier {}
pub trait Signature: Identifier + AsRef<[u8]> {
    fn verify(&self, bytes: &[u8]) -> bool;
}

impl PartialEq for dyn Signature {
    fn eq(&self, other: &Self) -> bool {
        self.as_ref() == other.as_ref()
    }
}

impl BorshSerialize for dyn Signature {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(self.as_ref(), writer)
    }
}

pub trait SubNetAddress: Identifier {}
