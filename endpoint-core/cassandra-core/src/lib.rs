#![cfg_attr(test, allow(clippy::unwrap_used))]
//! # Apache Cassandra Endpoint Core
//!
//! Cassandra/ScyllaDB driver integration using `scylla` driver with `deadpool` pooling.
//!
//! ## Usage
//!
//! ```ignore
//! use cassandra_core::config::CassandraConfig;
//! use cassandra_core::connection::{CassandraCredentials, CassandraTarget};
//!
//! # async fn example() -> Result<(), Box<dyn std::error::Error>> {
//! let config = CassandraConfig {
//!     target: CassandraTarget {
//!         known_nodes: vec!["127.0.0.1:9042".to_string()],
//!         ..Default::default()
//!     }),
//!     read_credentials: Some(CassandraCredentials::default()),
//!     write_credentials: Some(CassandraCredentials::default()),
//!     ..Default::default()
//! };
//! # Ok(())
//! # }
//! ```
//!
//! Supports cluster discovery, keyspace selection, and TLS configuration.

use borsh::{BorshDeserialize, BorshSerialize};
use deadpool::unmanaged::Pool;
use scylla::client::session::Session;
use serde::{Deserialize, Deserializer, Serialize};
use std::io::Read;
use std::num::NonZeroU32;
use std::ops::Deref;
use utoipa::openapi::{KnownFormat, ObjectBuilder, RefOr, Schema, SchemaFormat, Type};
use utoipa::{PartialSchema, ToSchema};

pub mod config;
pub mod connection;

/// Type alias for Cassandra async session pool (read operations).
pub type CassandraAsync = Pool<Session>;

/// Type alias for Cassandra session pool (write operations).
pub type CassandraTx = Pool<Session>;

/// Wrapper around `NonZeroU32` for serialization support.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct NonZeroU32Wrapper(pub NonZeroU32);
impl ToSchema for NonZeroU32Wrapper {}
impl PartialSchema for NonZeroU32Wrapper {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new().schema_type(Type::Integer).format(Some(SchemaFormat::KnownFormat(KnownFormat::Int32))).build(),
        ))
    }
}

impl Deref for NonZeroU32Wrapper {
    type Target = NonZeroU32;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

impl Serialize for NonZeroU32Wrapper {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        Serialize::serialize(&self.0, serializer)
    }
}

impl<'de> Deserialize<'de> for NonZeroU32Wrapper {
    fn deserialize<D: Deserializer<'de>>(d: D) -> Result<Self, D::Error> {
        let n = <NonZeroU32 as Deserialize>::deserialize(d)?;
        Ok(Self(n))
    }
}

impl BorshSerialize for NonZeroU32Wrapper {
    fn serialize<W: std::io::Write>(&self, writer: &mut W) -> std::io::Result<()> {
        BorshSerialize::serialize(&self.0, writer)
    }
}

impl BorshDeserialize for NonZeroU32Wrapper {
    fn deserialize(buf: &mut &[u8]) -> Result<Self, std::io::Error> {
        Self::deserialize_reader(&mut *buf)
    }

    fn deserialize_reader<R: Read>(reader: &mut R) -> Result<Self, std::io::Error> {
        let n = <NonZeroU32>::deserialize_reader(reader)?;
        Ok(Self(n))
    }
}
