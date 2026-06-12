use crate::NonZeroU32Wrapper;
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use scylla::frame::Compression;
use scylla::statement::Consistency;
use serde::{Deserialize, Serialize};
use std::net::{IpAddr, Ipv4Addr};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct CassandraConnection {
    pub known_nodes: Vec<String>,
    pub known_nodes_addr: Vec<SocketAddr>,
    pub user: Option<User>,
    pub compression: Option<CompressionWrapper>,
    pub auto_await_schema_agreement: Option<bool>,
    pub cluster_metadata_refresh_interval: Option<u64>,
    pub use_keyspace: Option<Keyspace>,
    pub host_filter: Option<String>,
    pub timeout: Option<u64>,
    pub write_coalescing: Option<bool>,
    pub keepalive_interval: Option<u64>,
    pub keepalive_timeout: Option<u64>,
    pub schema_agreement_interval: Option<u64>,
    pub schema_agreement_timeout: Option<u64>,
    pub keyspaces_to_fetch: Vec<String>,
    pub tcp_keepalive_interval: Option<u64>,
    pub tracing_info_fetch_interval: Option<u64>,
    pub tracing_info_fetch_attempts: Option<NonZeroU32Wrapper>,
    pub tracing_info_fetch_consistency: Option<ConsistencyWrapper>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub enum CompressionWrapper {
    #[default]
    Lz4,
    Snappy,
}

impl From<CompressionWrapper> for Compression {
    fn from(wrapper: CompressionWrapper) -> Self {
        match wrapper {
            CompressionWrapper::Lz4 => Compression::Lz4,
            CompressionWrapper::Snappy => Compression::Snappy,
        }
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[borsh(use_discriminant = true)]
pub enum ConsistencyWrapper {
    Any = 0x0000,
    One = 0x0001,
    Two = 0x0002,
    Three = 0x0003,
    Quorum = 0x0004,
    All = 0x0005,
    #[default]
    LocalQuorum = 0x0006,
    EachQuorum = 0x0007,
    LocalOne = 0x000A,
    Serial = 0x0008,
    LocalSerial = 0x0009,
}

impl From<ConsistencyWrapper> for Consistency {
    fn from(wrapper: ConsistencyWrapper) -> Self {
        match wrapper {
            ConsistencyWrapper::Any => Consistency::Any,
            ConsistencyWrapper::One => Consistency::One,
            ConsistencyWrapper::Two => Consistency::Two,
            ConsistencyWrapper::Three => Consistency::Three,
            ConsistencyWrapper::Quorum => Consistency::Quorum,
            ConsistencyWrapper::All => Consistency::All,
            ConsistencyWrapper::LocalQuorum => Consistency::LocalQuorum,
            ConsistencyWrapper::EachQuorum => Consistency::EachQuorum,
            ConsistencyWrapper::LocalOne => Consistency::LocalOne,
            ConsistencyWrapper::Serial => Consistency::Serial,
            ConsistencyWrapper::LocalSerial => Consistency::LocalSerial,
        }
    }
}

impl_connection!(CassandraConnection, EpKind::Cassandra);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect and protocol settings.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct CassandraTarget {
    pub known_nodes: Vec<String>,
    pub known_nodes_addr: Vec<SocketAddr>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<CompressionWrapper>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub auto_await_schema_agreement: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cluster_metadata_refresh_interval: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub use_keyspace: Option<Keyspace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub host_filter: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timeout: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_coalescing: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive_interval: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub keepalive_timeout: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_agreement_interval: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub schema_agreement_timeout: Option<u64>,
    pub keyspaces_to_fetch: Vec<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tcp_keepalive_interval: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing_info_fetch_interval: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing_info_fetch_attempts: Option<NonZeroU32Wrapper>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub tracing_info_fetch_consistency: Option<ConsistencyWrapper>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct CassandraCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<User>,
}

impl CassandraConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &CassandraTarget, creds: &CassandraCredentials) -> Self {
        Self {
            known_nodes: target.known_nodes.clone(),
            known_nodes_addr: target.known_nodes_addr.clone(),
            user: creds.user.clone(),
            compression: target.compression.clone(),
            auto_await_schema_agreement: target.auto_await_schema_agreement,
            cluster_metadata_refresh_interval: target.cluster_metadata_refresh_interval,
            use_keyspace: target.use_keyspace.clone(),
            host_filter: target.host_filter.clone(),
            timeout: target.timeout,
            write_coalescing: target.write_coalescing,
            keepalive_interval: target.keepalive_interval,
            keepalive_timeout: target.keepalive_timeout,
            schema_agreement_interval: target.schema_agreement_interval,
            schema_agreement_timeout: target.schema_agreement_timeout,
            keyspaces_to_fetch: target.keyspaces_to_fetch.clone(),
            tcp_keepalive_interval: target.tcp_keepalive_interval,
            tracing_info_fetch_interval: target.tracing_info_fetch_interval,
            tracing_info_fetch_attempts: target.tracing_info_fetch_attempts,
            tracing_info_fetch_consistency: target.tracing_info_fetch_consistency.clone(),
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(CassandraTarget, CassandraCredentials)> {
        Ok((
            CassandraTarget {
                known_nodes: self.known_nodes.clone(),
                known_nodes_addr: self.known_nodes_addr.clone(),
                compression: self.compression.clone(),
                auto_await_schema_agreement: self.auto_await_schema_agreement,
                cluster_metadata_refresh_interval: self.cluster_metadata_refresh_interval,
                use_keyspace: self.use_keyspace.clone(),
                host_filter: self.host_filter.clone(),
                timeout: self.timeout,
                write_coalescing: self.write_coalescing,
                keepalive_interval: self.keepalive_interval,
                keepalive_timeout: self.keepalive_timeout,
                schema_agreement_interval: self.schema_agreement_interval,
                schema_agreement_timeout: self.schema_agreement_timeout,
                keyspaces_to_fetch: self.keyspaces_to_fetch.clone(),
                tcp_keepalive_interval: self.tcp_keepalive_interval,
                tracing_info_fetch_interval: self.tracing_info_fetch_interval,
                tracing_info_fetch_attempts: self.tracing_info_fetch_attempts,
                tracing_info_fetch_consistency: self.tracing_info_fetch_consistency.clone(),
            },
            CassandraCredentials { user: self.user.clone() },
        ))
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct SocketAddr {
    ip: String,
    port: u16,
}

impl From<SocketAddr> for std::net::SocketAddr {
    fn from(addr: SocketAddr) -> Self {
        std::net::SocketAddr::new(addr.ip.parse().unwrap_or(IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1))), addr.port)
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct User {
    pub username: String,
    pub password: String,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct Keyspace {
    pub keyspace_name: String,
    pub case_sensitive: bool,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[serde(rename_all = "lowercase")]
pub enum SslMode {
    Disable,
    #[default]
    Prefer,
    Require,
}

impl From<SslMode> for tokio_postgres::config::SslMode {
    fn from(ssl_mode: SslMode) -> Self {
        match ssl_mode {
            SslMode::Disable => Self::Disable,
            SslMode::Prefer => Self::Prefer,
            SslMode::Require => Self::Require,
        }
    }
}
