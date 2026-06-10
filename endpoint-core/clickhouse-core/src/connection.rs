use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct ClickhouseConnection {
    /// HTTP API URL (e.g., "http://localhost:8123")
    pub url: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_info: Option<Product>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<Options>>,

    // Native protocol settings (TCP port 9000)
    /// Host for native protocol (if different from HTTP URL host)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_host: Option<String>,
    /// Port for native protocol (default: 9000)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_port: Option<u16>,
    /// Enable TLS for native protocol
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native_tls: Option<bool>,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct Options {
    pub name: String,
    pub value: String,
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct Product {
    pub name: String,
    pub version: String,
}

impl_connection!(ClickhouseConnection, EpKind::Clickhouse);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect and protocol settings.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct ClickhouseTarget {
    /// HTTP API URL (e.g., "http://localhost:8123")
    pub url: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub database: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compression: Option<Compression>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub product_info: Option<Product>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub options: Option<Vec<Options>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_host: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub native_tls: Option<bool>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct ClickhouseCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub user: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl ClickhouseConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &ClickhouseTarget, creds: &ClickhouseCredentials) -> Self {
        Self {
            url: target.url.clone(),
            password: creds.password.clone(),
            database: target.database.clone(),
            user: creds.user.clone(),
            compression: target.compression.clone(),
            product_info: target.product_info.clone(),
            options: target.options.clone(),
            native_host: target.native_host.clone(),
            native_port: target.native_port,
            native_tls: target.native_tls,
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(ClickhouseTarget, ClickhouseCredentials)> {
        Ok((
            ClickhouseTarget {
                url: self.url.clone(),
                database: self.database.clone(),
                compression: self.compression.clone(),
                product_info: self.product_info.clone(),
                options: self.options.clone(),
                native_host: self.native_host.clone(),
                native_port: self.native_port,
                native_tls: self.native_tls,
            },
            ClickhouseCredentials { user: self.user.clone(), password: self.password.clone() },
        ))
    }
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[non_exhaustive]
#[derive(Default)]
pub enum Compression {
    /// Disables any compression.
    /// Used by default if the `lz4` feature is disabled.
    #[default]
    None,
    /// Uses `LZ4` codec to (de)compress.
    /// Used by default if the `lz4` feature is enabled.
    Lz4,
}

impl From<Compression> for clickhouse::Compression {
    fn from(compression: Compression) -> Self {
        match compression {
            Compression::None => clickhouse::Compression::None,
            Compression::Lz4 => clickhouse::Compression::Lz4,
        }
    }
}
