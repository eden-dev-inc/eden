use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::tls::TlsData;
use ep_core::{ep::EpConnection, impl_connection};
use error::ResultEP;
use format::endpoint::EpKind;
use serde::{Deserialize, Deserializer, Serialize};
use utoipa::ToSchema;

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "RedisConnection")]
pub struct RedisConnection {
    /// Redis server hostname or IP address.
    pub host: String,
    /// Redis server port (default: 6379).
    pub port: Option<u16>,
    /// TLS configuration. Pass `true` for default TLS, or an object with
    /// `ca_cert`, `tls_cert`, `tls_key`, and `domain` fields for mTLS.
    #[serde(default, deserialize_with = "deserialize_tls", skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsData>,
    /// Skip TLS certificate verification (for development/testing only).
    pub insecure: Option<bool>,
    /// Redis database number to SELECT after connecting.
    pub db: Option<i64>,
    /// Username for Redis ACL authentication.
    pub username: Option<String>,
    /// Password for Redis AUTH.
    pub password: Option<String>,
    /// RESP protocol version: 2 or 3 (default: 3).
    pub protocol_version: Option<u8>,
    /// TCP connection timeout in seconds (default: 5).
    /// Controls how long to wait for the initial TCP handshake when
    /// establishing a new connection to the Redis server.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_secs: Option<u64>,
    /// Maximum number of retries on transient errors such as broken
    /// connections, IO failures, or pool errors (default: 1).
    /// Set to 0 to disable retries. Each retry acquires a fresh connection
    /// from the pool. Applies to both API and interlay wire-protocol paths.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
}

fn deserialize_tls<'de, D>(deserializer: D) -> Result<Option<TlsData>, D::Error>
where
    D: Deserializer<'de>,
{
    if let Ok(Some(value)) = Option::<serde_json::Value>::deserialize(deserializer) {
        // if tls is a boolean, return optional default TlsData
        if let Some(tls_bool) = value.as_bool() {
            if tls_bool { Ok(Some(TlsData::default())) } else { Ok(None) }
        } else {
            // deserialize TlsData struct, but with optional fields
            if let serde_json::Value::Object(ref map) = value {
                let mut tls_data = TlsData::default();
                if let Some(ok_val) = map.get("tls_cert")
                    && let serde_json::Value::String(s) = ok_val
                {
                    tls_data.tls_cert = s.to_owned();
                }
                if let Some(ok_val) = map.get("tls_key")
                    && let serde_json::Value::String(s) = ok_val
                {
                    tls_data.tls_key = s.to_owned();
                }
                if let Some(ok_val) = map.get("ca_cert")
                    && let serde_json::Value::String(s) = ok_val
                {
                    tls_data.ca_cert = s.to_owned();
                }
                if let Some(ok_val) = map.get("domain")
                    && let serde_json::Value::String(s) = ok_val
                {
                    tls_data.domain = s.to_owned();
                }
                Ok(Some(tls_data))
            } else {
                Err(serde::de::Error::custom("expected TlsData object"))
            }
        }
    } else {
        Ok(None) // no tls field or null
    }
}

const DEFAULT_CONNECT_TIMEOUT_SECS: u64 = 5;
const DEFAULT_MAX_RETRIES: u32 = 1;

impl RedisConnection {
    pub fn url(&self) -> String {
        format!("redis{}://{}:{}", if self.tls.is_some() { "s" } else { "" }, self.host, self.port.unwrap_or(6379),)
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version.unwrap_or(3)
    }

    pub fn connect_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.connect_timeout_secs.unwrap_or(DEFAULT_CONNECT_TIMEOUT_SECS))
    }

    pub fn max_retries(&self) -> u32 {
        self.max_retries.unwrap_or(DEFAULT_MAX_RETRIES)
    }
}

impl_connection!(RedisConnection, EpKind::Redis);

// ---------------------------------------------------------------------------
// Target + Credentials split
// ---------------------------------------------------------------------------

/// The connection target — WHERE to connect and protocol settings.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct RedisTarget {
    pub host: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub port: Option<u16>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub db: Option<i64>,
    #[serde(default, deserialize_with = "deserialize_tls", skip_serializing_if = "Option::is_none")]
    pub tls: Option<TlsData>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub insecure: Option<bool>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub protocol_version: Option<u8>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connect_timeout_secs: Option<u64>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_retries: Option<u32>,
}

/// Connection credentials — WHO to authenticate as.
#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
pub struct RedisCredentials {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub password: Option<String>,
}

impl RedisConnection {
    /// Compose a connection from a target and credentials.
    pub fn from_target_and_credentials(target: &RedisTarget, creds: &RedisCredentials) -> Self {
        Self {
            host: target.host.clone(),
            port: target.port,
            tls: target.tls.clone(),
            insecure: target.insecure,
            db: target.db,
            username: creds.username.clone(),
            password: creds.password.clone(),
            protocol_version: target.protocol_version,
            connect_timeout_secs: target.connect_timeout_secs,
            max_retries: target.max_retries,
        }
    }

    /// Split a connection into target and credentials.
    pub fn split(&self) -> ResultEP<(RedisTarget, RedisCredentials)> {
        Ok((
            RedisTarget {
                host: self.host.clone(),
                port: self.port,
                db: self.db,
                tls: self.tls.clone(),
                insecure: self.insecure,
                protocol_version: self.protocol_version,
                connect_timeout_secs: self.connect_timeout_secs,
                max_retries: self.max_retries,
            },
            RedisCredentials {
                username: self.username.clone(),
                password: self.password.clone(),
            },
        ))
    }
}
