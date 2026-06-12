use crate::PostgresAsync;
use crate::connection::{PostgresConnection, PostgresCredentials, PostgresTarget};
use borsh::{BorshDeserialize, BorshSerialize};
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::fmt;
use std::fmt::Debug;
use std::time::Duration;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

const DEFAULT_RAW_POOL_MAX_SIZE: usize = 64;
const DEFAULT_RAW_POOL_WAIT_TIMEOUT_MS: u64 = 30_000;
const ENV_RAW_POOL_MAX_SIZE: &str = "EDEN_POSTGRES_RAW_POOL_MAX_SIZE";
const ENV_RAW_READ_POOL_MAX_SIZE: &str = "EDEN_POSTGRES_RAW_READ_POOL_MAX_SIZE";
const ENV_RAW_WRITE_POOL_MAX_SIZE: &str = "EDEN_POSTGRES_RAW_WRITE_POOL_MAX_SIZE";
const ENV_RAW_ADMIN_POOL_MAX_SIZE: &str = "EDEN_POSTGRES_RAW_ADMIN_POOL_MAX_SIZE";
const ENV_RAW_SYSTEM_POOL_MAX_SIZE: &str = "EDEN_POSTGRES_RAW_SYSTEM_POOL_MAX_SIZE";
const ENV_RAW_POOL_WAIT_TIMEOUT_MS: &str = "EDEN_POSTGRES_RAW_POOL_WAIT_TIMEOUT_MS";
const ENV_RAW_POOL_RECYCLE_CHECK: &str = "EDEN_POSTGRES_RAW_POOL_RECYCLE_CHECK";

/// PostgreSQL endpoint configuration.
///
/// Stores a single connection **target** (host, port, database, TLS) shared
/// across all privilege tiers, with per-tier **credentials** (username,
/// password) that determine the database role used for each permission profile.
///
/// ## Accepted input formats
///
/// ### Legacy (4 independent connections)
/// ```json
/// {
///   "read_conn": {"url": "postgres://reader:pass@host/db"},
///   "write_conn": {"url": "postgres://writer:pass@host/db"}
/// }
/// ```
///
/// ### URL shorthand (single URL + per-tier credential overrides)
/// ```json
/// {
///   "url": "postgres://default:pass@host/db",
///   "read_credentials": {"username": "reader", "password": "readpass"}
/// }
/// ```
///
/// ### Split fields (canonical)
/// ```json
/// {
///   "target": {"host": "host", "port": 5432, "database": "db"},
///   "read_credentials": {"username": "reader", "password": "readpass"}
/// }
/// ```
#[derive(Debug, Default, Clone, Serialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[schema(title = "PostgresConfig")]
pub struct PostgresConfig {
    pub target: PostgresTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<PostgresCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<PostgresCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<PostgresCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<PostgresCredentials>,
}

impl_ep_config_target_auth!(PostgresConfig, PostgresConnection, PostgresTarget, PostgresCredentials, EpKind::Postgres);

impl fmt::Display for PostgresConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "target: {:?}, read: {:?}, write: {:?}, admin: {:?}, system: {:?}",
            self.target, self.read_credentials, self.write_credentials, self.admin_credentials, self.system_credentials
        )
    }
}

// ---------------------------------------------------------------------------
// Deserialization: target+credentials or URL shorthand
// ---------------------------------------------------------------------------

/// Intermediate representation for deserializing both input formats.
#[derive(Deserialize)]
struct PostgresConfigRaw {
    // Split fields (canonical)
    #[serde(default)]
    target: Option<PostgresTarget>,
    #[serde(default)]
    read_credentials: Option<PostgresCredentials>,
    #[serde(default)]
    write_credentials: Option<PostgresCredentials>,
    #[serde(default)]
    admin_credentials: Option<PostgresCredentials>,
    #[serde(default)]
    system_credentials: Option<PostgresCredentials>,

    // URL shorthand
    #[serde(default)]
    url: Option<String>,
    #[serde(default)]
    sslmode: Option<crate::connection::SslMode>,
}

impl<'de> Deserialize<'de> for PostgresConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = PostgresConfigRaw::deserialize(deserializer)?;

        if raw.target.is_some() && raw.url.is_some() {
            return Err(serde::de::Error::custom("Ambiguous config: provide either 'target' or 'url', not both"));
        }

        if let Some(target) = raw.target {
            // Split fields (canonical)
            Ok(PostgresConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if let Some(url) = raw.url {
            // URL shorthand — parse URL for target, use embedded creds as default
            let conn = PostgresConnection { url, sslmode: raw.sslmode };
            let (target, default_creds) = conn.split().map_err(serde::de::Error::custom)?;

            Ok(PostgresConfig {
                target,
                read_credentials: Some(raw.read_credentials.unwrap_or_else(|| default_creds.clone())),
                write_credentials: raw.write_credentials.or_else(|| Some(default_creds.clone())),
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
            })
        } else if raw.read_credentials.is_some() || raw.write_credentials.is_some() {
            Err(serde::de::Error::custom("Credentials provided without a target. Provide 'target' or 'url'."))
        } else {
            Ok(PostgresConfig::default())
        }
    }
}

// ---------------------------------------------------------------------------
// Pool creation
// ---------------------------------------------------------------------------

impl RWPool<PostgresAsync> for PostgresConfig {
    #[named]
    async fn conn_async(
        &self,
        connection: Box<dyn EpConnection>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<PostgresAsync, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("postgres.{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<PostgresConnection>() {
            Some(pg_conn) => pg_conn.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        Self::build_raw_pool(&connection)
    }
}

impl PostgresConfig {
    fn raw_pool_max_size_for_role(role: Option<&str>) -> usize {
        let role_env = match role {
            Some("read") => Some(ENV_RAW_READ_POOL_MAX_SIZE),
            Some("write") => Some(ENV_RAW_WRITE_POOL_MAX_SIZE),
            Some("admin") => Some(ENV_RAW_ADMIN_POOL_MAX_SIZE),
            Some("system") => Some(ENV_RAW_SYSTEM_POOL_MAX_SIZE),
            _ => None,
        };
        role_env
            .and_then(|name| std::env::var(name).ok())
            .or_else(|| std::env::var(ENV_RAW_POOL_MAX_SIZE).ok())
            .and_then(|value| value.trim().parse::<usize>().ok())
            .unwrap_or(DEFAULT_RAW_POOL_MAX_SIZE)
            .max(1)
    }

    fn raw_pool_recycle_check() -> bool {
        std::env::var(ENV_RAW_POOL_RECYCLE_CHECK)
            .ok()
            .map(|value| matches!(value.trim().to_ascii_lowercase().as_str(), "1" | "true" | "yes" | "on"))
            .unwrap_or(false)
    }

    fn raw_pool_wait_timeout() -> Duration {
        Duration::from_millis(
            std::env::var(ENV_RAW_POOL_WAIT_TIMEOUT_MS)
                .ok()
                .and_then(|value| value.trim().parse::<u64>().ok())
                .unwrap_or(DEFAULT_RAW_POOL_WAIT_TIMEOUT_MS)
                .max(1),
        )
    }

    /// Build a raw wire protocol connection pool from a PostgresConnection.
    ///
    /// The pool must be large enough to hold one connection per concurrent proxy
    /// client when extended query session pinning is active (each client that
    /// uses PARSE/BIND/EXECUTE holds a dedicated backend connection for its
    /// session lifetime to preserve prepared-statement state).
    pub fn build_raw_pool(conn: &PostgresConnection) -> ResultEP<crate::PgRawPool> {
        Self::build_raw_pool_for_endpoint(conn, telemetry::labels::SYSTEM_ORG_UUID, None)
    }

    pub fn build_raw_pool_for_endpoint_role(
        conn: &PostgresConnection,
        org_uuid: impl Into<String>,
        endpoint_uuid: Option<String>,
        role: &'static str,
    ) -> ResultEP<crate::PgRawPool> {
        Self::build_raw_pool_for_endpoint_inner(conn, org_uuid, endpoint_uuid, Some(role))
    }

    /// Build a raw pool tagged with an endpoint UUID so connections report
    /// per-endpoint labels on the `eden.connections` gauge.
    pub fn build_raw_pool_for_endpoint(
        conn: &PostgresConnection,
        org_uuid: impl Into<String>,
        endpoint_uuid: Option<String>,
    ) -> ResultEP<crate::PgRawPool> {
        Self::build_raw_pool_for_endpoint_inner(conn, org_uuid, endpoint_uuid, None)
    }

    fn build_raw_pool_for_endpoint_inner(
        conn: &PostgresConnection,
        org_uuid: impl Into<String>,
        endpoint_uuid: Option<String>,
        role: Option<&'static str>,
    ) -> ResultEP<crate::PgRawPool> {
        let parsed = crate::url::PostgresConnectionParsed::from_connection(conn)?;
        let mut manager =
            crate::pool::PgConnectionManager::new(parsed).with_org_uuid(org_uuid).with_recycle_check(Self::raw_pool_recycle_check());
        if let Some(uuid) = endpoint_uuid {
            manager = manager.with_endpoint_uuid(uuid);
        }
        crate::PgRawPool::builder(manager)
            .max_size(Self::raw_pool_max_size_for_role(role))
            .queue_mode(deadpool::managed::QueueMode::Lifo)
            .wait_timeout(Some(Self::raw_pool_wait_timeout()))
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .map_err(|e| EpError::connect(format!("Failed to build raw PG pool: {e}")))
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_deserialize_url_shorthand() {
        let json = serde_json::json!({
            "url": "postgresql://default:pass@dbhost:5432/mydb",
            "sslmode": "require",
            "read_credentials": {"username": "reader", "password": "readpass"}
        });

        let config: PostgresConfig = serde_json::from_value(json).expect("deserialize url");
        assert_eq!(config.target.host, "dbhost");
        assert_eq!(config.read_credentials.as_ref().map(|c| c.username.as_str()), Some("reader"));
        // write_credentials falls back to URL credentials
        assert_eq!(config.write_credentials.as_ref().map(|c| c.username.as_str()), Some("default"));
    }

    #[test]
    fn test_deserialize_split_fields() {
        let json = serde_json::json!({
            "target": {"host": "dbhost", "port": 5432, "database": "mydb", "sslmode": "require"},
            "read_credentials": {"username": "reader", "password": "readpass"},
            "write_credentials": {"username": "writer", "password": "writepass"}
        });

        let config: PostgresConfig = serde_json::from_value(json).expect("deserialize split");
        assert_eq!(config.target.host, "dbhost");
        assert_eq!(config.read_credentials.as_ref().map(|c| c.username.as_str()), Some("reader"));
        assert_eq!(config.write_credentials.as_ref().map(|c| c.username.as_str()), Some("writer"));
    }

    #[test]
    fn test_reject_ambiguous_target_and_url() {
        let json = serde_json::json!({
            "target": {"host": "host1"},
            "url": "postgresql://host2/db"
        });

        let result = serde_json::from_value::<PostgresConfig>(json);
        assert!(result.is_err());
    }

    #[test]
    fn test_serialize_roundtrip() {
        let config = PostgresConfig {
            target: PostgresTarget {
                host: "dbhost".to_string(),
                port: 5432,
                database: Some("mydb".to_string()),
                sslmode: None,
                application_name: None,
            },
            read_credentials: Some(PostgresCredentials {
                username: "reader".to_string(),
                password: Some("readpass".to_string()),
            }),
            write_credentials: Some(PostgresCredentials {
                username: "writer".to_string(),
                password: Some("writepass".to_string()),
            }),
            admin_credentials: None,
            system_credentials: None,
        };

        let json = serde_json::to_value(&config).expect("serialize");
        let parsed: PostgresConfig = serde_json::from_value(json).expect("deserialize");
        assert_eq!(parsed.target.host, "dbhost");
        assert_eq!(parsed.read_credentials.as_ref().map(|c| c.username.as_str()), Some("reader"));
        assert_eq!(parsed.write_credentials.as_ref().map(|c| c.username.as_str()), Some("writer"));
    }

    #[test]
    fn test_read_conn_composes_connection() {
        let config = PostgresConfig {
            target: PostgresTarget {
                host: "dbhost".to_string(),
                port: 5432,
                database: Some("mydb".to_string()),
                sslmode: None,
                application_name: None,
            },
            read_credentials: Some(PostgresCredentials {
                username: "reader".to_string(),
                password: Some("pass".to_string()),
            }),
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
        };

        let conn = config.read_conn().expect("read_conn should be Some");
        let pg_conn = conn.as_any().downcast_ref::<PostgresConnection>().expect("downcast");
        assert!(pg_conn.url.contains("reader"));
        assert!(pg_conn.url.contains("dbhost"));
    }
}
