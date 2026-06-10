use crate::RedisAsync;
use crate::connection::{RedisConnection, RedisCredentials, RedisTarget};
use crate::multiplex::{RedisDirectMultiplexer, RedisDirectMultiplexerConfig};
use crate::pool::RedisConnectionManager;
use borsh::{BorshDeserialize, BorshSerialize};
use core::fmt;
use eden_logger_internal::{ctx_with_trace, log_debug};
use ep_core::ep::{EpConfig, EpConnection, RWPool};
use ep_core::impl_ep_config_target_auth;
use error::{ConnectError, EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use std::io::{self, Read};
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

const DEFAULT_REDIS_POOL_MAX_SIZE: u32 = 32;
const DEFAULT_REDIS_POOL_MAX_CONNECTIONS_CAP: u32 = 64;
/// Initial worker count for the legacy processor multiplexer,
/// **per shard**. Each worker holds one TCP connection to the backend.
///
/// Redis traffic now defaults to the direct lane-pool path, so the
/// legacy multiplexer defaults off. Endpoints that explicitly need the
/// fallback multiplexer can set `multiplexed_connections` > 0.
const DEFAULT_MULTIPLEXED_CONNECTIONS: u32 = 0;
/// Hard ceiling on autoscaler-spawned worker connections **per shard**
/// for one endpoint. Each worker holds one TCP connection to Redis, so
/// the proxy's worst-case backend connection count for an endpoint is
/// `multiplexed_max_workers × shard_count` — multiply through when
/// sizing against backend `maxclients`.
const DEFAULT_MULTIPLEXED_MAX_WORKERS: u32 = 32;
const DEFAULT_MULTIPLEXED_MAX_INFLIGHT_PER_CONNECTION: u32 = 256;
const DEFAULT_MULTIPLEXED_QUEUE_CAPACITY_PER_CONNECTION: u32 = 1024;
const DEFAULT_MULTIPLEXED_WRITE_BURST: u32 = 32;
/// Autoscaler tick cadence; checked once per interval.
const DEFAULT_MULTIPLEXED_SCALE_INTERVAL_MS: u64 = 100;
/// Scale up when avg in-flight per worker exceeds this percent of
/// `max_inflight_per_worker`.
const DEFAULT_MULTIPLEXED_SCALE_UP_THRESHOLD_PERCENT: u32 = 75;

/// Maximum time to wait for an available connection from the pool.
/// Bounds `pool.get().await` when all connections are in use.
const POOL_WAIT_TIMEOUT_SECS: u64 = 5;

/// Maximum time allowed for creating a new connection or recycling an existing one.
/// Bounds `Manager::create()` (TCP connect + TLS handshake + AUTH + SELECT)
/// and `Manager::recycle()`.
const POOL_CREATE_TIMEOUT_SECS: u64 = 10;

fn redis_pool_max_connections_cap() -> u32 {
    eden_config::limits().redis_pool_max_connections_cap
}

/// Whether multi-key Redis commands run natively or are deconstructed into
/// equivalent single-key commands.
#[derive(Debug, Default, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, BorshSerialize, BorshDeserialize, ToSchema)]
#[serde(rename_all = "snake_case")]
pub enum MultiKeyExecution {
    #[default]
    Native,
    Deconstruct,
}

fn resolve_pool_sizes(pool_cfg: RedisConnectionPoolConfig, cap: u32) -> Result<(u32, Option<u32>), EpError> {
    let requested_max = pool_cfg.max_connections.unwrap_or(DEFAULT_REDIS_POOL_MAX_SIZE);
    if requested_max == 0 {
        return Err(EpError::connect("max_connections must be greater than zero"));
    }

    let cap = if cap > 0 { cap } else { DEFAULT_REDIS_POOL_MAX_CONNECTIONS_CAP };
    let max_size = if requested_max > cap {
        if pool_cfg.max_connections.is_some() {
            return Err(EpError::connect(format!(
                "max_connections ({}) exceeds the allowed cap ({}) (set via limits.redis_pool_max_connections_cap)",
                requested_max, cap
            )));
        }
        cap
    } else {
        requested_max
    };

    let min_connections = match pool_cfg.min_connections {
        Some(0) => None,
        None => None,
        Some(min) => Some(min),
    };

    if let Some(min) = min_connections
        && min > max_size
    {
        return Err(EpError::connect(format!("min_connections ({}) cannot exceed max_connections ({})", min, max_size)));
    }

    Ok((max_size, min_connections))
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, Deserialize, BorshSerialize, ToSchema)]
#[schema(title = "RedisConnectionPoolConfig")]
pub struct RedisConnectionPoolConfig {
    /// Minimum number of pooled connections to keep warm. Defaults to
    /// disabled because Redis direct-proxy traffic uses the lane pool
    /// instead of this legacy endpoint pool. Set above `0` to warm
    /// fallback/pinned pool connections explicitly.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub min_connections: Option<u32>,
    /// Maximum number of pooled connections allowed (default: 100).
    /// Subject to a server-side cap via `limits.redis_pool_max_connections_cap`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub max_connections: Option<u32>,
    /// Maximum time in seconds to wait for a connection from the pool
    /// when all connections are in use (default: 5). If no connection
    /// becomes available within this window, the request fails.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub wait_timeout_secs: Option<u64>,
    /// Maximum time in seconds to create a new pool connection or recycle
    /// an existing one (default: 10). Covers TCP connect, TLS handshake,
    /// AUTH, and SELECT for new connections.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub create_timeout_secs: Option<u64>,
    /// Number of dedicated stateless Redis connections used by the
    /// legacy processor fallback's request multiplexer, **per shard**.
    /// This is separate from the primary direct-proxy lane pool in
    /// `eden_gateway::direct_pool`. Defaults to disabled because direct
    /// mode owns the normal hot path. Set this above `0` only for an
    /// endpoint that intentionally uses the legacy processor
    /// multiplexer.
    ///
    /// With thread-per-core dispatch every shard runtime lazy-builds
    /// its own multiplexer for an endpoint on first use, so the
    /// process-wide backend connection count is
    /// `multiplexed_connections * shard_count` per endpoint; pick a
    /// value that fits inside the backend's `maxclients` budget and
    /// the proxy host's open-fd limit.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiplexed_connections: Option<u32>,
    /// Maximum number of in-flight requests allowed per legacy processor
    /// multiplexer connection before the worker stops draining more
    /// requests from its queue until it reads a response back from Redis.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiplexed_max_inflight_per_connection: Option<u32>,
    /// Maximum number of queued legacy processor fallback requests waiting
    /// per multiplexed connection.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiplexed_queue_capacity_per_connection: Option<u32>,
    /// Maximum number of queued requests a multiplexed worker will write
    /// upstream before it yields to read at least one response.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiplexed_write_burst: Option<u32>,
    /// Hard ceiling on the number of multiplexed worker connections the
    /// autoscaler may spawn **per shard** for this endpoint. Defaults to
    /// `DEFAULT_MULTIPLEXED_MAX_WORKERS`. Process-wide ceiling is
    /// `multiplexed_max_workers × shard_count`; size against backend
    /// `maxclients`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiplexed_max_workers: Option<u32>,
    /// How often the autoscaler samples in-flight depth and decides whether
    /// to spawn additional workers, in milliseconds.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiplexed_scale_interval_ms: Option<u64>,
    /// Scale-up trigger expressed as a percentage of
    /// `multiplexed_max_inflight_per_connection`. When the average in-flight
    /// per worker exceeds this percent, the autoscaler appends one worker.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub multiplexed_scale_up_threshold_percent: Option<u32>,
    /// Deprecated compatibility field. Redis proxy connections now
    /// always use the direct-proxy lane-pool path; this value is
    /// ignored if present, but retained so existing serialized and
    /// JSON configs continue to deserialize.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub direct_mode: Option<bool>,
}

#[cfg(all(test, feature = "infra-tests"))]
#[derive(Debug, Default, Clone, PartialEq, BorshSerialize, BorshDeserialize)]
struct LegacyRedisConnectionPoolConfig {
    min_connections: Option<u32>,
    max_connections: Option<u32>,
    wait_timeout_secs: Option<u64>,
    create_timeout_secs: Option<u64>,
}

impl BorshDeserialize for RedisConnectionPoolConfig {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        Ok(Self {
            min_connections: deserialize_optional_borsh_field(reader)?,
            max_connections: deserialize_optional_borsh_field(reader)?,
            wait_timeout_secs: deserialize_optional_borsh_field(reader)?,
            create_timeout_secs: deserialize_optional_borsh_field(reader)?,
            multiplexed_connections: deserialize_optional_borsh_field(reader)?,
            multiplexed_max_inflight_per_connection: deserialize_optional_borsh_field(reader)?,
            multiplexed_queue_capacity_per_connection: deserialize_optional_borsh_field(reader)?,
            multiplexed_write_burst: deserialize_optional_borsh_field(reader)?,
            multiplexed_max_workers: deserialize_optional_borsh_field(reader)?,
            multiplexed_scale_interval_ms: deserialize_optional_borsh_field(reader)?,
            multiplexed_scale_up_threshold_percent: deserialize_optional_borsh_field(reader)?,
            direct_mode: deserialize_optional_borsh_field(reader)?,
        })
    }
}

fn deserialize_optional_borsh_field<T, R>(reader: &mut R) -> io::Result<Option<T>>
where
    T: BorshDeserialize,
    R: Read,
{
    let mut tag = [0u8; 1];
    match reader.read_exact(&mut tag) {
        Ok(()) => match tag[0] {
            0 => Ok(None),
            1 => T::deserialize_reader(reader).map(Some),
            unexpected => Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!("Invalid Option tag for RedisConnectionPoolConfig field: {unexpected}"),
            )),
        },
        Err(error) if error.kind() == io::ErrorKind::UnexpectedEof => Ok(None),
        Err(error) => Err(error),
    }
}

impl RedisConnectionPoolConfig {
    pub fn wait_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.wait_timeout_secs.unwrap_or(POOL_WAIT_TIMEOUT_SECS))
    }

    pub fn create_timeout(&self) -> std::time::Duration {
        std::time::Duration::from_secs(self.create_timeout_secs.unwrap_or(POOL_CREATE_TIMEOUT_SECS))
    }

    pub fn direct_multiplexer_config(&self, _max_pool_size: u32) -> Option<RedisDirectMultiplexerConfig> {
        // This config feeds the legacy Redis processor fallback's
        // multiplexer, not the primary direct-proxy lane pool. The
        // multiplexer holds its own TCP connections, independent of the
        // pool. `_max_pool_size` is accepted for API symmetry with other
        // config builders but does not constrain worker count.
        let worker_count = self.multiplexed_connections.unwrap_or(DEFAULT_MULTIPLEXED_CONNECTIONS);
        if worker_count == 0 {
            return None;
        }

        let max_workers = self.multiplexed_max_workers.unwrap_or(DEFAULT_MULTIPLEXED_MAX_WORKERS).max(worker_count);
        let scale_interval_ms = self.multiplexed_scale_interval_ms.unwrap_or(DEFAULT_MULTIPLEXED_SCALE_INTERVAL_MS).max(1);
        let scale_up_threshold_percent =
            self.multiplexed_scale_up_threshold_percent.unwrap_or(DEFAULT_MULTIPLEXED_SCALE_UP_THRESHOLD_PERCENT).clamp(1, 100);

        Some(RedisDirectMultiplexerConfig {
            worker_count: worker_count as usize,
            max_workers: max_workers as usize,
            max_inflight_per_worker: self
                .multiplexed_max_inflight_per_connection
                .unwrap_or(DEFAULT_MULTIPLEXED_MAX_INFLIGHT_PER_CONNECTION)
                .max(1) as usize,
            queue_capacity_per_worker: self
                .multiplexed_queue_capacity_per_connection
                .unwrap_or(DEFAULT_MULTIPLEXED_QUEUE_CAPACITY_PER_CONNECTION)
                .max(1) as usize,
            write_burst: self.multiplexed_write_burst.unwrap_or(DEFAULT_MULTIPLEXED_WRITE_BURST).max(1) as usize,
            scale_interval_ms,
            scale_up_threshold_percent,
        })
    }
}

#[derive(Debug, Default, Clone, PartialEq, Serialize, BorshSerialize, ToSchema)]
#[schema(title = "RedisConfig")]
pub struct RedisConfig {
    pub target: RedisTarget,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub read_credentials: Option<RedisCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub write_credentials: Option<RedisCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub admin_credentials: Option<RedisCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub system_credentials: Option<RedisCredentials>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub connection_pool: Option<RedisConnectionPoolConfig>,
    #[serde(default)]
    pub multi_key_execution: MultiKeyExecution,
}

impl_ep_config_target_auth!(RedisConfig, RedisConnection, RedisTarget, RedisCredentials, EpKind::Redis);

impl fmt::Display for RedisConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "target: {:?}, read: {:?}, write: {:?}, admin: {:?}, system: {:?}, connection_pool: {:?}, multi_key_execution: {:?}",
            self.target,
            self.read_credentials,
            self.write_credentials,
            self.admin_credentials,
            self.system_credentials,
            self.connection_pool,
            self.multi_key_execution
        )
    }
}

impl BorshDeserialize for RedisConfig {
    fn deserialize_reader<R: Read>(reader: &mut R) -> io::Result<Self> {
        let target = RedisTarget::deserialize_reader(reader)?;
        let read_credentials = Option::<RedisCredentials>::deserialize_reader(reader)?;
        let write_credentials = Option::<RedisCredentials>::deserialize_reader(reader)?;
        let admin_credentials = Option::<RedisCredentials>::deserialize_reader(reader)?;
        let system_credentials = Option::<RedisCredentials>::deserialize_reader(reader)?;
        let connection_pool = Option::<RedisConnectionPoolConfig>::deserialize_reader(reader)?;

        let mut tag = [0u8; 1];
        let multi_key_execution = match reader.read_exact(&mut tag) {
            Ok(()) => match tag[0] {
                0 => MultiKeyExecution::Native,
                1 => MultiKeyExecution::Deconstruct,
                unexpected => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("Unexpected MultiKeyExecution variant tag: {unexpected}"),
                    ));
                }
            },
            Err(e) if e.kind() == io::ErrorKind::UnexpectedEof => MultiKeyExecution::default(),
            Err(e) => return Err(e),
        };

        Ok(Self {
            target,
            read_credentials,
            write_credentials,
            admin_credentials,
            system_credentials,
            connection_pool,
            multi_key_execution,
        })
    }
}

// ---------------------------------------------------------------------------
// Backward-compatible deserialization
// ---------------------------------------------------------------------------

#[derive(Deserialize)]
struct RedisConfigRaw {
    // New format
    #[serde(default)]
    target: Option<RedisTarget>,
    #[serde(default)]
    read_credentials: Option<RedisCredentials>,
    #[serde(default)]
    write_credentials: Option<RedisCredentials>,
    #[serde(default)]
    admin_credentials: Option<RedisCredentials>,
    #[serde(default)]
    system_credentials: Option<RedisCredentials>,

    // Legacy format
    #[serde(default)]
    read_conn: Option<RedisConnection>,
    #[serde(default)]
    write_conn: Option<RedisConnection>,
    #[serde(default)]
    admin_conn: Option<RedisConnection>,
    #[serde(default)]
    system_conn: Option<RedisConnection>,

    // Extra fields
    #[serde(default)]
    connection_pool: Option<RedisConnectionPoolConfig>,
    #[serde(default)]
    multi_key_execution: MultiKeyExecution,
}

impl<'de> Deserialize<'de> for RedisConfig {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        let raw = RedisConfigRaw::deserialize(deserializer)?;

        let has_target = raw.target.is_some();
        let has_legacy = raw.read_conn.is_some() || raw.write_conn.is_some() || raw.admin_conn.is_some() || raw.system_conn.is_some();

        if has_target && has_legacy {
            return Err(serde::de::Error::custom(
                "Ambiguous config: provide either 'target' or legacy 'read_conn'/'write_conn' fields, not both",
            ));
        }

        if let Some(target) = raw.target {
            Ok(RedisConfig {
                target,
                read_credentials: raw.read_credentials,
                write_credentials: raw.write_credentials,
                admin_credentials: raw.admin_credentials,
                system_credentials: raw.system_credentials,
                connection_pool: raw.connection_pool,
                multi_key_execution: raw.multi_key_execution,
            })
        } else if has_legacy {
            let first = raw.read_conn.as_ref().or(raw.write_conn.as_ref()).or(raw.admin_conn.as_ref()).or(raw.system_conn.as_ref());
            let (target, _) = first.map(|c| c.split()).transpose().map_err(serde::de::Error::custom)?.unwrap_or_default();

            let extract = |c: &Option<RedisConnection>| c.as_ref().and_then(|c| c.split().ok().map(|(_, creds)| creds));

            Ok(RedisConfig {
                target,
                read_credentials: extract(&raw.read_conn),
                write_credentials: extract(&raw.write_conn),
                admin_credentials: extract(&raw.admin_conn),
                system_credentials: extract(&raw.system_conn),
                connection_pool: raw.connection_pool,
                multi_key_execution: raw.multi_key_execution,
            })
        } else {
            Ok(RedisConfig {
                connection_pool: raw.connection_pool,
                multi_key_execution: raw.multi_key_execution,
                ..Default::default()
            })
        }
    }
}

impl RWPool<RedisAsync> for RedisConfig {
    #[named]
    async fn conn_async(&self, connection: Box<dyn EpConnection>, telemetry_wrapper: &mut TelemetryWrapper) -> Result<RedisAsync, EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", self.kind(), function_name!()));

        let connection = match connection.as_any().downcast_ref::<RedisConnection>() {
            Some(redis_config) => redis_config.to_owned(),
            None => return Err(EpError::Connect(ConnectError::FailedToDowncastConfig)),
        };

        let _ctx = ctx_with_trace!().with_feature("redis_core");
        log_debug!(
            _ctx,
            "Creating Redis connection pool to {}",
            audience = eden_logger_internal::LogAudience::Internal,
            details = format!("{}", connection.url())
        );

        let cap = redis_pool_max_connections_cap();
        let pool_cfg = self.connection_pool.as_ref().cloned().unwrap_or_default();
        let (max_size, min_connections) = resolve_pool_sizes(pool_cfg.clone(), cap)?;

        let org_uuid = telemetry_wrapper
            .labels()
            .org_uuid()
            .map(ToOwned::to_owned)
            .unwrap_or_else(|| telemetry::labels::SYSTEM_ORG_UUID.to_string());
        let endpoint_uuid = telemetry_wrapper.labels().endpoint_uuid().map(ToOwned::to_owned);
        let mut manager = RedisConnectionManager::new(connection.clone()).with_multi_key_execution(self.multi_key_execution);
        manager = manager.with_org_uuid(org_uuid.clone());
        if let Some(uuid) = endpoint_uuid.clone() {
            manager = manager.with_endpoint_uuid(uuid);
        }
        let max_retries = manager.max_retries();

        let pool = deadpool::managed::Pool::builder(manager)
            .max_size(max_size as usize)
            .wait_timeout(Some(pool_cfg.wait_timeout()))
            .create_timeout(Some(pool_cfg.create_timeout()))
            .recycle_timeout(Some(pool_cfg.create_timeout()))
            .runtime(deadpool::Runtime::Tokio1)
            .build()
            .map_err(|e| EpError::connect(format!("Failed to create connection pool: {}", e)))?;

        // Lazy pool status poller: samples pool.status() every 5s to emit
        // active-vs-idle counts without touching the hot-path.
        let poll_pool = pool.clone();
        let poll_uuid = endpoint_uuid.clone();
        let poll_org_uuid = org_uuid.clone();
        let pool_status_poller =
            telemetry::spawn_pool_status_poller("redis", poll_org_uuid, poll_uuid, std::time::Duration::from_secs(5), move || {
                let s = poll_pool.status();
                Some((s.size, s.available.max(0)))
            });

        if let Some(min) = min_connections {
            // Hold the connections until we've opened the desired count to avoid reusing the same one.
            let mut warmed = Vec::with_capacity(min as usize);
            for _ in 0..min {
                let conn = pool.get().await.map_err(|e| EpError::connect(format!("Failed to prefill pool: {}", e)))?;
                warmed.push(conn);
            }
            drop(warmed);
        }

        let direct_multiplexer = if matches!(self.multi_key_execution, MultiKeyExecution::Native) {
            pool_cfg
                .direct_multiplexer_config(max_size)
                .map(|config| RedisDirectMultiplexer::new(connection.clone(), org_uuid.clone(), endpoint_uuid.clone(), config))
        } else {
            None
        };

        Ok(RedisAsync::new(
            pool,
            direct_multiplexer,
            connection,
            endpoint_uuid,
            max_retries,
            self.multi_key_execution,
            Some(pool_status_poller),
        ))
    }
}

#[cfg(test)]
mod unit_tests {
    use super::*;

    #[derive(BorshSerialize)]
    struct LegacyRedisConfig {
        target: RedisTarget,
        read_credentials: Option<RedisCredentials>,
        write_credentials: Option<RedisCredentials>,
        admin_credentials: Option<RedisCredentials>,
        system_credentials: Option<RedisCredentials>,
        connection_pool: Option<RedisConnectionPoolConfig>,
    }

    fn redis_target() -> RedisTarget {
        RedisTarget {
            host: "localhost".to_string(),
            port: Some(6379),
            ..Default::default()
        }
    }

    #[test]
    fn redis_config_serde_default_is_native() {
        let config: RedisConfig = serde_json::from_str("{}").expect("deserialize redis config");

        assert_eq!(config.multi_key_execution, MultiKeyExecution::Native);
    }

    #[test]
    fn redis_config_serde_explicit_deconstruct() {
        let config: RedisConfig = serde_json::from_str(r#"{"multi_key_execution":"deconstruct"}"#).expect("deserialize redis config");

        assert_eq!(config.multi_key_execution, MultiKeyExecution::Deconstruct);

        let value = serde_json::to_value(config).expect("serialize redis config");
        assert_eq!(value["multi_key_execution"], "deconstruct");
    }

    #[test]
    fn redis_config_legacy_format_defaults_multi_key_execution() {
        let config: RedisConfig = serde_json::from_str(
            r#"{
                "read_conn": {
                    "host": "localhost",
                    "port": 6379,
                    "tls": null
                }
            }"#,
        )
        .expect("deserialize legacy redis config");

        assert_eq!(config.multi_key_execution, MultiKeyExecution::Native);
    }

    #[test]
    fn redis_config_borsh_roundtrip() {
        let config = RedisConfig {
            target: redis_target(),
            multi_key_execution: MultiKeyExecution::Deconstruct,
            ..Default::default()
        };

        let bytes = borsh::to_vec(&config).expect("serialize redis config");
        let decoded: RedisConfig = borsh::from_slice(&bytes).expect("deserialize redis config");

        assert_eq!(decoded, config);
        assert_eq!(decoded.multi_key_execution, MultiKeyExecution::Deconstruct);
    }

    #[test]
    fn redis_config_borsh_roundtrip_with_pool_preserves_multi_key_execution() {
        let config = RedisConfig {
            target: redis_target(),
            connection_pool: Some(RedisConnectionPoolConfig {
                min_connections: Some(1),
                max_connections: Some(4),
                multiplexed_connections: Some(2),
                direct_mode: Some(false),
                ..Default::default()
            }),
            multi_key_execution: MultiKeyExecution::Deconstruct,
            ..Default::default()
        };

        let bytes = borsh::to_vec(&config).expect("serialize redis config");
        let decoded: RedisConfig = borsh::from_slice(&bytes).expect("deserialize redis config");

        assert_eq!(decoded, config);
        assert_eq!(decoded.multi_key_execution, MultiKeyExecution::Deconstruct);
    }

    #[test]
    fn redis_config_borsh_legacy_bytes_decode_as_native() {
        let legacy = LegacyRedisConfig {
            target: redis_target(),
            read_credentials: Some(RedisCredentials {
                username: Some("user".to_string()),
                password: Some("pass".to_string()),
            }),
            write_credentials: None,
            admin_credentials: None,
            system_credentials: None,
            connection_pool: Some(RedisConnectionPoolConfig {
                min_connections: Some(1),
                max_connections: Some(2),
                ..Default::default()
            }),
        };

        let bytes = borsh::to_vec(&legacy).expect("serialize legacy redis config");
        let decoded: RedisConfig = borsh::from_slice(&bytes).expect("deserialize redis config");

        assert_eq!(decoded.target, legacy.target);
        assert_eq!(decoded.read_credentials, legacy.read_credentials);
        assert_eq!(decoded.connection_pool, legacy.connection_pool);
        assert_eq!(decoded.multi_key_execution, MultiKeyExecution::Native);
    }

    #[test]
    fn redis_connection_manager_defaults_and_overrides_multi_key_execution() {
        let manager = RedisConnectionManager::new(RedisConnection { host: "localhost".to_string(), ..Default::default() });
        assert_eq!(manager.multi_key_execution(), MultiKeyExecution::Native);

        let manager = manager.with_multi_key_execution(MultiKeyExecution::Deconstruct);
        assert_eq!(manager.multi_key_execution(), MultiKeyExecution::Deconstruct);
    }
}

#[cfg(all(test, feature = "infra-tests"))]
mod tests {
    use super::*;

    #[test]
    fn resolve_pool_sizes_rejects_explicit_max_above_cap() {
        let err = resolve_pool_sizes(
            RedisConnectionPoolConfig {
                min_connections: None,
                max_connections: Some(33),
                ..Default::default()
            },
            32,
        )
        .expect_err("expected error");
        assert!(err.to_string().contains("exceeds the allowed cap"));
    }

    #[test]
    fn resolve_pool_sizes_clamps_default_max_to_cap() {
        let (max, min) = resolve_pool_sizes(
            RedisConnectionPoolConfig {
                min_connections: None,
                max_connections: None,
                ..Default::default()
            },
            16,
        )
        .expect("should resolve");
        assert_eq!(max, 16);
        assert_eq!(min, None);
    }

    #[test]
    fn resolve_pool_sizes_treats_min_zero_as_unset() {
        let (max, min) = resolve_pool_sizes(
            RedisConnectionPoolConfig {
                min_connections: Some(0),
                max_connections: Some(10),
                ..Default::default()
            },
            32,
        )
        .expect("should resolve");
        assert_eq!(max, 10);
        assert_eq!(min, None);
    }

    #[test]
    fn legacy_processor_multiplexer_defaults_off() {
        let config = RedisConnectionPoolConfig::default().direct_multiplexer_config(4);
        assert!(config.is_none());
    }

    #[test]
    fn legacy_processor_multiplexer_can_be_enabled_explicitly() {
        let config = RedisConnectionPoolConfig {
            multiplexed_connections: Some(4),
            multiplexed_max_workers: Some(8),
            ..Default::default()
        }
        .direct_multiplexer_config(4)
        .expect("legacy processor multiplexer should be enabled explicitly");
        assert_eq!(config.worker_count, 4);
        assert_eq!(config.max_workers, 8);
        assert_eq!(config.max_inflight_per_worker, DEFAULT_MULTIPLEXED_MAX_INFLIGHT_PER_CONNECTION as usize);
    }

    #[test]
    fn legacy_processor_multiplexer_max_workers_is_at_least_worker_count() {
        let config = RedisConnectionPoolConfig {
            multiplexed_connections: Some(4),
            multiplexed_max_workers: Some(2),
            ..Default::default()
        }
        .direct_multiplexer_config(4)
        .expect("legacy processor multiplexer should be enabled explicitly");
        assert_eq!(config.worker_count, 4);
        assert_eq!(config.max_workers, 4);
    }

    #[test]
    fn direct_mode_field_is_deprecated_compatibility_only() {
        let off = RedisConnectionPoolConfig { direct_mode: Some(false), ..Default::default() };
        assert_eq!(off.direct_mode, Some(false));
        assert!(off.direct_multiplexer_config(4).is_none());
    }

    #[test]
    fn legacy_processor_multiplexer_can_be_disabled() {
        let config = RedisConnectionPoolConfig { multiplexed_connections: Some(0), ..Default::default() }.direct_multiplexer_config(32);
        assert!(config.is_none());
    }

    #[test]
    fn redis_connection_pool_config_borsh_deserializes_legacy_payloads() {
        let legacy = LegacyRedisConnectionPoolConfig {
            min_connections: Some(2),
            max_connections: Some(16),
            wait_timeout_secs: Some(7),
            create_timeout_secs: Some(11),
        };

        let encoded = borsh::to_vec(&legacy).expect("legacy config should serialize");
        let decoded = borsh::from_slice::<RedisConnectionPoolConfig>(&encoded).expect("legacy config should deserialize");

        assert_eq!(
            decoded,
            RedisConnectionPoolConfig {
                min_connections: Some(2),
                max_connections: Some(16),
                wait_timeout_secs: Some(7),
                create_timeout_secs: Some(11),
                multiplexed_connections: None,
                multiplexed_max_inflight_per_connection: None,
                multiplexed_queue_capacity_per_connection: None,
                multiplexed_write_burst: None,
                multiplexed_max_workers: None,
                multiplexed_scale_interval_ms: None,
                multiplexed_scale_up_threshold_percent: None,
                direct_mode: None,
            }
        );
    }

    #[test]
    fn redis_connection_pool_config_borsh_rejects_partial_trailing_field() {
        let legacy = LegacyRedisConnectionPoolConfig {
            min_connections: Some(2),
            max_connections: Some(16),
            wait_timeout_secs: Some(7),
            create_timeout_secs: Some(11),
        };
        let mut encoded = borsh::to_vec(&legacy).expect("legacy config should serialize");
        encoded.push(1);

        let err = borsh::from_slice::<RedisConnectionPoolConfig>(&encoded).expect_err("partial option value should fail");
        assert!(matches!(err.kind(), std::io::ErrorKind::UnexpectedEof | std::io::ErrorKind::InvalidData));
    }

    #[test]
    fn resolve_pool_sizes_rejects_min_above_max() {
        let err = resolve_pool_sizes(
            RedisConnectionPoolConfig {
                min_connections: Some(11),
                max_connections: Some(10),
                ..Default::default()
            },
            32,
        )
        .expect_err("expected error");
        assert!(err.to_string().contains("min_connections (11) cannot exceed"));
    }
}
