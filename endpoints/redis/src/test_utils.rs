#![allow(dead_code)]
/// Test utilities for the Redis endpoint crate.
///
/// Provides `TestContext` for integration tests with RESP2/RESP3 support.
///
/// # Example
/// ```rust
/// use crate::test_utils::*;
///
/// #[tokio::test]
/// async fn test_get_command() {
///     let mut ctx = setup_resp2().await;
///     let result = ctx.read(GetInput { key: "foo".into() }).await;
///     ctx.stop().await;
/// }
/// ```
use crate::api::RedisApi;
use crate::ep::RedisEp;
use crate::request::RedisRequest;
use endpoint_test_utils::DEFAULT_REDIS_STACK_VERSION;
use endpoint_test_utils::database_manager_test_utils::{initialize_redis, initialize_redis_stack};
use endpoint_test_utils::telemetry_test_utils::test_telemetry;
use endpoint_types::{EP, EpRequest, Operation};
use ep_core::settings::EdenSettings;
use error::ResultEP;
use format::cache_uuid::EndpointCacheUuid;
use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
use redis_core::config::{MultiKeyExecution, RedisConfig};
use redis_core::connection::RedisConnection;
use redis_core::{RedisAsync, RedisTx};
use std::panic::AssertUnwindSafe;
use std::pin::Pin;
use telemetry::TelemetryWrapper;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};
/// The default Redis versions
pub const DEFAULT_REDIS_VERSION: &str = "7.4.1";

/// All the Redis versions we want to test
pub const REDIS_VERSIONS: &[&str] = &["5", "6", "7.2", "7.4", "8"];
pub const REDIS_STACK_VERSIONS: &[&str] = &["7.2.0-v20", "7.4.0-v8"];

/// Check version constraints and run test if valid
pub async fn check_version<F, Fut>(version: &str, min_version: Vec<&str>, max_version: Vec<&str>, f: F)
where
    F: FnOnce(&str) -> Fut,
    Fut: Future<Output = ()>,
{
    for min in min_version {
        if version_is_earlier(min, version) {
            println!("Skipping Redis {version}: earlier than minimum {min}");
            return;
        }
    }

    for max in max_version {
        if version_is_later(max, version) {
            println!("Skipping Redis {version}: later than maximum {max}");
            return;
        }
    }

    f(version).await
}

/// Run test for all versions, skipping those outside constraints
pub async fn test_all_versions<F, Fut>(min_version: Vec<&str>, max_version: Vec<&str>, f: F)
where
    F: Fn(&str) -> Fut,
    Fut: Future<Output = ()>,
{
    for version in REDIS_VERSIONS {
        check_version(version, min_version.clone(), max_version.clone(), |v| f(v)).await;
    }
}

/// Helper for commands that require a minimum Redis version
pub async fn test_all_protocols_min_version<F>(min_version: &str, f: F)
where
    F: for<'a> Fn(&'a mut TestContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
{
    use futures::FutureExt;

    for version in REDIS_VERSIONS {
        if version_is_earlier(min_version, version) {
            println!("Skipping Redis {version}: earlier than minimum {min_version}");
            continue;
        }

        for resp in [RespVersion::Resp2, RespVersion::Resp3] {
            if matches!(resp, RespVersion::Resp3) && version_is_earlier("6", version) {
                continue;
            }

            let mut ctx = setup(resp, Some(version)).await;
            let result = AssertUnwindSafe(f(&mut ctx)).catch_unwind().await;
            ctx.stop().await;

            if let Err(e) = result {
                std::panic::resume_unwind(e);
            }
        }
    }
}

/// Helper for commands that require a maximum Redis version
pub async fn test_all_protocols_max_version<F>(max_version: &str, f: F)
where
    F: for<'a> Fn(&'a mut TestContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
{
    use futures::FutureExt;

    for version in REDIS_VERSIONS {
        if version_is_later(max_version, version) {
            println!("Skipping Redis {version}: older than maximum {max_version}");
            continue;
        }

        for resp in [RespVersion::Resp2, RespVersion::Resp3] {
            if matches!(resp, RespVersion::Resp3) && version_is_earlier("6", version) {
                continue;
            }

            let mut ctx = setup(resp, Some(version)).await;
            let result = AssertUnwindSafe(f(&mut ctx)).catch_unwind().await;
            ctx.stop().await;

            if let Err(e) = result {
                std::panic::resume_unwind(e);
            }
        }
    }
}

/// check if the `test` version is greater than the `base` version
pub fn version_is_later(base: &str, test: &str) -> bool {
    let (base, test) = match_versions(base, test);

    for (b, t) in base.iter().zip(test.iter()) {
        match b.cmp(t) {
            std::cmp::Ordering::Less => return true,
            std::cmp::Ordering::Greater => return false,
            std::cmp::Ordering::Equal => continue,
        }
    }
    false
}

/// check if the `test` version is smaller than the `base` version
pub fn version_is_earlier(base: &str, test: &str) -> bool {
    let (base, test) = match_versions(base, test);

    for (b, t) in base.iter().zip(test.iter()) {
        match b.cmp(t) {
            std::cmp::Ordering::Greater => return true,
            std::cmp::Ordering::Less => return false,
            std::cmp::Ordering::Equal => continue,
        }
    }
    false
}

fn match_versions(base: &str, test: &str) -> (Vec<u16>, Vec<u16>) {
    let mut base: Vec<u16> = base.split('.').map(|b| b.parse::<u16>().unwrap_or(0)).collect();
    let mut test: Vec<u16> = test.split('.').map(|b| b.parse::<u16>().unwrap_or(0)).collect();

    let max_len = base.len().max(test.len());
    base.resize(max_len, 0);
    test.resize(max_len, 0);

    (base, test)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum RespVersion {
    Resp2,
    Resp3,
}

impl RespVersion {
    pub fn protocol_number(&self) -> u8 {
        match self {
            RespVersion::Resp2 => 2,
            RespVersion::Resp3 => 3,
        }
    }
}

impl std::fmt::Display for RespVersion {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            RespVersion::Resp2 => write!(f, "RESP2"),
            RespVersion::Resp3 => write!(f, "RESP3"),
        }
    }
}

// =========================================================================
// Test Context
// =========================================================================

pub struct TestContext {
    container: ContainerAsync<GenericImage>,
    host: String,
    port: u16,
    pub endpoint_uuid: EndpointCacheUuid,
    pub ep: RedisEp,
    pub telemetry: TelemetryWrapper,
    pub resp_version: RespVersion,
}

impl TestContext {
    pub async fn stop(self) {
        let _ = self.container.stop().await;
    }

    pub fn connection_config(&self) -> RedisConnection {
        RedisConnection {
            host: self.host.clone(),
            port: Some(self.port),
            username: None,
            password: None,
            db: None,
            tls: None,
            insecure: None,
            protocol_version: Some(self.resp_version.protocol_number()),
            connect_timeout_secs: None,
            max_retries: None,
        }
    }

    /// Execute a read operation
    pub async fn read<T>(&mut self, request: T) -> serde_json::Value
    where
        T: Clone + Operation<RedisAsync, RedisApi, RedisTx>,
    {
        let mut req = Box::new(RedisRequest(Box::new(request))) as Box<dyn EpRequest>;
        self.ep.read(&self.endpoint_uuid, &mut *req, EdenSettings::default(), &mut self.telemetry).await.expect("read failed")
    }

    /// Execute a write operation
    pub async fn write<T>(&mut self, request: T) -> serde_json::Value
    where
        T: Clone + Operation<RedisAsync, RedisApi, RedisTx>,
    {
        let req = Box::new(RedisRequest(Box::new(request))) as Box<dyn EpRequest>;
        self.ep.write(&self.endpoint_uuid, &*req, EdenSettings::default(), &mut self.telemetry).await.expect("write failed")
    }

    /// Send raw RESP bytes and get raw response
    pub async fn raw(&mut self, bytes: &[u8]) -> ResultEP<bytes::Bytes> {
        use crate::protocol::RedisBytes;
        self.ep
            .raw_bytes(&self.endpoint_uuid, RedisBytes::from(bytes.to_vec()), EdenSettings::default(), &mut self.telemetry)
            .await
    }

    pub async fn pinned_connection(&mut self) -> ResultEP<crate::ep::RedisPinnedConnection> {
        self.ep.pinned_write_connection(&self.endpoint_uuid, &mut self.telemetry).await
    }

    pub async fn raw_on_pinned(conn: &mut crate::ep::RedisPinnedConnection, bytes: &[u8]) -> ResultEP<bytes::Bytes> {
        conn.send_command_raw(bytes).await.map(|(r, _latency)| r.to_bytes())
    }

    /// Get a connection pool for metadata operations
    pub fn pool(&self) -> RedisAsync {
        use ep_core::GetPool;
        self.ep
            .pool()
            .pool()
            .get(&self.endpoint_uuid)
            .expect("pool should exist for endpoint")
            .conn()
            .read_conn()
            .expect("read connection should exist")
            .clone()
    }

    /// Get a mutable reference to telemetry wrapper
    pub fn telemetry(&self) -> TelemetryWrapper {
        self.telemetry.clone()
    }
}

pub async fn setup(resp_version: RespVersion, redis_version: Option<&str>) -> TestContext {
    setup_with_multi_key_execution(resp_version, redis_version, MultiKeyExecution::Native).await
}

pub async fn setup_with_multi_key_execution(
    resp_version: RespVersion,
    redis_version: Option<&str>,
    multi_key_execution: MultiKeyExecution,
) -> TestContext {
    let mut telemetry = test_telemetry();
    let version = redis_version.unwrap_or(DEFAULT_REDIS_VERSION);
    let (container, host, port) = initialize_redis(Some(version)).await;

    let conn = RedisConnection {
        host: host.clone(),
        port: Some(port),
        tls: None,
        insecure: None,
        db: None,
        username: None,
        password: None,
        protocol_version: Some(resp_version.protocol_number()),
        connect_timeout_secs: None,
        max_retries: None,
    };

    let (target, creds) = conn.split().expect("split connection");
    let config = Box::new(RedisConfig {
        target,
        read_credentials: Some(creds.clone()),
        write_credentials: Some(creds),
        multi_key_execution,
        ..Default::default()
    });

    let endpoint_uuid =
        EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

    let mut ep = RedisEp::new();
    ep.connect_async(&endpoint_uuid, config, &mut telemetry).await.expect("Failed to connect to Redis");

    TestContext {
        container,
        host,
        port,
        endpoint_uuid,
        ep,
        telemetry,
        resp_version,
    }
}

pub async fn setup_with_stack(resp_version: RespVersion, redis_version: Option<&str>) -> TestContext {
    let mut telemetry = test_telemetry();
    let version = redis_version.unwrap_or(DEFAULT_REDIS_STACK_VERSION);
    let (container, host, port) = initialize_redis_stack(Some(version)).await;

    let conn = RedisConnection {
        host: host.clone(),
        port: Some(port),
        tls: None,
        insecure: None,
        db: None,
        username: None,
        password: None,
        protocol_version: Some(resp_version.protocol_number()),
        connect_timeout_secs: None,
        max_retries: None,
    };

    let (target, creds) = conn.split().expect("split connection");
    let config = Box::new(RedisConfig {
        target,
        read_credentials: Some(creds.clone()),
        write_credentials: Some(creds),
        ..Default::default()
    });

    let endpoint_uuid =
        EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

    let mut ep = RedisEp::new();
    ep.connect_async(&endpoint_uuid, config, &mut telemetry).await.expect("Failed to connect to Redis");

    TestContext {
        container,
        host,
        port,
        endpoint_uuid,
        ep,
        telemetry,
        resp_version,
    }
}

pub async fn setup_resp2() -> TestContext {
    setup(RespVersion::Resp2, None).await
}

pub async fn setup_resp3() -> TestContext {
    setup(RespVersion::Resp3, None).await
}

/// Helper that can be reused for commands that run on both R2 and R3 instances
pub async fn test_all_protocols<F>(f: F)
where
    F: for<'a> Fn(&'a mut TestContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
{
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    for version in REDIS_VERSIONS {
        for resp in [RespVersion::Resp2, RespVersion::Resp3] {
            // RESP3 requires Redis 6+
            if matches!(resp, RespVersion::Resp3) && version_is_earlier("6", version) {
                continue;
            }

            let mut ctx = setup(resp, Some(version)).await;

            let result = AssertUnwindSafe(f(&mut ctx)).catch_unwind().await;

            ctx.stop().await;

            if let Err(e) = result {
                std::panic::resume_unwind(e);
            }
        }
    }
}

pub async fn test_all_protocols_with_stack<F>(f: F)
where
    F: for<'a> Fn(&'a mut TestContext) -> Pin<Box<dyn Future<Output = ()> + Send + 'a>>,
{
    use futures::FutureExt;
    use std::panic::AssertUnwindSafe;

    for version in REDIS_STACK_VERSIONS {
        for resp in [RespVersion::Resp2, RespVersion::Resp3] {
            // RESP3 requires Redis Stack 7.2+
            if matches!(resp, RespVersion::Resp3) && version_is_earlier("7.2.0", version) {
                continue;
            }

            let mut ctx = setup_with_stack(resp, Some(version)).await;

            let result = AssertUnwindSafe(f(&mut ctx)).catch_unwind().await;

            ctx.stop().await;

            if let Err(e) = result {
                std::panic::resume_unwind(e);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_connection_resp2() {
        setup_resp2().await.stop().await;
    }

    #[tokio::test]
    async fn test_connection_resp3() {
        setup_resp3().await.stop().await;
    }

    #[tokio::test]
    async fn test_connections() {
        test_all_protocols(|_ctx| {
            Box::pin(async move {
                // connection established successfully
            })
        })
        .await;
    }
}

#[cfg(test)]
mod version_tests {
    use super::*;

    #[test]
    fn test_version_is_later() {
        assert!(version_is_later("7.0.0", "7.2.4"));
        assert!(version_is_later("7", "7.2.4"));
        assert!(version_is_later("6.2.16", "7.0.0"));
        assert!(!version_is_later("7.2.4", "7.0.0"));
        assert!(!version_is_later("7.2.4", "7.2.4"));
    }

    #[test]
    fn test_version_is_earlier() {
        assert!(version_is_earlier("7.2.4", "7.0.0"));
        assert!(version_is_earlier("7.2.4", "7"));
        assert!(version_is_earlier("7.0.0", "6.2.16"));
        assert!(!version_is_earlier("7.0.0", "7.2.4"));
        assert!(!version_is_earlier("7.2.4", "7.2.4"));
    }

    #[test]
    fn test_version_equal() {
        assert!(!version_is_later("7.2.4", "7.2.4"));
        assert!(!version_is_earlier("7.2.4", "7.2.4"));
    }
}
