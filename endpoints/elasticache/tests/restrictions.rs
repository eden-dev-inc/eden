use bytes::Bytes;
use endpoint_test_utils::database_manager_test_utils::initialize_redis;
use endpoint_test_utils::telemetry_test_utils::test_telemetry;
use endpoint_types::{EP, EpRequest};
use ep_core::settings::EdenSettings;
use ep_elasticache::ep::ElasticacheEp;
use ep_elasticache::protocol::ElasticacheBytes;
use ep_elasticache::request::ElasticacheRequest;
use ep_redis::api::{RedisApi, RedisJsonValue};
use error::ResultEP;
use format::cache_uuid::EndpointCacheUuid;
use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
use redis_core::config::RedisConfig;
use redis_core::connection::{RedisCredentials, RedisTarget};
use telemetry::TelemetryWrapper;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

struct TestContext {
    container: ContainerAsync<GenericImage>,
    endpoint_uuid: EndpointCacheUuid,
    ep: ElasticacheEp,
    telemetry: TelemetryWrapper,
}

impl TestContext {
    async fn stop(self) {
        let _ = self.container.stop().await;
    }

    async fn read(&mut self, op: Box<dyn ep_redis::RedisOperation>) -> ResultEP<serde_json::Value> {
        let mut req = Box::new(ElasticacheRequest::from(op)) as Box<dyn EpRequest>;
        self.ep.read(&self.endpoint_uuid, &mut *req, EdenSettings::default(), &mut self.telemetry).await
    }

    async fn write(&mut self, op: Box<dyn ep_redis::RedisOperation>) -> ResultEP<serde_json::Value> {
        let req = Box::new(ElasticacheRequest::from(op)) as Box<dyn EpRequest>;
        self.ep.write(&self.endpoint_uuid, &*req, EdenSettings::default(), &mut self.telemetry).await
    }

    async fn raw(&mut self, bytes: &[u8]) -> ResultEP<Bytes> {
        self.ep
            .raw_bytes(
                &self.endpoint_uuid,
                ElasticacheBytes::from(bytes.to_vec()),
                EdenSettings::default(),
                &mut self.telemetry,
            )
            .await
    }
}

async fn setup(protocol_version: u8) -> TestContext {
    let mut telemetry = test_telemetry();
    let (container, host, port) = initialize_redis(None).await;

    let target = RedisTarget {
        host: host.clone(),
        port: Some(port),
        db: None,
        tls: None,
        insecure: None,
        protocol_version: Some(protocol_version),
        connect_timeout_secs: None,
        max_retries: None,
    };

    let credentials = RedisCredentials { username: None, password: None };

    let config = Box::new(RedisConfig {
        target,
        read_credentials: Some(credentials.clone()),
        write_credentials: Some(credentials),
        ..Default::default()
    });

    let endpoint_uuid =
        EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

    let mut ep = ElasticacheEp::new();
    ep.connect_async(&endpoint_uuid, config, &mut telemetry).await.expect("Failed to connect to Redis");

    TestContext { container, endpoint_uuid, ep, telemetry }
}

#[tokio::test]
async fn typed_restrictions_enforced() {
    for protocol_version in [2u8, 3u8] {
        let mut ctx = setup(protocol_version).await;

        let ping = RedisApi::Ping.decode_from_args(vec![]).expect("decode ping");
        ctx.read(ping).await.expect("PING should be allowed");

        let set = RedisApi::Set
            .decode_from_args(vec![
                RedisJsonValue::String("elasticache:key".into()),
                RedisJsonValue::String("value".into()),
            ])
            .expect("decode set");
        ctx.write(set).await.expect("SET should be allowed");

        let config_get = RedisApi::ConfigGet.decode_from_args(vec![RedisJsonValue::String("*".into())]).expect("decode config get");
        let err = ctx.read(config_get).await.expect_err("CONFIG GET should be blocked");
        assert!(err.to_string().contains("NOPERM"), "unexpected error: {err}");

        let save = RedisApi::Save.decode_from_args(vec![]).expect("decode save");
        let err = ctx.write(save).await.expect_err("SAVE should be blocked");
        assert!(err.to_string().contains("NOPERM"), "unexpected error: {err}");

        ctx.stop().await;
    }
}

#[tokio::test]
async fn raw_restrictions_enforced() {
    for protocol_version in [2u8, 3u8] {
        let mut ctx = setup(protocol_version).await;

        let resp = ctx.raw(b"*1\r\n$4\r\nPING\r\n").await.expect("PING should succeed");
        assert!(resp.starts_with(b"+PONG"), "unexpected PING response: {resp:?}");

        let err = ctx.raw(b"*2\r\n$6\r\nCONFIG\r\n$3\r\nGET\r\n").await.expect_err("CONFIG GET should be blocked");
        assert!(err.to_string().contains("NOPERM"), "unexpected error: {err}");

        ctx.stop().await;
    }
}
