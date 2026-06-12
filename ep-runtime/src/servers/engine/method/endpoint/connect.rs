use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus};
#[cfg(feature = "llm")]
use llm_core::{LlmCredential, config::LlmConfig};

use crate::comp::MyEngineService;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::methods::insert::InsertMethod;
use database::db::methods::insert::endpoint::InsertEndpoint;
use database::endpoint_schema::endpoint::EndpointSchema;
use eden_core::error::{ConnectError, EpError, ResultEP};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{OrganizationCacheUuid, OrganizationUuid};
use eden_core::format::{cache_id::EndpointCacheId, cache_uuid::EndpointCacheUuid};
use eden_core::telemetry::TelemetryWrapper;
use endpoint::router_for;
use ep_core::ep::EpConfig;
use function_name::named;
use std::borrow::Cow;
#[cfg(feature = "llm")]
use std::collections::HashSet;
#[cfg(feature = "llm")]
use uuid::Uuid;

impl MyEngineService {
    #[named]
    async fn connect_driver(
        &self,
        database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_schema: &EndpointSchema,
        organization_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<EndpointCacheUuid> {
        let mut span = telemetry_wrapper.client_tracer(format!("endpoint.{}", function_name!()));
        let PreparedEndpointRuntimeConfig { kind, endpoint_cache_key, config } =
            prepared_endpoint_runtime_config(database, endpoint_schema, organization_uuid, telemetry_wrapper).await?;

        self.ensure_router_initialized(kind).await?;

        {
            let mut lock = self.router.write().await;
            let ep = lock.get_mut(&kind).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

            ep.connect_boxed(&endpoint_cache_key, config, telemetry_wrapper).await?;
        }

        {
            let lock = self.router.read().await;
            let ep = lock.get(&kind).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

            if let Err(e) = ep.health_check_boxed(&endpoint_cache_key, telemetry_wrapper).await {
                span.add_event(
                    "health_check failed for new connection",
                    vec![FastSpanAttribute::new("endpoint_type", kind.to_string())],
                );
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(format!("Health check failed: {}", e)) });

                drop(lock);

                let rollback_result = {
                    let mut lock = self.router.write().await;
                    match lock.get_mut(&kind) {
                        Some(ep) => ep.disconnect_boxed(&endpoint_cache_key, telemetry_wrapper).await,
                        None => Err(EpError::Connect(ConnectError::CouldNotGetEndpoint)),
                    }
                };

                if let Err(rollback_error) = rollback_result {
                    span.add_event(
                        "failed to roll back connection after health_check failure",
                        vec![
                            FastSpanAttribute::new("endpoint_type", kind.to_string()),
                            FastSpanAttribute::new("error", rollback_error.to_string()),
                        ],
                    );
                }

                return Err(e);
            }
        }

        span.add_event("health_check passed", vec![FastSpanAttribute::new("endpoint_type", kind.to_string())]);

        Ok(endpoint_cache_key)
    }

    #[named]
    pub async fn validate_endpoint_runtime_config(
        &self,
        database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_schema: &EndpointSchema,
        organization_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(format!("endpoint.{}", function_name!()));
        let PreparedEndpointRuntimeConfig { kind, endpoint_cache_key, config } =
            prepared_endpoint_runtime_config(database, endpoint_schema, organization_uuid, telemetry_wrapper).await?;

        let mut ep = router_for(kind);

        ep.connect_boxed(&endpoint_cache_key, config, telemetry_wrapper).await?;

        let health_result = ep.health_check_boxed(&endpoint_cache_key, telemetry_wrapper).await;
        let disconnect_result = ep.disconnect_boxed(&endpoint_cache_key, telemetry_wrapper).await;

        if let Err(disconnect_error) = disconnect_result {
            span.add_event(
                "failed to disconnect temporary endpoint validation connection",
                vec![
                    FastSpanAttribute::new("endpoint_type", kind.to_string()),
                    FastSpanAttribute::new("error", disconnect_error.to_string()),
                ],
            );

            if health_result.is_ok() {
                return Err(disconnect_error);
            }
        }

        if let Err(e) = health_result {
            span.add_event(
                "health_check failed for temporary endpoint validation connection",
                vec![FastSpanAttribute::new("endpoint_type", kind.to_string())],
            );
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(format!("Health check failed: {}", e)) });
            return Err(e);
        }

        span.add_event("validation health_check passed", vec![FastSpanAttribute::new("endpoint_type", kind.to_string())]);

        Ok(())
    }

    async fn ensure_router_initialized(&self, kind: EpKind) -> ResultEP<()> {
        let mut lock = self.router.write().await;

        lock.entry(kind).or_insert_with(|| router_for(kind));

        Ok(())
    }
}

struct PreparedEndpointRuntimeConfig {
    kind: EpKind,
    endpoint_cache_key: EndpointCacheUuid,
    config: Box<dyn EpConfig>,
}

async fn prepared_endpoint_runtime_config(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    endpoint_schema: &EndpointSchema,
    organization_uuid: &OrganizationUuid,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<PreparedEndpointRuntimeConfig> {
    #[cfg(not(feature = "llm"))]
    let _ = (database, telemetry_wrapper);

    let kind = endpoint_schema.kind();
    #[cfg(feature = "llm")]
    let mut config = endpoint_schema.config();
    #[cfg(not(feature = "llm"))]
    let config = endpoint_schema.config();

    #[cfg(feature = "llm")]
    {
        if kind == EpKind::Llm
            && let Some(llm_config) = config.as_mut_any().downcast_mut::<LlmConfig>()
        {
            hydrate_llm_credentials(database, organization_uuid, llm_config, telemetry_wrapper).await?;
        }
    }

    let organization_cache_uuid = OrganizationCacheUuid::from(organization_uuid.clone());
    let endpoint_cache_key = endpoint_schema.cache_key(organization_cache_uuid);

    Ok(PreparedEndpointRuntimeConfig { kind, endpoint_cache_key, config })
}

impl MyEngineService {
    #[named]
    pub async fn connect(
        &self,
        database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        input: &InsertEndpoint,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<String> {
        let mut span = telemetry_wrapper.client_tracer(format!("database.{}.role", function_name!()));
        let endpoint_schema = input.get_endpoint_schema();
        let organization_uuid = input.get_organization_uuid();
        let kind = endpoint_schema.kind();

        span.add_event("Processing new connection", vec![FastSpanAttribute::new("kind".to_string(), kind.to_string())]);

        let endpoint_cache_key = self.connect_driver(database, endpoint_schema, organization_uuid, telemetry_wrapper).await?;

        match <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as InsertMethod<
            EndpointSchema,
            EndpointCacheUuid,
            EndpointCacheId,
            InsertEndpoint,
        >>::insert(database, input.clone(), telemetry_wrapper)
        .await
        {
            Ok(_) => {
                span.add_simple_event("added endpoint to directory");
                Ok("connected".to_string())
            }
            Err(e) => {
                span.add_simple_event("failed to add endpoint to directory");

                let mut lock = self.router.write().await;
                let ep = lock.get_mut(&kind).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

                let _ = ep.disconnect_boxed(&endpoint_cache_key, telemetry_wrapper).await;
                Err(EpError::connect(e))
            }
        }
    }

    #[named]
    pub async fn reconnect(
        &self,
        database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_schema: &EndpointSchema,
        organization_uuid: &OrganizationUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<String> {
        let mut span = telemetry_wrapper.client_tracer(format!("database.{}.role", function_name!()));
        let PreparedEndpointRuntimeConfig { kind, endpoint_cache_key, config } =
            prepared_endpoint_runtime_config(database, endpoint_schema, organization_uuid, telemetry_wrapper).await?;

        span.add_event("Reconnecting existing endpoint", vec![FastSpanAttribute::new("kind".to_string(), kind.to_string())]);

        self.ensure_router_initialized(kind).await?;

        {
            let mut lock = self.router.write().await;
            let ep = lock.get_mut(&kind).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

            ep.reconnect_boxed(&endpoint_cache_key, config, telemetry_wrapper).await?;
        }

        Ok("connected".to_string())
    }
}

#[cfg(feature = "llm")]
async fn hydrate_llm_credentials(
    database: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
    organization_uuid: &OrganizationUuid,
    config: &mut LlmConfig,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    let mut credential_ids: HashSet<Uuid> = HashSet::new();

    if let Some(read_creds) = &config.read_credentials
        && let Some(id) = read_creds.credential_id
    {
        credential_ids.insert(id);
    }

    if let Some(write_creds) = &config.write_credentials
        && let Some(id) = write_creds.credential_id
    {
        credential_ids.insert(id);
    }

    if credential_ids.is_empty() {
        return Ok(());
    }

    let credential_id_list: Vec<Uuid> = credential_ids.into_iter().collect();
    let stored_credentials = database.fetch_llm_credentials_by_ids(organization_uuid, &credential_id_list, telemetry_wrapper).await?;

    for stored in stored_credentials {
        let api_key = stored.api_key.trim();
        let api_key = if api_key.is_empty() { None } else { Some(api_key.to_string()) };

        let credential = LlmCredential {
            id: stored.id,
            provider: stored.provider,
            label: stored.label.clone(),
            description: stored.description.clone(),
            base_url: stored.base_url.clone(),
            api_key,
        };

        config.register_credential(credential);
    }

    Ok(())
}

#[cfg(all(test, feature = "http", not(embedded_db), not(feature = "embedded-db")))]
mod tests {
    use super::*;
    use database::db::lib::{
        CacheTtl, ClickhouseDbConfig, create_clickhouse_connection, create_postgres_connection, create_redis_connection,
    };
    use eden_core::format::{EdenId, EndpointId, UserUuid};
    #[cfg(feature = "http")]
    use endpoint::http::ep::HttpEp;
    use ep_core::GetPool;
    use ep_core::ep::EpConfig;
    #[cfg(feature = "http")]
    use http_core::config::HttpConfig;
    #[cfg(feature = "http")]
    use http_core::connection::{HttpCredentials, HttpTarget};
    use httpmock::MockServer;

    async fn lazy_database_manager() -> DatabaseManager<RedisConn, PgConn, ClickhouseConn> {
        let redis_cache = create_redis_connection("redis://127.0.0.1:1", 0).await.expect("create lazy redis cache pool");
        let redis_rbac = create_redis_connection("redis://127.0.0.1:1", 1).await.expect("create lazy redis rbac pool");
        let postgres_pool = create_postgres_connection("postgres://postgres:postgres@127.0.0.1:1/postgres")
            .await
            .expect("create lazy postgres pool");
        let clickhouse_config = ClickhouseDbConfig::new("http://127.0.0.1:1", None, None, None, 1).expect("clickhouse config");
        let clickhouse_pool = create_clickhouse_connection(&clickhouse_config).expect("create lazy clickhouse pool");

        DatabaseManager::new_with_connections(redis_cache, redis_rbac, postgres_pool, clickhouse_pool, CacheTtl::from_secs(60), None)
    }

    #[tokio::test]
    async fn connect_driver_health_check_failure_rolls_back_runtime_pool() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/health");
                then.status(404);
            })
            .await;

        let config = HttpConfig {
            target: HttpTarget { url: server.base_url() },
            write_credentials: Some(HttpCredentials { headers: None }),
            ..Default::default()
        };
        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_http_rollback".to_string()),
            EpKind::Http,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );
        let organization_uuid = OrganizationUuid::new_uuid();
        let endpoint_cache_key = endpoint_schema.cache_key(OrganizationCacheUuid::from(organization_uuid.clone()));
        let database = lazy_database_manager().await;
        let engine_service = MyEngineService::default();
        let telemetry_wrapper = &mut crate::test_utils::telemetry_test_utils::test_telemetry();

        let err = engine_service
            .connect_driver(&database, &endpoint_schema, &organization_uuid, telemetry_wrapper)
            .await
            .expect_err("health check failure should be returned");

        assert!(err.to_string().contains("health check failed with status"));

        let lock = engine_service.router.read().await;
        let http_ep = lock.get(&EpKind::Http).and_then(|ep| ep.as_any().downcast_ref::<HttpEp>()).expect("http router");

        assert!(
            !http_ep.pool().pool().contains_key(&endpoint_cache_key),
            "failed health check should remove the runtime pool entry"
        );
    }

    #[tokio::test]
    async fn reconnect_health_check_failure_preserves_existing_runtime_pool() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let healthy_server = MockServer::start_async().await;
        healthy_server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/health");
                then.status(200);
            })
            .await;

        let failing_server = MockServer::start_async().await;
        failing_server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/health");
                then.status(404);
            })
            .await;

        let healthy_config = HttpConfig {
            target: HttpTarget { url: healthy_server.base_url() },
            write_credentials: Some(HttpCredentials { headers: None }),
            ..Default::default()
        };
        let mut endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_http_reconnect_rollback".to_string()),
            EpKind::Http,
            healthy_config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );
        let organization_uuid = OrganizationUuid::new_uuid();
        let endpoint_cache_key = endpoint_schema.cache_key(OrganizationCacheUuid::from(organization_uuid.clone()));
        let database = lazy_database_manager().await;
        let engine_service = MyEngineService::default();
        let telemetry_wrapper = &mut crate::test_utils::telemetry_test_utils::test_telemetry();

        engine_service
            .connect_driver(&database, &endpoint_schema, &organization_uuid, telemetry_wrapper)
            .await
            .expect("initial health check should pass");

        let failing_config = HttpConfig {
            target: HttpTarget { url: failing_server.base_url() },
            write_credentials: Some(HttpCredentials { headers: None }),
            ..Default::default()
        };
        endpoint_schema.update_config(failing_config.as_config());

        let err = engine_service
            .reconnect(&database, &endpoint_schema, &organization_uuid, telemetry_wrapper)
            .await
            .expect_err("reconnect health check failure should be returned");

        assert!(err.to_string().contains("health check failed with status"));

        let lock = engine_service.router.read().await;
        let http_ep = lock.get(&EpKind::Http).and_then(|ep| ep.as_any().downcast_ref::<HttpEp>()).expect("http router");

        assert!(
            http_ep.pool().pool().contains_key(&endpoint_cache_key),
            "failed reconnect should preserve the old runtime pool entry"
        );
    }

    #[tokio::test]
    async fn validate_endpoint_runtime_config_does_not_insert_shared_router() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let server = MockServer::start_async().await;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/health");
                then.status(200);
            })
            .await;

        let config = HttpConfig {
            target: HttpTarget { url: server.base_url() },
            write_credentials: Some(HttpCredentials { headers: None }),
            ..Default::default()
        };
        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_http_validation".to_string()),
            EpKind::Http,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );
        let organization_uuid = OrganizationUuid::new_uuid();
        let database = lazy_database_manager().await;
        let engine_service = MyEngineService::default();
        let telemetry_wrapper = &mut crate::test_utils::telemetry_test_utils::test_telemetry();

        engine_service
            .validate_endpoint_runtime_config(&database, &endpoint_schema, &organization_uuid, telemetry_wrapper)
            .await
            .expect("validation health check should pass");

        let lock = engine_service.router.read().await;
        assert!(
            !lock.contains_key(&EpKind::Http),
            "isolated validation should not initialize or populate the shared runtime router"
        );
    }

    #[tokio::test]
    async fn validate_endpoint_runtime_config_health_failure_keeps_shared_runtime_pool() {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let healthy_server = MockServer::start_async().await;
        healthy_server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/health");
                then.status(200);
            })
            .await;

        let failing_server = MockServer::start_async().await;
        failing_server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/health");
                then.status(404);
            })
            .await;

        let healthy_config = HttpConfig {
            target: HttpTarget { url: healthy_server.base_url() },
            write_credentials: Some(HttpCredentials { headers: None }),
            ..Default::default()
        };
        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_http_validation_failure".to_string()),
            EpKind::Http,
            healthy_config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );
        let organization_uuid = OrganizationUuid::new_uuid();
        let endpoint_cache_key = endpoint_schema.cache_key(OrganizationCacheUuid::from(organization_uuid.clone()));
        let database = lazy_database_manager().await;
        let engine_service = MyEngineService::default();
        let telemetry_wrapper = &mut crate::test_utils::telemetry_test_utils::test_telemetry();

        engine_service
            .connect_driver(&database, &endpoint_schema, &organization_uuid, telemetry_wrapper)
            .await
            .expect("initial health check should pass");

        let failing_config = HttpConfig {
            target: HttpTarget { url: failing_server.base_url() },
            write_credentials: Some(HttpCredentials { headers: None }),
            ..Default::default()
        };
        let mut candidate_endpoint_schema = endpoint_schema.clone();
        candidate_endpoint_schema.update_config(failing_config.as_config());

        let err = engine_service
            .validate_endpoint_runtime_config(&database, &candidate_endpoint_schema, &organization_uuid, telemetry_wrapper)
            .await
            .expect_err("validation health check failure should be returned");

        assert!(err.to_string().contains("health check failed with status"));

        let lock = engine_service.router.read().await;
        let http_ep = lock.get(&EpKind::Http).and_then(|ep| ep.as_any().downcast_ref::<HttpEp>()).expect("http router");

        assert!(
            http_ep.pool().pool().contains_key(&endpoint_cache_key),
            "isolated validation failure should not remove the existing shared runtime pool entry"
        );
    }
}
#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod test_connection {
    use crate::test_utils::database_test_utils::{initialize_database, initialize_engine_service};
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    #[cfg(feature = "clickhouse")]
    use clickhouse_core::{config::ClickhouseConfig, connection::ClickhouseConnection};
    use database::endpoint_schema::endpoint::EndpointSchema;
    use database::methods::insert::endpoint::InsertEndpoint;
    use eden_core::format::{EdenId, EndpointId, UserUuid};
    use ep_core::database::schema::Table;
    use ep_core::ep::{EpConfig, EpConnection};
    #[cfg(feature = "mongo")]
    use mongo_core::{config::MongoConfig, connection::MongoConnection};
    #[cfg(feature = "mssql")]
    use mssql_core::{auth::MssqlAuth, config::MssqlConfig, connection::MssqlConnection};
    #[cfg(feature = "mysql")]
    use mysql_core::{config::MysqlConfig, connection::MysqlConnection};
    #[cfg(feature = "postgres")]
    use postgres_core::{config::PostgresConfig, connection::PostgresConnection};
    #[cfg(feature = "redis")]
    use redis_core::{config::RedisConfig, connection::RedisConnection};
    use testcontainers_modules::testcontainers::{ImageExt, runners::AsyncRunner};

    pub(crate) async fn connect<C: EpConfig>(config: &mut C, connection: Box<dyn EpConnection>) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let kind = connection.kind();

        assert!(config.update_write_conn(connection).is_ok());

        let endpoint_schema = EndpointSchema::new(
            EndpointId::new(format!("test_{}", kind.clone())),
            kind,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema, eden_node_schema.uuid());

        engine_service.connect(&db_manager, &insert_endpoint, test_telemetry).await.expect("Failed to connect to database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    async fn postgres_connection() {
        let container = testcontainers_modules::postgres::Postgres::default().start().await.expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(5432).await.expect("Failed to get host port");

        let connection = Box::new(PostgresConnection {
            url: format!("postgres://postgres:postgres@{host_ip}:{host_port}/postgres"),
            sslmode: None,
        });

        connect::<PostgresConfig>(&mut PostgresConfig::default(), connection.as_connection()).await;
    }

    #[tokio::test]
    async fn postgres_endpoint_health_check_fails() {
        let container = testcontainers_modules::postgres::Postgres::default().start().await.expect("Failed to start postgres");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(5432).await.expect("Failed to get host port");

        let (_redis_cont, _pg_cont, _ch_cont, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();
        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        // 1: Create connection while database is running
        let connection = Box::new(PostgresConnection {
            url: format!("postgresql://postgres:postgres@{host_ip}:{host_port}/postgres"),
            sslmode: None,
        });

        let ep_kind = connection.kind();

        let mut config = PostgresConfig::default();
        assert!(config.update_write_conn(connection).is_ok());

        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_postgres_stops".to_string()),
            ep_kind,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid());

        // 2: Connect successfully
        let result = engine_service.connect(&db_manager, &insert_endpoint, test_telemetry).await;

        assert!(result.is_ok(), "Connection should succeed with running database");

        // 3: Stop the database
        container.stop().await.expect("Failed to stop database");

        // 4. Try to connect again - health check should fail
        let insert_endpoint_2 = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema, eden_node_schema.uuid());

        let result = engine_service.connect(&db_manager, &insert_endpoint_2, test_telemetry).await;

        // Healh check should detect the database is down
        assert!(result.is_err(), "Connection should fail when database is stopped");
    }

    async fn http_endpoint_health_check_helper(status_code: u16, expect_success: bool, endpoint_id: &str) {
        use database::db::methods::insert::endpoint::InsertEndpoint;
        use database::endpoint_schema::endpoint::EndpointSchema;
        use eden_core::format::EndpointId;
        #[cfg(feature = "http")]
        use http_core::config::HttpConfig;
        #[cfg(feature = "http")]
        use http_core::connection::HttpConnection;
        use httpmock::MockServer;

        let (_redis_cont, _pg_cont, _ch_cont, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        // Create HTTPS mock server using httpmock
        // httpmock auto-generates self-signed certificates for HTTPS when https feature is enabled
        let server = MockServer::start_async().await;

        // Mock the /health endpoint with the specified status code
        let status = status_code;
        server
            .mock_async(|when, then| {
                when.method(httpmock::Method::GET).path("/health");
                then.status(status);
            })
            .await;

        let https_url = server.base_url();
        let connection = Box::new(HttpConnection { url: https_url, headers: None });

        let ep_kind = connection.kind();
        let mut config = HttpConfig::default();
        assert!(config.update_write_conn(connection).is_ok());

        let endpoint_schema = EndpointSchema::new(
            EndpointId::new(endpoint_id.to_string()),
            ep_kind,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema, eden_node_schema.uuid());

        // Test HTTP connection with health check
        let result = engine_service.connect(&db_manager, &insert_endpoint, test_telemetry).await;

        if expect_success {
            assert!(result.is_ok(), "Connection should succeed with {} response. Error: {:?}", status_code, result.err());
        } else {
            assert!(result.is_err(), "Connection should fail with {} response", status_code);
        }
    }

    #[tokio::test]
    async fn https_endpoint_health_check() {
        http_endpoint_health_check_helper(200, true, "test_http_endpoint").await;
        http_endpoint_health_check_helper(404, false, "test_http_endpoint_404").await;
    }

    #[tokio::test]
    async fn redis_connection() {
        use crate::test_utils::redis_test_utils::wait_for_redis_ready;
        use testcontainers_modules::testcontainers::{GenericImage, core::ContainerPort};

        let container = GenericImage::new("redis", "7.2.4")
            .with_exposed_port(ContainerPort::Tcp(6379))
            .start()
            .await
            .expect("Failed to start database");

        wait_for_redis_ready(&container).await;

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(6379).await.expect("Failed to get host port");

        let connection = Box::new(RedisConnection {
            host: host_ip.to_string(),
            port: Some(host_port),
            tls: None,
            insecure: None,
            db: None,
            username: None,
            password: None,
            protocol_version: None,
            connect_timeout_secs: None,
            max_retries: None,
        });

        connect::<RedisConfig>(&mut RedisConfig::default(), connection.as_connection()).await;
    }

    #[tokio::test]
    async fn clickhouse_connection() {
        let container = testcontainers_modules::clickhouse::ClickHouse::default().start().await.expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(8123).await.expect("Failed to get host port");

        let connection = Box::new(ClickhouseConnection {
            url: format!("http://{host_ip}:{host_port}"),
            user: None,
            compression: None,
            product_info: None,
            password: None,
            database: None,
            options: None,
            native_host: None,
            native_port: None,
            native_tls: None,
        });

        connect::<ClickhouseConfig>(&mut ClickhouseConfig::default(), connection.as_connection()).await;

        container.stop().await.expect("Failed to stop database");
    }

    #[tokio::test]
    async fn mongo_connection() {
        let container = testcontainers_modules::mongo::Mongo::default().start().await.expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(27017).await.expect("Failed to get host port");

        let connection = Box::new(MongoConnection {
            url: format!("mongodb://{host_ip}:{host_port}/?directConnection=true"),
            auth: None,
        });

        connect::<MongoConfig>(&mut MongoConfig::default(), connection.as_connection()).await;

        container.stop().await.expect("Failed to stop database");
    }

    #[tokio::test]
    async fn mysql_connection() {
        let container = testcontainers_modules::mysql::Mysql::default()
            .with_cmd(["--default-authentication-plugin=mysql_native_password"])
            .start()
            .await
            .expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(3306).await.expect("Failed to get host port");

        let connection = Box::new(MysqlConnection { url: format!("mysql://root@{host_ip}:{host_port}/test") });

        connect::<MysqlConfig>(&mut MysqlConfig::default(), connection.as_connection()).await;

        container.stop().await.expect("Failed to stop database");
    }

    #[tokio::test]
    async fn mssql_connection() {
        let container = testcontainers_modules::mssql_server::MssqlServer::default()
            .with_accept_eula()
            .start()
            .await
            .expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(1433).await.expect("Failed to get host port");

        let connection = Box::new(MssqlConnection {
            url: format!(
                "Server=tcp:{host_ip},{host_port};Database=master;User Id=sa;Password=yourStrong(!)Password;TrustServerCertificate=True;",
            ),
            auth: MssqlAuth {
                username: "sa".to_string(),
                password: "yourStrong(!)Password".to_string(),
            },
        });

        connect::<MssqlConfig>(&mut MssqlConfig::default(), connection.as_connection()).await;

        container.stop().await.expect("Failed to stop database");
    }

    #[tokio::test]
    async fn llm_health_check() {
        use httpmock::MockServer;
        #[cfg(feature = "llm")]
        use llm_core::config::LlmConfig;
        #[cfg(feature = "llm")]
        use llm_core::connection::{LlmConnection, LlmConnectionDefaults, LlmTarget};

        let (_redis_cont, _pg_cont, _ch_cont, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        // Start mock server on localhost
        let server = MockServer::start_async().await;

        // Mock the LLM API endpoint with a successful response
        let mock = server
            .mock_async(|when, then| {
                when.method(httpmock::Method::POST).path("/chat/completions");
                then.status(200).header("content-type", "application/json").body(
                    r#"{
                            "choices": [
                                {
                                    "message": {
                                        "role": "assistant",
                                        "content": "pong"
                                    }
                                }
                            ]
                        }"#,
                );
            })
            .await;

        // Create LLM connection pointing to the mock server
        let mock_url = format!("http://{}", server.address());
        let connection = Box::new(LlmConnection {
            target: LlmTarget::OpenAI {
                defaults: LlmConnectionDefaults {
                    model: "gpt-4".to_string(),
                    temperature: None,
                    max_tokens: Some(1),
                    base_url_override: Some(mock_url.clone()),
                    ..Default::default()
                },
            },
            credential_id: None,
            inline_api_key: Some("test-api-key".to_string()),
        });

        let ep_kind = connection.kind();
        let mut config = LlmConfig::default();
        assert!(config.update_write_conn(connection).is_ok());

        let endpoint_schema = EndpointSchema::new(
            EndpointId::new("test_llm_https".to_string()),
            ep_kind,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema, eden_node_schema.uuid());

        // Test LLM connection with health check - should succeed
        let result = engine_service.connect(&db_manager, &insert_endpoint, test_telemetry).await;

        assert!(result.is_ok(), "Connection should succeed with mock HTTPS server. Error: {:?}", result.err());

        // Verify the mock was called at least once (health check)
        assert!(mock.calls() >= 1, "Should have sent at least one request to the mock server for health check");
    }
}
