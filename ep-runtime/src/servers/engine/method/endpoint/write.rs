use std::borrow::Cow;

use database::endpoint_schema::endpoint::EndpointSchema;
use eden_core::error::{ConnectError, EpError};
use eden_core::format::OrganizationCacheUuid;
#[cfg(any(feature = "mongo", feature = "redis", feature = "postgres"))]
use eden_core::format::endpoint::EpKind;
use eden_core::macros::execute_with_timeout;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus, TelemetryWrapper};
use endpoint::EpRequest;
use ep_core::ep::EpConnection;
use ep_core::settings::EdenSettings;
use function_name::named;
use serde_json::Value;
use tokio::time::Duration;

#[cfg(any(feature = "mongo", feature = "redis", feature = "postgres"))]
use super::analytics;
use crate::comp::MyEngineService;

enum WriteDispatch {
    Pooled { organization_cache_uuid: OrganizationCacheUuid },
    Els { els_conn: Box<dyn EpConnection> },
}

impl MyEngineService {
    #[named]
    async fn write_with_reconnect(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        let endpoint_cache_key = endpoint_schema.cache_key(organization_cache_uuid);
        let kind = endpoint_schema.kind();

        let lock = self.router.read().await;
        let ep = match lock.get(&kind) {
            Some(route) => route,
            None => return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint)),
        };

        span.add_simple_event("processing async execute");

        let result = execute_with_timeout!(
            span,
            telemetry_wrapper,
            settings,
            ep,
            write_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
        );

        drop(lock);
        span.add_simple_event("dropped lock");

        if let Err(EpError::Connect(e)) = result {
            span.add_event("connection error, attempting to reconnect", vec![FastSpanAttribute::new("error", e.to_string())]);

            let mut lock = self.router.write().await;
            let ep = match lock.get_mut(&kind) {
                Some(route) => route,
                None => return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint)),
            };

            ep.reconnect_boxed(&endpoint_cache_key, endpoint_schema.config(), telemetry_wrapper).await?;

            span.add_simple_event("reconnected! sending execute again");

            execute_with_timeout!(
                span,
                telemetry_wrapper,
                settings,
                ep,
                write_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
            )
        } else {
            result
        }
    }

    #[named]
    async fn write_els_with_conn(
        &self,
        els_conn: Box<dyn EpConnection>,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        let kind = endpoint_schema.kind();

        let lock = self.router.read().await;
        let ep = match lock.get(&kind) {
            Some(route) => route,
            None => return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint)),
        };

        span.add_simple_event("processing ELS write with override connection");
        let result = ep.write_with_conn_boxed(els_conn, endpoint_schema.config(), request, settings, telemetry_wrapper).await;

        drop(lock);
        result
    }

    async fn dispatch_write_endpoint_result(
        &self,
        dispatch: WriteDispatch,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        match dispatch {
            WriteDispatch::Pooled { organization_cache_uuid } => {
                self.write_with_reconnect(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            WriteDispatch::Els { els_conn } => {
                self.write_els_with_conn(els_conn, request, endpoint_schema, settings, telemetry_wrapper).await
            }
        }
    }
}

impl MyEngineService {
    #[named]
    pub async fn write(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

        span.add_simple_event("processing endpoint write...");
        span.add_event(format!("{} execute", endpoint_schema.kind()), vec![]);

        match endpoint_schema.kind() {
            #[cfg(feature = "mongo")]
            EpKind::Mongo => {
                self.write_with_mongo_analytics(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            #[cfg(feature = "redis")]
            EpKind::Redis => {
                self.write_with_redis_analytics(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            #[cfg(feature = "postgres")]
            EpKind::Postgres => {
                self.write_with_postgres_analytics(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await
            }
            _ => {
                self.dispatch_write_endpoint_result(
                    WriteDispatch::Pooled { organization_cache_uuid },
                    request,
                    endpoint_schema,
                    settings,
                    telemetry_wrapper,
                )
                .await
            }
        }
        .inspect_err(|e: &EpError| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
    }

    #[named]
    pub async fn write_els(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        els_conn: Option<Box<dyn EpConnection>>,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let els_conn = match els_conn {
            Some(conn) => conn,
            None => return self.write(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper).await,
        };

        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        span.add_event(format!("{} ELS write", endpoint_schema.kind()), vec![]);

        let result = self
            .dispatch_write_endpoint_result(WriteDispatch::Els { els_conn }, request, endpoint_schema, settings, telemetry_wrapper)
            .await;

        result.inspect_err(|e: &EpError| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
    }

    #[cfg(feature = "mongo")]
    async fn write_with_mongo_analytics(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let start = std::time::Instant::now();
        let facts = analytics::extract_mongo_request_facts(request);
        let endpoint_uuid = endpoint_schema.endpoint_uuid();
        let organization_uuid = organization_cache_uuid.to_string();

        let result = self
            .dispatch_write_endpoint_result(
                WriteDispatch::Pooled { organization_cache_uuid },
                request,
                endpoint_schema,
                settings,
                telemetry_wrapper,
            )
            .await;

        if let Some(facts) = facts {
            let latency_us = start.elapsed().as_micros() as u64;
            let response_facts = result.as_ref().ok().map(analytics::extract_response_facts);
            let response_bytes = result
                .as_ref()
                .ok()
                .and_then(|value| serde_json::to_vec(value).ok())
                .map(|bytes| analytics::usize_to_u32(bytes.len()))
                .unwrap_or(0);

            analytics::record_mongo_operation(
                &endpoint_uuid,
                &organization_uuid,
                &facts,
                response_facts.as_ref(),
                latency_us,
                result.is_err(),
                response_bytes,
                telemetry_wrapper.labels().user_uuid(),
            );
        }

        result
    }

    #[cfg(feature = "redis")]
    async fn write_with_redis_analytics(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let start = std::time::Instant::now();
        let facts = analytics::extract_redis_request_facts(request);
        let endpoint_uuid = endpoint_schema.endpoint_uuid();
        let organization_uuid = organization_cache_uuid.to_string();

        let result = self
            .dispatch_write_endpoint_result(
                WriteDispatch::Pooled { organization_cache_uuid },
                request,
                endpoint_schema,
                settings,
                telemetry_wrapper,
            )
            .await;

        if let Some(facts) = facts {
            let latency_us = start.elapsed().as_micros() as u64;
            let response_bytes = result
                .as_ref()
                .ok()
                .and_then(|value| serde_json::to_vec(value).ok())
                .map(|bytes| analytics::usize_to_u32(bytes.len()))
                .unwrap_or(0);

            analytics::record_redis_operation(
                &endpoint_uuid,
                &organization_uuid,
                &facts,
                latency_us,
                result.is_err(),
                response_bytes,
                telemetry_wrapper.labels().user_uuid(),
            );
        }

        result
    }

    #[cfg(feature = "postgres")]
    async fn write_with_postgres_analytics(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let start = std::time::Instant::now();
        let facts = analytics::extract_postgres_request_facts(request);
        let endpoint_uuid = endpoint_schema.endpoint_uuid();
        let organization_uuid = organization_cache_uuid.to_string();

        let result = self
            .dispatch_write_endpoint_result(
                WriteDispatch::Pooled { organization_cache_uuid },
                request,
                endpoint_schema,
                settings,
                telemetry_wrapper,
            )
            .await;

        if let Some(facts) = facts {
            let latency_us = start.elapsed().as_micros() as u64;
            let response_bytes = result
                .as_ref()
                .ok()
                .and_then(|value| serde_json::to_vec(value).ok())
                .map(|bytes| analytics::usize_to_u32(bytes.len()))
                .unwrap_or(0);

            analytics::record_postgres_operation(
                &endpoint_uuid,
                &organization_uuid,
                &facts,
                latency_us,
                result.is_err(),
                response_bytes,
                telemetry_wrapper.labels().user_uuid(),
            );
        }

        result
    }
}

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod write_endpoint {
    use crate::comp::MyEngineService;
    use crate::test_utils::database_test_utils::{initialize_database, initialize_engine_service};
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use borsh::BorshSerialize;
    use database::endpoint_schema::endpoint::EndpointSchema;
    use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use database::methods::insert::endpoint::InsertEndpoint;
    use eden_core::format::{EdenId, EdenNodeUuid, EndpointId, OrganizationCacheUuid, OrganizationUuid, UserUuid};
    use eden_core::telemetry::TelemetryWrapper;
    use endpoint::EpRequest;
    #[cfg(feature = "clickhouse")]
    use endpoint::clickhouse::api::lib::{QueryInputBuilder, QueryReadOnlyInputBuilder};
    #[cfg(feature = "redis")]
    use endpoint::ep_redis::api::key::RedisKey;
    #[cfg(feature = "mongo")]
    use endpoint::mongo::{
        api::lib::database::collection::{FindOneInput, InsertOneInput},
        api::wrapper::DocumentWrapper,
        request::MongoRequest,
    };
    use ep_core::database::schema::Table;
    use ep_core::ep::{EpConfig, EpConnection};
    use mongodb::bson;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    pub(crate) async fn connect<C: EpConfig + BorshSerialize>(
        db_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        test_telemetry: &mut TelemetryWrapper,
        engine_service: MyEngineService,
        config: &mut C,
        connection: Box<dyn EpConnection>,
        organization_uuid: OrganizationUuid,
        eden_node_uuid: EdenNodeUuid,
    ) -> EndpointSchema {
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

        let insert_endpoint = InsertEndpoint::new(organization_uuid, endpoint_schema.clone(), eden_node_uuid);

        engine_service.connect(db_manager, &insert_endpoint, test_telemetry).await.expect("Failed to connect to database");

        endpoint_schema
    }

    #[tokio::test]
    async fn mongo_write() {
        use {
            ep_core::settings::EdenSettings,
            mongo_core::{config::MongoConfig, connection::MongoConnection},
        };

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let container = testcontainers_modules::mongo::Mongo::default().start().await.expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(27017).await.expect("Failed to get host port");

        let connection = Box::new(MongoConnection {
            url: format!("mongodb://{host_ip}:{host_port}/?directConnection=true"),
            auth: None,
        });

        let endpoint_schema = connect::<MongoConfig>(
            &db_manager,
            test_telemetry,
            engine_service.clone(),
            &mut MongoConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let mut write = Box::new(MongoRequest(Box::new(InsertOneInput::new(
            "some_db".to_string(),
            "some-coll".to_string(),
            DocumentWrapper::from(bson::doc! { "x": 42}),
            None,
        ))))
        .as_request();

        engine_service
            .write(
                &mut *write,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        let mut read = Box::new(MongoRequest(Box::new(FindOneInput::new(
            "some_db".to_string(),
            "some-coll".to_string(),
            Some(DocumentWrapper::from(bson::doc! { "x": 42})),
            None,
        ))))
        .as_request();

        let _output = engine_service
            .write(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        // println!("{:?}", output);

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    #[serial]
    async fn pg_write() {
        #[cfg(feature = "postgres")]
        use endpoint::postgres::{
            api::lib::{query::QueryInputBuilder, query_read_only::QueryReadOnlyInputBuilder},
            request::PostgresRequest,
        };
        use testcontainers_modules::testcontainers::{ImageExt, core::ContainerPort};
        use {
            ep_core::settings::EdenSettings,
            postgres_core::{PostgresConfig, connection::PostgresConnection},
        };

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let container = testcontainers_modules::postgres::Postgres::default()
            .with_mapped_port(5433, ContainerPort::Tcp(5432))
            .start()
            .await
            .expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");

        let connection = Box::new(PostgresConnection {
            url: format!("postgresql://postgres:postgres@{host_ip}:5433"),
            sslmode: None,
        });

        let endpoint_schema = connect::<PostgresConfig>(
            &db_manager,
            test_telemetry,
            engine_service.clone(),
            &mut PostgresConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let mut write = Box::new(PostgresRequest(Box::new(
            QueryInputBuilder::default().query("SELECT 1+1").params(vec![]).build().unwrap_or_default(),
        )))
        .as_request();

        engine_service
            .write(
                &mut *write,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        let mut read = Box::new(PostgresRequest(Box::new(
            QueryReadOnlyInputBuilder::default().query("SELECT 1+1").params(vec![]).build().unwrap_or_default(),
        )))
        .as_request();

        let _output = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        // println!("{:?}", output);

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    async fn redis_write() {
        use crate::test_utils::redis_test_utils::wait_for_redis_ready;
        #[cfg(feature = "redis")]
        use endpoint::ep_redis::{
            api::{GetInputBuilder, RedisJsonValue, SetInputBuilder},
            request::RedisRequest,
        };
        use testcontainers_modules::testcontainers::{GenericImage, ImageExt, core::ContainerPort};
        use {
            ep_core::settings::EdenSettings,
            redis_core::{config::RedisConfig, connection::RedisConnection},
        };

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let container = GenericImage::new("redis", "7.2.4")
            .with_mapped_port(6378, ContainerPort::Tcp(6379))
            .start()
            .await
            .expect("Failed to start database");

        wait_for_redis_ready(&container).await;

        let host_ip = container.get_host().await.expect("Failed to get host");

        let connection = Box::new(RedisConnection {
            host: host_ip.to_string(),
            port: Some(6378),
            tls: None,
            insecure: None,
            db: None,
            username: None,
            password: None,
            protocol_version: None,
            connect_timeout_secs: None,
            max_retries: None,
        });

        let endpoint_schema = connect::<RedisConfig>(
            &db_manager,
            test_telemetry,
            engine_service.clone(),
            &mut RedisConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let mut write = Box::new(RedisRequest(Box::new(
            SetInputBuilder::default()
                .key(RedisKey::String("x".into()))
                .value(RedisJsonValue::Integer(42))
                .rule(None)
                .get(None)
                .options(None)
                .build()
                .unwrap_or_default(),
        )))
        .as_request();

        engine_service
            .write(
                &mut *write,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        let mut read = Box::new(RedisRequest(Box::new(
            GetInputBuilder::default().key(RedisKey::String("x".into())).build().expect("Failed to build get input"),
        )))
        .as_request();

        let _output = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        // println!("{:?}", output);

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    async fn redis_write_version5() {
        // TODO: fix the bug that we can't connect to Redis 5
        // (probably other versions should be tested)
        // HELLO cmd doesn't work
    }

    #[tokio::test]
    async fn cassandra_write() {
        use std::time::Duration;

        #[cfg(feature = "cassandra")]
        use endpoint::cassandra::{api::lib::QuerySinglePageInputBuilder, request::CassandraRequest};
        use testcontainers_modules::testcontainers::{GenericImage, ImageExt, core::ContainerPort};
        use {
            cassandra_core::{config::CassandraConfig, connection::CassandraConnection},
            ep_core::settings::EdenSettings,
        };

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let container = GenericImage::new("cassandra", "5.0.3")
            .with_mapped_port(9042, ContainerPort::Tcp(9042))
            .start()
            .await
            .expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let connection = Box::new(CassandraConnection {
            known_nodes: vec![format!("{host_ip}:9042")],
            timeout: Some(10000),
            ..Default::default()
        });

        // wait for Cassandra pool to get up
        println!("Waiting for Cassandra to become available...60 seconds");
        tokio::time::sleep(Duration::from_secs(60)).await;

        let endpoint_schema = connect::<CassandraConfig>(
            &db_manager,
            test_telemetry,
            engine_service.clone(),
            &mut CassandraConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let cmds = vec![
            "CREATE KEYSPACE Eden WITH replication={'class': 'SimpleStrategy', 'replication_factor' : 1};",
            "CREATE TABLE Eden.x(n int, name text, created_at timeuuid primary key, ticks list<timestamp>);",
            "INSERT INTO Eden.x(n, name, created_at, ticks) VALUES(42, 'Answer', now(), [toTimestamp(now()), toTimestamp(now())]);",
        ];
        for &cmd in &cmds {
            let mut write = Box::new(CassandraRequest(Box::new(
                QuerySinglePageInputBuilder::default().query(cmd).build().unwrap_or_default(),
            )))
            .as_request();

            engine_service
                .write(
                    &mut *write,
                    &endpoint_schema,
                    OrganizationCacheUuid::from(organization_schema.uuid()),
                    EdenSettings::default(),
                    test_telemetry,
                )
                .await
                .expect("Failed to write data");
        }
        let mut read = Box::new(CassandraRequest(Box::new(
            QuerySinglePageInputBuilder::default()
                .query("SELECT n*2, name, created_at, toTimestamp(created_at), ticks FROM Eden.x;")
                .build()
                .unwrap_or_default(),
        )))
        .as_request();

        let output = engine_service
            .write(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");
        // println!("{}", serde_json::to_string(&output).unwrap_or_default());
        let rows = output.as_object().expect("Failed object").get("rows").unwrap_or_default().as_array().expect("Failed array");
        let obj = rows[0].as_object().expect("Failed object");
        // println!("{:?}", obj);
        assert_eq!(
            obj.get("n * 2").unwrap_or_default().as_number().expect("Failed number"),
            &serde_json::Number::from(84)
        );
        assert_eq!(obj.get("name").unwrap_or_default().as_str().unwrap_or_default(), "Answer");

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    async fn clickhouse_write() {
        #[cfg(feature = "clickhouse")]
        use endpoint::clickhouse::request::ClickhouseRequest;
        use {
            clickhouse_core::{config::ClickhouseConfig, connection::ClickhouseConnection},
            ep_core::settings::EdenSettings,
        };

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let container = testcontainers_modules::clickhouse::ClickHouse::default().start().await.expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(8123).await.expect("Failed to get port");
        let connection = Box::new(ClickhouseConnection {
            url: format!("http://{host_ip}:{host_port}"),
            ..Default::default()
        });

        let endpoint_schema = connect::<ClickhouseConfig>(
            &db_manager,
            test_telemetry,
            engine_service.clone(),
            &mut ClickhouseConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let mut write = Box::new(ClickhouseRequest(Box::new(
            QueryInputBuilder::default()
                .query("SELECT version()")
                .binds(vec![])
                .params(vec![])
                .build()
                .expect("Failed to build query input"),
        )))
        .as_request();

        engine_service
            .write(
                &mut *write,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        let mut read = Box::new(ClickhouseRequest(Box::new(
            QueryReadOnlyInputBuilder::default().query("SELECT version()").binds(vec![]).params(vec![]).build().unwrap_or_default(),
        )))
        .as_request();

        let output = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");
        // println!("{}", serde_json::to_string(&output).unwrap_or_default());
        assert_eq!(
            output
                .as_object()
                .expect("Expected object to be object")
                .get("data")
                .unwrap_or_default()
                .as_array()
                .expect("Failed array")[0]
                .as_array()
                .expect("Failed array")[0]
                .as_array()
                .expect("Failed array")[0]
                .as_str()
                .unwrap_or_default(),
            "version()"
        );

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    async fn pinecone_write() {
        #[cfg(feature = "pinecone")]
        use endpoint::pinecone::{api::lib::DescribeIndexStatsInput, request::PineconeRequest};
        use serde_json::Number;
        use testcontainers_modules::testcontainers::{GenericImage, core::ContainerPort};
        use {
            ep_core::settings::EdenSettings,
            pinecone_core::{config::PineconeConfig, connection::PineconeConnection},
        };

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let container = GenericImage::new("ghcr.io/pinecone-io/pinecone-index", "latest")
            .with_exposed_port(ContainerPort::Tcp(5081))
            .start()
            .await
            .expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(5081).await.expect("Failed to get port");
        let connection = Box::new(PineconeConnection {
            url: format!("http://{host_ip}:{host_port}"),
            ..Default::default()
        });

        let endpoint_schema = connect::<PineconeConfig>(
            &db_manager,
            test_telemetry,
            engine_service.clone(),
            &mut PineconeConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        let mut write = Box::new(PineconeRequest(Box::new(DescribeIndexStatsInput::default()))).as_request();

        engine_service
            .write(
                &mut *write,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        let mut read = Box::new(PineconeRequest(Box::new(DescribeIndexStatsInput::default()))).as_request();

        let output = engine_service
            .write(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to read data");
        // println!("{:?}", output);
        assert_eq!(
            output.get("dimension").expect("Missing dimension").as_number().expect("Failed number"),
            &Number::from(1536)
        );

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    async fn redis_read_on_write_endpoint() {
        use crate::test_utils::redis_test_utils::wait_for_redis_ready;
        #[cfg(feature = "redis")]
        use endpoint::ep_redis::{
            api::{GetInputBuilder, RedisJsonValue, SetInputBuilder},
            request::RedisRequest,
        };
        use testcontainers_modules::testcontainers::{GenericImage, ImageExt, core::ContainerPort};
        use {
            ep_core::settings::EdenSettings,
            redis_core::{config::RedisConfig, connection::RedisConnection},
        };

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let container = GenericImage::new("redis", "7.2.4")
            .with_mapped_port(6378, ContainerPort::Tcp(6379))
            .start()
            .await
            .expect("Failed to start database");

        wait_for_redis_ready(&container).await;

        let host_ip = container.get_host().await.expect("Failed to get host");

        let connection = Box::new(RedisConnection {
            host: host_ip.to_string(),
            port: Some(6378),
            tls: None,
            insecure: None,
            db: None,
            username: None,
            password: None,
            protocol_version: None,
            connect_timeout_secs: None,
            max_retries: None,
        });

        let endpoint_schema = connect::<RedisConfig>(
            &db_manager,
            test_telemetry,
            engine_service.clone(),
            &mut RedisConfig::default(),
            connection.as_connection(),
            organization_schema.uuid(),
            eden_node_schema.uuid(),
        )
        .await;

        // First, set a key using the write endpoint
        let mut set_request = Box::new(RedisRequest(Box::new(
            SetInputBuilder::default()
                .key(RedisKey::String("test_key".into()))
                .value(RedisJsonValue::Integer(123))
                .rule(None)
                .get(None)
                .options(None)
                .build()
                .unwrap_or_default(),
        )))
        .as_request();

        engine_service
            .write(
                &mut *set_request,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to set key");

        let mut get_request = Box::new(RedisRequest(Box::new(
            GetInputBuilder::default().key(RedisKey::String("test_key".into())).build().unwrap(),
        )))
        .as_request();

        let result = engine_service
            .write(
                &mut *get_request,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to get key via write endpoint");

        // Verify the operation succeeded and we got the correct value back
        if let serde_json::Value::Object(ref map) = result {
            assert!(map.contains_key("kind"));
            assert_eq!(map.get("kind").and_then(|k| k.as_str()), Some("redis"));

            // Parse the Redis response to verify we got "123"
            // Response format: {"kind": "redis", "data": {"Resp3": {"0": [bytes]}}}
            if let Some(data) = map.get("data").and_then(|d| d.as_object())
                && let Some(resp3) = data.get("Resp3").and_then(|r| r.as_object())
                && let Some(bytes_arr) = resp3.get("0").and_then(|b| b.as_array())
            {
                // Convert JSON array of u8 values to String
                let bytes: Vec<u8> = bytes_arr.iter().filter_map(|v| v.as_u64().map(|n| n as u8)).collect();
                let value = String::from_utf8(bytes).expect("Invalid UTF-8 in response");
                assert_eq!(value, "123", "Expected to read back the value '123' that was set");
            }
        } else {
            panic!("Expected JSON object response, got: {:?}", result);
        }

        container.stop().await.expect("Failed to stop database");

        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
