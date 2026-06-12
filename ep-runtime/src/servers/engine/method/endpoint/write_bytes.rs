use std::borrow::Cow;

use bytes::Bytes;
use database::endpoint_schema::endpoint::EndpointSchema;
use eden_core::error::{ConnectError, EpError};
use eden_core::format::OrganizationCacheUuid;
use eden_core::macros::execute_with_timeout;
use eden_core::telemetry::{FastSpanAttribute, FastSpanStatus, TelemetryWrapper};
use endpoint::EpRequest;
use ep_core::settings::EdenSettings;
use function_name::named;
use tokio::time::Duration;

use crate::comp::MyEngineService;

impl MyEngineService {
    #[named]
    async fn write_bytes_with_reconnect(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Bytes, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        let endpoint_cache_key = endpoint_schema.cache_key(organization_cache_uuid);
        let kind = endpoint_schema.kind();

        let lock = self.router.read().await;
        let ep = match lock.get(&kind) {
            Some(route) => route,
            None => {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned("could not get endpoint".to_string()) });
                return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint));
            }
        };

        span.add_simple_event("processing async write bytes");

        let result = execute_with_timeout!(
            span,
            telemetry_wrapper,
            settings,
            ep,
            write_bytes_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
        );

        drop(lock);
        span.add_simple_event("dropped lock");

        if let Err(EpError::Connect(e)) = result {
            span.add_event("connection error, attempting to reconnect", vec![FastSpanAttribute::new("error", e.to_string())]);

            let mut lock = self.router.write().await;
            let ep = match lock.get_mut(&kind) {
                Some(route) => route,
                None => {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned("could not get endpoint".to_string()) });
                    return Err(EpError::Connect(ConnectError::CouldNotGetEndpoint));
                }
            };

            ep.reconnect_boxed(&endpoint_cache_key, endpoint_schema.config(), telemetry_wrapper).await?;

            span.add_simple_event("reconnected! sending write bytes again");

            execute_with_timeout!(
                span,
                telemetry_wrapper,
                settings,
                ep,
                write_bytes_boxed(&endpoint_cache_key, request, settings, telemetry_wrapper)
            )
        } else {
            result
        }
    }
}

impl MyEngineService {
    #[named]
    pub async fn write_bytes(
        &self,
        request: &mut dyn EpRequest,
        endpoint_schema: &EndpointSchema,
        organization_cache_uuid: OrganizationCacheUuid,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Bytes, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

        span.add_simple_event("processing endpoint write bytes...");
        span.add_event(format!("{} write bytes", endpoint_schema.kind()), vec![]);

        self.write_bytes_with_reconnect(request, endpoint_schema, organization_cache_uuid, settings, telemetry_wrapper)
            .await
            .inspect_err(|e: &EpError| span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) }))
    }
}

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod write_bytes_endpoint {
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
            GetInputBuilder::default().key(RedisKey::String("x".into())).build().expect("failed to build redis request"),
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
    async fn redis_write_bytes_version5() {
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
        let rows = output
            .as_object()
            .expect("Expected object to be object")
            .get("rows")
            .unwrap_or_default()
            .as_array()
            .expect("Expected array to have values");
        let obj = rows[0].as_object().expect("Expected object to be object");
        // println!("{:?}", obj);
        assert_eq!(
            obj.get("n * 2").unwrap_or_default().as_number().expect("Expected number to have values"),
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
                .expect("Expect array")[0]
                .as_array()
                .expect("Expect array")[0]
                .as_array()
                .expect("Expect array")[0]
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
            output.get("dimension").expect("Missing dimension").as_number().expect("Expect number"),
            &Number::from(1536)
        );

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
