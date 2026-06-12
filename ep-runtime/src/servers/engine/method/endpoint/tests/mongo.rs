mod read;

#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod mongo_endpoint {
    use crate::comp::MyEngineService;
    use crate::test_utils::database_test_utils::{initialize_database, initialize_engine_service};
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use borsh::BorshSerialize;
    use database::endpoint_schema::endpoint::EndpointSchema;
    use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
    use database::methods::insert::endpoint::InsertEndpoint;
    use eden_core::format::{CacheUuid, EdenId, EdenNodeUuid, EndpointId, EndpointUuid, OrganizationCacheUuid, OrganizationUuid, UserUuid};
    use eden_core::telemetry::TelemetryWrapper;
    use endpoint::EpRequest;
    #[cfg(feature = "mongo")]
    use endpoint::mongo::api::lib::database::collection::InsertOneInput;
    #[cfg(feature = "mongo")]
    use endpoint::mongo::api::wrapper::DocumentWrapper;
    #[cfg(feature = "mongo")]
    use endpoint::mongo::request::MongoRequest;
    use ep_core::database::schema::Table;
    use ep_core::ep::{EpConfig, EpConnection};
    use ep_core::settings::EdenSettings;
    #[cfg(feature = "mongo")]
    use mongo_core::config::MongoConfig;
    #[cfg(feature = "mongo")]
    use mongo_core::connection::MongoConnection;
    use mongodb::bson;
    use testcontainers_modules::mongo::Mongo;
    use testcontainers_modules::testcontainers::ContainerAsync;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    type RedisTestContainer = crate::test_utils::database_test_utils::TestContainer;

    type PostgresTestContainer = crate::test_utils::database_test_utils::TestContainer;

    #[allow(dead_code)]
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

    #[allow(dead_code)]
    pub async fn initialize_mongo() -> (
        MyEngineService,
        TelemetryWrapper,
        OrganizationCacheUuid,
        EndpointUuid,
        ContainerAsync<Mongo>,
        RedisTestContainer,
        PostgresTestContainer,
    ) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

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

        let kind = connection.kind();

        let mut config: MongoConfig = MongoConfig::default();

        assert!(config.update_write_conn(connection).is_ok());

        let endpoint_schema = EndpointSchema::new(
            EndpointId::new(format!("test_{}", kind.clone())),
            kind,
            config.as_config(),
            None,
            None,
            UserUuid::new_uuid(),
        );

        let insert_endpoint = InsertEndpoint::new(organization_schema.uuid(), endpoint_schema.clone(), eden_node_schema.uuid());

        engine_service.connect(&db_manager, &insert_endpoint, test_telemetry).await.expect("Failed to connect to database");

        let org_cache_uuid = OrganizationCacheUuid::new(None, organization_schema.uuid());

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

        (
            engine_service,
            test_telemetry.clone(),
            org_cache_uuid,
            endpoint_schema.uuid(),
            container,
            redis_container,
            pg_container,
        )
    }
}
