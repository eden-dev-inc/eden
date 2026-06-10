#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod read_tests {
    use crate::test_utils::database_test_utils::{initialize_database, initialize_engine_service};
    use crate::test_utils::telemetry_test_utils::test_telemetry;
    use database::endpoint_schema::endpoint::EndpointSchema;
    use database::methods::insert::endpoint::InsertEndpoint;
    use eden_core::format::{CacheUuid, EdenId, EndpointId, OrganizationCacheUuid, UserUuid};
    #[cfg(feature = "mongo")]
    use endpoint::mongo::api::lib::{
        MongoApi,
        database::collection::{FindOneInput, InsertOneInput},
    };
    #[cfg(feature = "mongo")]
    use endpoint::mongo::api::wrapper::DocumentWrapper;
    #[cfg(feature = "mongo")]
    use endpoint::mongo::request::MongoRequest;
    use endpoint::{EpRequest, Operation};
    use ep_core::database::schema::Table;
    use ep_core::ep::{EpConfig, EpConnection};
    use ep_core::settings::EdenSettings;
    #[cfg(feature = "mongo")]
    use mongo_core::config::MongoConfig;
    #[cfg(feature = "mongo")]
    use mongo_core::connection::MongoConnection;
    #[cfg(feature = "mongo")]
    use mongo_core::{MongoAsync, MongoTx};
    use mongodb::bson;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;

    pub async fn mongo_test<T: Operation<MongoAsync, MongoApi, MongoTx>>(mongo_request: T, is_read: bool) -> serde_json::Value {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let (_user_schema, eden_node_schema, organization_schema) = initialize_database(&db_manager, test_telemetry).await;

        let mongo_container = testcontainers_modules::mongo::Mongo::default().start().await.expect("Failed to start database");

        let host_ip = mongo_container.get_host().await.expect("Failed to get host");
        let host_port = mongo_container.get_host_port_ipv4(27017).await.expect("Failed to get host port");

        let connection = Box::new(MongoConnection {
            url: format!("mongodb://{host_ip}:{host_port}/?directConnection=true"),
            auth: None,
        });

        let kind = connection.kind();

        let mut config: MongoConfig = MongoConfig::default();

        // For read operations, we need both read and write connections
        // For write operations, we only need write connection
        if is_read {
            assert!(config.update_read_conn(connection.clone()).is_ok());
        }
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

        let _org_cache_uuid = OrganizationCacheUuid::new(None, organization_schema.uuid());

        // Initial write to set up data
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
            .expect("Failed to write initial data");

        // Perform the actual test operation
        let mut read = Box::new(MongoRequest(Box::new(mongo_request))).as_request();

        let output = if is_read {
            engine_service
                .read(
                    &mut *read,
                    &endpoint_schema,
                    OrganizationCacheUuid::from(organization_schema.uuid()),
                    EdenSettings::default(),
                    test_telemetry,
                )
                .await
        } else {
            engine_service
                .write(
                    &mut *read,
                    &endpoint_schema,
                    OrganizationCacheUuid::from(organization_schema.uuid()),
                    EdenSettings::default(),
                    test_telemetry,
                )
                .await
        }
        .expect("Operation failed");

        // Teardown
        mongo_container.stop().await.expect("Failed to stop database");

        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");

        output
    }

    // Helper functions to maintain backward compatibility
    pub async fn read_mongo_test<T: Operation<MongoAsync, MongoApi, MongoTx>>(mongo_request: T) -> serde_json::Value {
        mongo_test(mongo_request, true).await
    }

    #[allow(dead_code)]
    pub async fn write_mongo_test<T: Operation<MongoAsync, MongoApi, MongoTx>>(mongo_request: T) -> serde_json::Value {
        mongo_test(mongo_request, false).await
    }

    #[tokio::test]
    #[serial]
    async fn database_collection_find() {
        let response = read_mongo_test(FindOneInput::new(
            "some_db".to_string(),
            "some-coll".to_string(),
            Some(DocumentWrapper::from(bson::doc! { "x": 42})),
            None,
        ))
        .await;

        println!("{:?}", response);
    }

    #[tokio::test]
    #[serial]
    async fn database_collection_find_one() {
        let response = read_mongo_test(FindOneInput::new(
            "some_db".to_string(),
            "some-coll".to_string(),
            Some(DocumentWrapper::from(bson::doc! { "x": 42})),
            None,
        ))
        .await;

        println!("{:?}", response);
    }

    #[tokio::test]
    #[serial]
    async fn database_collection_find_one_and_delete() {
        let response = read_mongo_test(FindOneInput::new(
            "some_db".to_string(),
            "some-coll".to_string(),
            Some(DocumentWrapper::from(bson::doc! { "x": 42})),
            None,
        ))
        .await;

        println!("{:?}", response);
    }
}
