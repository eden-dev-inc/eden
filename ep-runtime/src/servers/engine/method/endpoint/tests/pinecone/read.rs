#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod read_tests {
    use std::time::Duration;

    use crate::{
        servers::engine::method::endpoint::tests::pinecone::pinecone_endpoint::initialize_pinecone,
        test_utils::{database_test_utils::initialize_engine_service, telemetry_test_utils::test_telemetry},
    };

    use eden_core::format::OrganizationCacheUuid;
    use endpoint::EpRequest;
    #[cfg(feature = "pinecone")]
    use endpoint::pinecone::{
        api::lib::{DescribeIndexStatsInputBuilder, FetchInputBuilder, ListInputBuilder, QueryInputBuilder, UpsertInputBuilder},
        request::PineconeRequest,
    };
    use ep_core::database::schema::Table;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::{GenericImage, core::ContainerPort, runners::AsyncRunner};
    use {ep_core::settings::EdenSettings, pinecone_core::connection::PineconeConnection};

    #[tokio::test]
    #[serial]
    async fn describe_index_stats() {
        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let container = GenericImage::new("ghcr.io/pinecone-io/pinecone-index", "latest")
            .with_exposed_port(ContainerPort::Tcp(5081))
            .start()
            .await
            .expect("Failed to start database");
        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(5081).await.expect("Failed to get port");
        let connection = Box::new(PineconeConnection {
            url: format!("http://{host_ip}:{host_port}"),
            token: "test-token".to_string(),
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_pinecone(&db_manager, connection, &engine_service, test_telemetry).await;
        let mut read =
            Box::new(PineconeRequest(Box::new(DescribeIndexStatsInputBuilder::default().build().unwrap_or_default()))).as_request();

        let output = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to read data");
        // println!("{}", serde_json::to_string(&output).unwrap_or_default());
        assert!(output.get("dimension").is_some());
        container.stop().await.expect("Failed to stop database");
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    #[serial]
    async fn query_vectors() {
        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let container = GenericImage::new("ghcr.io/pinecone-io/pinecone-index", "latest")
            .with_exposed_port(ContainerPort::Tcp(5081))
            .start()
            .await
            .expect("Failed to start database");
        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(5081).await.expect("Failed to get port");
        let connection = Box::new(PineconeConnection {
            url: format!("http://{host_ip}:{host_port}"),
            token: "test-token".to_string(),
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_pinecone(&db_manager, connection, &engine_service, test_telemetry).await;

        // First upsert some test vectors
        let vec_string = (0..1536).map(|n| format!("{:.2}", (n as f32) / 10.0)).collect::<Vec<String>>().join(",");

        let vec_body = format!(r#"{{"vectors": [{{"id":"vec1", "values":[{}]}}]}}"#, vec_string);
        let mut write =
            Box::new(PineconeRequest(Box::new(UpsertInputBuilder::default().body(vec_body).build().unwrap_or_default()))).as_request();

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
        // Then query the vectors
        let mut read = Box::new(PineconeRequest(Box::new(
            QueryInputBuilder::default()
                .body(format!(r#"{{"vector":[{}], "topK":10, "includeMetadata":true}}"#, vec_string,))
                .build()
                .unwrap_or_default(),
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
            .expect("Failed to read data");
        // Parse the response and verify matches field exists
        // println!("{}", output);
        let response = output.as_object().expect("Expected object response");
        assert!(response.get("matches").is_some(), "Response missing 'matches' field: {:?}", response);
        container.stop().await.expect("Failed to stop database");
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    #[serial]
    async fn fetch_vectors() {
        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let container = GenericImage::new("ghcr.io/pinecone-io/pinecone-index", "latest")
            .with_exposed_port(ContainerPort::Tcp(5081))
            .start()
            .await
            .expect("Failed to start database");
        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(5081).await.expect("Failed to get port");
        let connection = Box::new(PineconeConnection {
            url: format!("http://{host_ip}:{host_port}"),
            token: "test-token".to_string(),
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_pinecone(&db_manager, connection, &engine_service, test_telemetry).await;
        let mut read = Box::new(PineconeRequest(Box::new(
            FetchInputBuilder::default().ids(vec!["vec1".to_string()]).namespace(None).build().unwrap_or_default(),
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
            .expect("Failed to read data");
        // println!("{}", serde_json::to_string(&output).unwrap_or_default_or_default());
        assert!(output.get("vectors").is_some());
        container.stop().await.expect("Failed to stop database");
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    #[serial]
    async fn list_vectors() {
        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let container = GenericImage::new("ghcr.io/pinecone-io/pinecone-index", "latest")
            .with_exposed_port(ContainerPort::Tcp(5081))
            .start()
            .await
            .expect("Failed to start database");
        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(5081).await.expect("Failed to get port");
        let connection = Box::new(PineconeConnection {
            url: format!("http://{host_ip}:{host_port}"),
            token: "test-token".to_string(),
        });
        tokio::time::sleep(Duration::from_secs(1)).await;
        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_pinecone(&db_manager, connection, &engine_service, test_telemetry).await;
        let mut read = Box::new(PineconeRequest(Box::new(
            ListInputBuilder::default().prefix(None).namespace(None).limit(None).pagination_token(None).build().unwrap_or_default(),
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
            .expect("Failed to read data");
        // println!("{}", serde_json::to_string(&output).unwrap_or_default_or_default());
        assert!(output.get("vectors").is_some());
        container.stop().await.expect("Failed to stop database");
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
