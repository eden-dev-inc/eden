#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod read_tests {
    use crate::{
        servers::engine::method::endpoint::tests::clickhouse::clickhouse_endpoint::initialize_clickhouse,
        test_utils::{database_test_utils::initialize_engine_service, telemetry_test_utils::test_telemetry},
    };
    use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
    use eden_core::format::OrganizationCacheUuid;
    use endpoint::EpRequest;
    #[cfg(feature = "clickhouse")]
    use endpoint::clickhouse::{
        api::lib::{FetchAllInputBuilder, FetchOneInputBuilder, FetchOptionalInputBuilder, QueryReadOnlyInputBuilder},
        request::ClickhouseRequest,
    };
    use ep_core::database::schema::Table;
    use serde_json::Number;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use {clickhouse_core::connection::ClickhouseConnection, ep_core::settings::EdenSettings};

    #[tokio::test]
    #[serial]
    async fn test_query() {
        let (redis_container, pg_container, clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let container = testcontainers_modules::clickhouse::ClickHouse::default().start().await.expect("Failed to start database");
        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(8123).await.expect("Failed to get port");
        let connection = Box::new(ClickhouseConnection {
            url: format!("http://{host_ip}:{host_port}"),
            ..Default::default()
        });
        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_clickhouse(&db_manager, connection, &engine_service, test_telemetry).await;
        let mut read = Box::new(ClickhouseRequest(Box::new(
            QueryReadOnlyInputBuilder::default().query("SELECT version()").binds(vec![]).params(vec![]).build().unwrap_or_default(),
        )))
        .as_request();

        // test async connection
        let mut async_map = HeaderMap::new();
        async_map.append(HeaderName::from_static("x-eden-sync"), HeaderValue::from_static("true"));

        let output = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::from(&async_map),
                test_telemetry,
            )
            .await
            .expect("Failed to read data");
        assert_eq!(
            output.get("data").unwrap_or_default().as_array().expect("Failed to unwrap array")[0]
                .as_array()
                .expect("Failed to unwrap array")[0]
                .as_array()
                .expect("Failed to unwrap array")
                .len(),
            2
        );
        container.stop().await.expect("Failed to stop database");
        clickhouse_container.stop().await.expect("stop failed");
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    #[serial]
    async fn test_all_read_operations() {
        let (redis_container, pg_container, clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;
        let test_telemetry = &mut test_telemetry();
        let container = testcontainers_modules::clickhouse::ClickHouse::default().start().await.expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");
        let host_port = container.get_host_port_ipv4(8123).await.expect("Failed to get port");

        let connection = Box::new(ClickhouseConnection {
            url: format!("http://{host_ip}:{host_port}"),
            ..Default::default()
        });
        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_clickhouse(&db_manager, connection, &engine_service, test_telemetry).await;
        // Test FetchAll
        let mut read = Box::new(ClickhouseRequest(Box::new(
            FetchAllInputBuilder::default().query("SELECT 1").binds(vec![]).params(vec![]).build().unwrap_or_default(),
        )))
        .as_request();

        let result = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to fetch all");
        assert!(!result.as_object().expect("Failed to unwrap object").is_empty());
        // Test FetchOne
        let mut read = Box::new(ClickhouseRequest(Box::new(
            FetchOneInputBuilder::default().query("SELECT 1").binds(vec![]).params(vec![]).build().unwrap_or_default(),
        )))
        .as_request();
        let result = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to fetch one");
        assert_eq!(
            result.as_object().expect("Failed to unwrap object").get("1").unwrap_or_default(),
            &serde_json::Value::Number(Number::from(1))
        );
        // Test FetchOptional
        let mut read = Box::new(ClickhouseRequest(Box::new(
            FetchOptionalInputBuilder::default()
                .query("SELECT 1 WHERE 1=2")
                .binds(vec![])
                .params(vec![])
                .build()
                .unwrap_or_default(),
        )))
        .as_request();

        let result = engine_service
            .read(
                &mut *read,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to fetch optional");

        assert!(result.get("data").unwrap_or_default().is_null());
        container.stop().await.expect("Failed to stop database");
        clickhouse_container.stop().await.expect("stop failed");
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
