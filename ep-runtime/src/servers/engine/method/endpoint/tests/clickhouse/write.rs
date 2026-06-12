#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod write_tests {
    use crate::{
        servers::engine::method::endpoint::tests::clickhouse::clickhouse_endpoint::initialize_clickhouse,
        test_utils::{database_test_utils::initialize_engine_service, telemetry_test_utils::test_telemetry},
    };
    use eden_core::format::OrganizationCacheUuid;
    use endpoint::EpRequest;
    #[cfg(feature = "clickhouse")]
    use endpoint::clickhouse::{
        api::lib::{ExecuteInputBuilder, QueryReadOnlyInputBuilder},
        request::ClickhouseRequest,
    };
    use ep_core::database::schema::Table;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use {clickhouse_core::connection::ClickhouseConnection, ep_core::settings::EdenSettings};

    #[tokio::test]
    #[serial]
    async fn test_create_table() {
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
        // Create test table
        let mut write = Box::new(ClickhouseRequest(Box::new(
            ExecuteInputBuilder::default()
                .query(
                    "CREATE TABLE test_table (id UInt32, name String, value Float64)
                             ENGINE = MergeTree() ORDER BY id",
                )
                .binds(vec![])
                .params(vec![])
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
            .expect("Failed to create table");
        // Insert test data
        let mut write = Box::new(ClickhouseRequest(Box::new(
            ExecuteInputBuilder::default()
                .query(
                    "INSERT INTO test_table (id, name, value) VALUES
                             (1, 'test1', 10.5), (2, 'test2', 20.7)",
                )
                .binds(vec![])
                .params(vec![])
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
            .expect("Failed to insert data");
        // Verify data
        let mut read = Box::new(ClickhouseRequest(Box::new(
            QueryReadOnlyInputBuilder::default()
                .query("SELECT count(*) as count FROM test_table")
                .binds(vec![])
                .params(vec![])
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
        // count(*) returns UInt64; ClickHouse serializes 64-bit integers as strings in FORMAT JSON
        let count_val = output.get("data").unwrap_or_default().as_array().expect("Expected data array")[0]
            .as_array()
            .expect("Expected row array")
            .iter()
            .find(|kv| kv.as_array().map(|a| a[0].as_str() == Some("count")).unwrap_or(false))
            .and_then(|kv| kv.as_array())
            .map(|a| &a[1])
            .expect("Expected count value");
        // UInt64 is quoted as string; cast to u64 for comparison
        let count: u64 = count_val
            .as_u64()
            .unwrap_or_else(|| count_val.as_str().and_then(|s| s.parse().ok()).expect("Expected numeric count string"));
        assert_eq!(count, 2);
        container.stop().await.expect("Failed to stop database");
        clickhouse_container.stop().await.expect("stop failed");
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
