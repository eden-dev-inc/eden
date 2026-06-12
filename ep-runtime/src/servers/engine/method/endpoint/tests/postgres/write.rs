#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod write_tests {
    use crate::{
        servers::engine::method::endpoint::tests::postgres::postgres_endpoint::initialize_postgres,
        test_utils::{database_test_utils::initialize_engine_service, telemetry_test_utils::test_telemetry},
    };

    use eden_core::format::OrganizationCacheUuid;
    use endpoint::EpRequest;
    #[cfg(feature = "postgres")]
    use endpoint::postgres::{
        api::lib::{copy_in::CopyInInputBuilder, query_read_only::QueryReadOnlyInputBuilder},
        request::PostgresRequest,
    };
    use ep_core::database::schema::Table;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::{ImageExt, core::ContainerPort, runners::AsyncRunner};
    use {ep_core::settings::EdenSettings, postgres_core::connection::PostgresConnection};

    #[tokio::test]
    #[serial]
    async fn copy_in_data() {
        let (redis_container, pg_container, _clickhouse_conn, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

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

        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_postgres(&db_manager, connection, &engine_service, test_telemetry).await;

        // test regular Query that returns rows
        let mut write = Box::new(PostgresRequest(Box::new(
            CopyInInputBuilder::default()
                .query("COPY test_table FROM STDIN")
                .value("3\tClive\t42\n4\tDaisy\t11\n")
                .build()
                .unwrap_or_default(),
        )))
        .as_request();

        let output = engine_service
            .write(
                &mut *write,
                &endpoint_schema,
                OrganizationCacheUuid::from(organization_schema.uuid()),
                EdenSettings::default(),
                test_telemetry,
            )
            .await
            .expect("Failed to write data");

        // println!("{}", serde_json::to_string(&output).unwrap_or_default());
        assert_eq!(
            output.as_object().expect("Expected object to be object").get("rows").unwrap_or_default(),
            &serde_json::Value::Number(2.into()),
        );

        // check that there are 4 records in the database now (2 initialized, 2 added)
        let mut read = Box::new(PostgresRequest(Box::new(
            QueryReadOnlyInputBuilder::default()
                .query("SELECT count(*) FROM test_table;")
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

        assert_eq!(output.as_object().expect("Expected object to be object").get("count").unwrap_or_default(), 4);

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
