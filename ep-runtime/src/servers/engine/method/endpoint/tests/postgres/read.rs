#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod read_tests {
    use crate::{
        servers::engine::method::endpoint::tests::postgres::postgres_endpoint::initialize_postgres,
        test_utils::{database_test_utils::initialize_engine_service, telemetry_test_utils::test_telemetry},
    };

    use eden_core::format::OrganizationCacheUuid;
    use endpoint::EpRequest;
    #[cfg(feature = "postgres")]
    use endpoint::postgres::{
        api::{
            lib::{
                copy_out::CopyOutInputBuilder, query_one_read_only::QueryOneReadOnlyInputBuilder,
                query_read_only::QueryReadOnlyInputBuilder, query_typed::SqlParamType,
                query_typed_read_only::QueryTypedReadOnlyInputBuilder, simple_query_read_only::SimpleQueryReadOnlyInputBuilder,
            },
            wrapper::input::{SqlParam, SqlType},
        },
        request::PostgresRequest,
    };
    use ep_core::database::schema::Table;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::{ImageExt, core::ContainerPort, runners::AsyncRunner};
    use {ep_core::settings::EdenSettings, postgres_core::connection::PostgresConnection};

    #[tokio::test]
    #[serial]
    async fn count_rows() {
        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

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

        // println!("{}", serde_json::to_string(&output).unwrap_or_default());
        assert_eq!(output.as_object().expect("Expected object").get("count").unwrap_or_default(), 2);

        // do the same test with SimpleQuery that returns string message
        let mut read = Box::new(PostgresRequest(Box::new(
            SimpleQueryReadOnlyInputBuilder::default().query("SELECT count(*) FROM test_table;").build().unwrap_or_default(),
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

        // SimpleQueryReadOnly: count(*) is INT8, text_to_json converts to Number
        assert_eq!(output.get("count").unwrap_or_default(), 2);

        // and another one using QueryTyped
        let mut read = Box::new(PostgresRequest(Box::new(
            QueryTypedReadOnlyInputBuilder::default()
                .query("SELECT count(*) FROM test_table WHERE name=$1;")
                .params(vec![SqlParamType(SqlParam::Text("Alice".to_string()), SqlType::Text)])
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

        assert_eq!(output.get("count").unwrap_or_default(), 1);

        // and another one using QueryOne
        let mut read = Box::new(PostgresRequest(Box::new(
            QueryOneReadOnlyInputBuilder::default()
                .query("SELECT count(*) FROM test_table WHERE name=$1;")
                .params(vec![SqlParam::Text("Bob".to_string())])
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

        assert_eq!(output.get("count").unwrap_or_default(), 1);

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }

    #[tokio::test]
    #[serial]
    async fn copy_out_data() {
        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

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
        let mut read = Box::new(PostgresRequest(Box::new(
            CopyOutInputBuilder::default().query("COPY test_table TO STDOUT").build().unwrap_or_default(),
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

        // println!("{}", serde_json::to_string(&output).unwrap_or_default());
        assert_eq!(
            output.as_object().expect("Expected object").get("value").unwrap_or_default(),
            &serde_json::Value::String("1\tAlice\t25\n2\tBob\t27\n".into()),
        );

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
