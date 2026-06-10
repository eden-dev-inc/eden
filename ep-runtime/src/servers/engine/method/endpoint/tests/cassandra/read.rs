#[cfg(all(test, feature = "infra-tests", external_db))]
pub mod read_tests {
    use crate::{
        servers::engine::method::endpoint::tests::cassandra::cassandra_endpoint::initialize_cassandra,
        test_utils::{database_test_utils::initialize_engine_service, telemetry_test_utils::test_telemetry},
    };
    use actix_web::http::header::{HeaderMap, HeaderName, HeaderValue};
    use eden_core::format::OrganizationCacheUuid;
    use endpoint::{
        EpRequest,
        cassandra::{api::lib::QuerySinglePageReadOnlyInputBuilder, request::CassandraRequest},
    };
    use ep_core::database::schema::Table;
    use serde_json::Number;
    use serial_test::serial;
    use testcontainers_modules::testcontainers::{GenericImage, ImageExt, core::ContainerPort, runners::AsyncRunner};
    use {cassandra_core::connection::CassandraConnection, ep_core::settings::EdenSettings};

    #[tokio::test]
    #[serial]
    async fn count_rows() {
        let (redis_container, pg_container, _clickhouse_container, db_manager, engine_service) = initialize_engine_service().await;

        let test_telemetry = &mut test_telemetry();

        let container = GenericImage::new("cassandra", "latest")
            .with_mapped_port(9042, ContainerPort::Tcp(9042))
            .start()
            .await
            .expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host");

        let connection = Box::new(CassandraConnection {
            known_nodes: vec![format!("{host_ip}:9042")],
            ..Default::default()
        });

        let (_user_schema, _eden_node_schema, organization_schema, endpoint_schema) =
            initialize_cassandra(&db_manager, connection, &engine_service, test_telemetry).await;

        // test regular Query that returns rows
        let mut read = Box::new(CassandraRequest(Box::new(
            QuerySinglePageReadOnlyInputBuilder::default()
                .query("SELECT n FROM Eden.x WHERE name = 'Answer' ALLOW FILTERING;")
                .build()
                .unwrap_or_default(),
        )))
        .as_request();

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

        // println!("{:?}", output);
        assert_eq!(
            output.get("rows").unwrap_or_default().as_array().expect("Expected array of rows to be an array of objects.").len(),
            1
        );
        assert_eq!(
            output.get("rows").unwrap_or_default().as_array().expect("Failed array")[0]
                .as_object()
                .expect("Expected object to be an object.")
                .get("n")
                .unwrap_or_default()
                .as_number()
                .expect("Expected number to be an integer."),
            &Number::from(42)
        );

        container.stop().await.expect("Failed to stop database");

        //manually teardown containers
        redis_container.stop().await.expect("stop failed");
        pg_container.stop().await.expect("stop failed");
    }
}
