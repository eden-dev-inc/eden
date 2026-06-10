#[allow(dead_code)]
pub(crate) mod database_test_utils {
    use crate::api::lib::PostgresApi;
    use crate::ep::PostgresEp;
    use crate::ep::{PostgresAsync, PostgresConfig, PostgresTx};
    use crate::request::PostgresRequest;
    use crate::{ApiExample, EP, EpRequest, Operation};
    use endpoint_test_utils::database_test_utils::generic_write;
    use endpoint_test_utils::telemetry_test_utils::test_telemetry;
    use ep_core::settings::EdenSettings;
    use format::cache_uuid::EndpointCacheUuid;
    use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
    use postgres_core::connection::PostgresConnection;
    use std::future::Future;
    use telemetry::TelemetryWrapper;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

    pub(crate) async fn generic_write_sync_test<T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx>>(example: &[ApiExample<T>]) {
        let (container, endpoint_cache_uuid, postgres_ep, mut test_telemetry) = connect_to_postgres().await;

        let test_telemetry = &mut test_telemetry;

        for api_example in example {
            let _ = generic_write(api_example.to_owned(), &endpoint_cache_uuid, postgres_ep.clone(), test_telemetry, true).await;
        }

        container.stop().await.expect("Failed to stop database");
    }

    pub(crate) async fn generic_write_async_test<T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx>>(example: &[ApiExample<T>]) {
        let (container, endpoint_cache_uuid, postgres_ep, mut test_telemetry) = connect_to_postgres().await;
        let test_telemetry = &mut test_telemetry;

        for api_example in example {
            let _ = generic_write(api_example.to_owned(), &endpoint_cache_uuid, postgres_ep.clone(), test_telemetry, true).await;
        }

        container.stop().await.expect("Failed to stop database");
    }

    pub(crate) async fn run_read<T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx>>(
        request: T,
        endpoint_cache_uuid: &EndpointCacheUuid,
        postgres_ep: PostgresEp,
        test_telemetry: &mut TelemetryWrapper,
    ) -> serde_json::Value {
        let mut request = Box::new(PostgresRequest(Box::new(request))) as Box<dyn EpRequest>;

        postgres_ep
            .read(endpoint_cache_uuid, &mut *request, EdenSettings::default(), test_telemetry)
            .await
            .expect("Failed to write to postgres")
    }

    pub(crate) async fn write_read_async_test<T, F, Fut>(f: F)
    where
        T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx>,
        F: FnOnce(EndpointCacheUuid, PostgresEp, &mut TelemetryWrapper) -> Fut,
        Fut: Future<Output = (serde_json::Value, T)>,
    {
        let (container, endpoint_cache_uuid, postgres_ep, mut test_telemetry) = connect_to_postgres().await;

        let test_telemetry = &mut test_telemetry;

        let (value, request) = f(endpoint_cache_uuid.clone(), postgres_ep.clone(), test_telemetry).await;

        let mut request = Box::new(PostgresRequest(Box::new(request))) as Box<dyn EpRequest>;

        let output = postgres_ep
            .read(&endpoint_cache_uuid, &mut *request, EdenSettings::default(), test_telemetry)
            .await
            .expect("Failed to write to postgres");

        assert_eq!(value, output);

        container.stop().await.expect("Failed to stop database");
    }

    pub(crate) async fn write_write_read_async_test<T, F, Fut, G>(f: F)
    where
        T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx>,
        F: FnOnce(EndpointCacheUuid, PostgresEp, &mut TelemetryWrapper) -> Fut,
        Fut: Future<Output = (ApiExample<T>, serde_json::Value, G)>,
        G: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx>,
    {
        let (container, endpoint_cache_uuid, postgres_ep, mut test_telemetry) = connect_to_postgres().await;

        let test_telemetry = &mut test_telemetry;

        // Pass owned values instead of references
        let (example, value, read_request) = f(endpoint_cache_uuid.clone(), postgres_ep.clone(), test_telemetry).await;

        let request = Box::new(PostgresRequest(Box::new(example.request))) as Box<dyn EpRequest>;

        let output = postgres_ep
            .write(&endpoint_cache_uuid, &*request, EdenSettings::default(), test_telemetry)
            .await
            .expect("Failed to write to postgres");

        match example.response {
            Ok(response) => {
                if let Some(response) = response {
                    assert_eq!(
                        serde_json::from_str::<serde_json::Value>(&response.to_string()).expect("failed to deserialize"),
                        output
                    );
                }
            }
            Err(e) => {
                if let Some(e) = e {
                    assert_eq!(serde_json::from_str::<serde_json::Value>(&e.to_string()).expect("failed to deserialize"), output)
                }
            }
        }

        let output = run_read(read_request, &endpoint_cache_uuid, postgres_ep, test_telemetry).await;

        assert_eq!(value, output);

        container.stop().await.expect("Failed to stop database");
    }

    async fn initialize_postgres() -> (ContainerAsync<GenericImage>, String, u16) {
        use testcontainers_modules::testcontainers::{GenericImage, ImageExt, core::ContainerPort};

        let container = GenericImage::new("postgres", "17")
            .with_exposed_port(ContainerPort::Tcp(5432))
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .start()
            .await
            .expect("Failed to start database");

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(5432).await.expect("Failed to get host port");

        (container, host_ip.to_string(), host_port)
    }

    pub(crate) async fn connect_to_postgres() -> (ContainerAsync<GenericImage>, EndpointCacheUuid, PostgresEp, TelemetryWrapper) {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let test_telemetry = &mut test_telemetry();

        let (container, host, port) = initialize_postgres().await;

        let connection = PostgresConnection {
            url: format!("postgres://postgres:postgres@{host}:{port}/postgres"),
            sslmode: None,
        };

        let (target, creds) = connection.split().expect("split postgres connection");
        let postgres_config = Box::new(PostgresConfig {
            target,
            read_credentials: Some(creds.clone()),
            write_credentials: Some(creds),
            ..Default::default()
        });

        let endpoint_cache_uuid =
            EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

        let mut postgres_endpoint = PostgresEp::new();

        postgres_endpoint
            .connect_async(&endpoint_cache_uuid, postgres_config.clone(), test_telemetry)
            .await
            .expect("Failed to connect sync to postgres");

        postgres_endpoint
            .connect_async(&endpoint_cache_uuid, postgres_config, test_telemetry)
            .await
            .expect("Failed to connect async to postgres");

        (container, endpoint_cache_uuid, postgres_endpoint, test_telemetry.clone())
    }

    #[tokio::test]
    async fn postgres_connection() {
        let _ = connect_to_postgres().await;
    }
}
