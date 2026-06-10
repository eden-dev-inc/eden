#![allow(dead_code)]

use crate::api::lib::DatadogApi;
use crate::ep::DatadogEp;
use crate::request::DatadogRequest;
use crate::{EP, EpRequest, Operation};
use datadog_core::config::DatadogConfig;
use datadog_core::connection::DatadogConnection;
use datadog_core::{DatadogAsync, DatadogTx};
use endpoint_test_utils::telemetry_test_utils::test_telemetry;
use ep_core::settings::EdenSettings;
use error::EpError;
use format::cache_uuid::EndpointCacheUuid;
use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
use serde_json::Value;
use telemetry::TelemetryWrapper;
use wiremock::MockServer;

pub struct DatadogTestContext {
    pub mock_server: MockServer,
    pub endpoint_cache_uuid: EndpointCacheUuid,
    pub ep: DatadogEp,
    pub telemetry: TelemetryWrapper,
}

impl DatadogTestContext {
    pub async fn new() -> Self {
        let telemetry = test_telemetry();

        let mock_server = MockServer::start().await;

        let connection = DatadogConnection {
            site: mock_server.uri(),
            api_key: "test-api-key".to_string(),
            application_key: Some("test-app-key".to_string()),
        };

        let (target, creds) = connection.split().expect("split connection");
        let config = Box::new(DatadogConfig {
            target,
            read_credentials: Some(creds.clone()),
            write_credentials: Some(creds),
            ..Default::default()
        });

        let endpoint_cache_uuid =
            EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid());

        let mut ep = DatadogEp::new();
        let mut tel = telemetry.clone();

        ep.connect_async(&endpoint_cache_uuid, config, &mut tel).await.expect("Failed to connect to mock Datadog");

        Self { mock_server, endpoint_cache_uuid, ep, telemetry: tel }
    }

    pub async fn write_op<T: Clone + Operation<DatadogAsync, DatadogApi, DatadogTx> + 'static>(&mut self, input: T) -> Value {
        let request = Box::new(DatadogRequest(Box::new(input))) as Box<dyn EpRequest>;
        let raw = self
            .ep
            .write(&self.endpoint_cache_uuid, &*request, EdenSettings::default(), &mut self.telemetry)
            .await
            .expect("Write operation failed");
        Self::unwrap_response(raw)
    }

    pub async fn read_op<T: Clone + Operation<DatadogAsync, DatadogApi, DatadogTx> + 'static>(&mut self, input: T) -> Value {
        let mut request = Box::new(DatadogRequest(Box::new(input))) as Box<dyn EpRequest>;
        let raw = self
            .ep
            .read(&self.endpoint_cache_uuid, &mut *request, EdenSettings::default(), &mut self.telemetry)
            .await
            .expect("Read operation failed");
        Self::unwrap_response(raw)
    }

    pub async fn write_op_err<T: Clone + Operation<DatadogAsync, DatadogApi, DatadogTx> + 'static>(&mut self, input: T) -> EpError {
        let request = Box::new(DatadogRequest(Box::new(input))) as Box<dyn EpRequest>;
        self.ep
            .write(&self.endpoint_cache_uuid, &*request, EdenSettings::default(), &mut self.telemetry)
            .await
            .expect_err("Expected write operation to fail")
    }

    fn unwrap_response(value: Value) -> Value {
        match value {
            Value::Object(mut map) => {
                map.remove("kind");
                if let Some(data) = map.remove("data") {
                    data
                } else if let Some(ok) = map.remove("ok") {
                    ok
                } else if let Some(error) = map.remove("error") {
                    error
                } else {
                    Value::Object(map)
                }
            }
            other => other,
        }
    }
}
