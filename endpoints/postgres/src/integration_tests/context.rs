#![allow(dead_code)]

use crate::api::lib::PostgresApi;
use crate::api::lib::batch_execute::BatchExecuteInput;
use crate::api::lib::copy_in::CopyInInput;
use crate::api::lib::copy_out::CopyOutInput;
use crate::api::lib::execute::ExecuteInput;
use crate::api::lib::query::QueryInput;
use crate::api::lib::query_one::QueryOneInput;
use crate::api::lib::query_opt::QueryOptInput;
use crate::api::lib::query_typed::{QueryTypedInput, SqlParamType};
use crate::api::lib::simple_query::SimpleQueryInput;
use crate::api::wrapper::input::SqlParam;
use crate::ep::PostgresEp;
use crate::ep::{PostgresAsync, PostgresConfig, PostgresTx};
use crate::request::PostgresRequest;
use crate::{EP, EpRequest, Operation};
use endpoint_test_utils::telemetry_test_utils::test_telemetry;
use ep_core::settings::EdenSettings;
use error::EpError;
use format::cache_uuid::EndpointCacheUuid;
use format::{CacheUuid, EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
use postgres_core::connection::PostgresConnection;
use serde_json::Value;
use telemetry::TelemetryWrapper;
use testcontainers_modules::testcontainers::core::ContainerPort;
use testcontainers_modules::testcontainers::runners::AsyncRunner;
use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage, ImageExt};

pub struct PostgresTestContext {
    container: ContainerAsync<GenericImage>,
    pub endpoint_cache_uuid: EndpointCacheUuid,
    pub ep: PostgresEp,
    pub telemetry: TelemetryWrapper,
}

impl PostgresTestContext {
    pub async fn new() -> Self {
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

        let telemetry = test_telemetry();

        let container = GenericImage::new("postgres", "17")
            .with_exposed_port(ContainerPort::Tcp(5432))
            .with_env_var("POSTGRES_PASSWORD", "postgres")
            .start()
            .await
            .expect("Failed to start postgres container");

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(5432).await.expect("Failed to get host port");

        let connection = PostgresConnection {
            url: format!("postgres://postgres:postgres@{host_ip}:{host_port}/postgres"),
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

        let mut ep = PostgresEp::new();
        let mut tel = telemetry.clone();

        ep.connect_async(&endpoint_cache_uuid, postgres_config.clone(), &mut tel).await.expect("Failed to connect to postgres");

        ep.connect_async(&endpoint_cache_uuid, postgres_config, &mut tel).await.expect("Failed to connect to postgres (second)");

        Self { container, endpoint_cache_uuid, ep, telemetry: tel }
    }

    pub async fn stop(self) {
        self.container.stop().await.expect("Failed to stop container");
    }

    // ---- Write operations (go through ep.write) ----

    /// Execute a write statement (INSERT/UPDATE/DELETE/DDL), returns affected row count
    pub async fn execute(&mut self, query: &str, params: &[SqlParam]) -> Value {
        let input = ExecuteInput::new(query.to_string(), params.to_vec());
        self.write_op(input).await
    }

    /// Execute a write statement expecting an error
    pub async fn execute_err(&mut self, query: &str, params: &[SqlParam]) -> EpError {
        let input = ExecuteInput::new(query.to_string(), params.to_vec());
        self.write_op_err(input).await
    }

    /// Execute multiple statements as a batch (DDL), returns success
    pub async fn batch_execute(&mut self, query: &str) -> Value {
        let input = BatchExecuteInput::new(query.to_string());
        self.write_op(input).await
    }

    /// Execute batch_execute expecting an error
    pub async fn batch_execute_err(&mut self, query: &str) -> EpError {
        let input = BatchExecuteInput::new(query.to_string());
        self.write_op_err(input).await
    }

    /// Execute COPY FROM STDIN
    pub async fn copy_in(&mut self, query: &str, value: &str) -> Value {
        let input = CopyInInput::new(query.to_string(), value.to_string());
        self.write_op(input).await
    }

    /// Execute COPY FROM STDIN expecting an error
    pub async fn copy_in_err(&mut self, query: &str, value: &str) -> EpError {
        let input = CopyInInput::new(query.to_string(), value.to_string());
        self.write_op_err(input).await
    }

    // ---- Read operations (go through ep.read) ----

    /// Execute a query, returns rows (null for 0, object for 1, array for 2+)
    pub async fn query(&mut self, query: &str, params: &[SqlParam]) -> Value {
        let input = QueryInput::new(query.to_string(), params.to_vec());
        self.read_op(input).await
    }

    /// Execute a query expecting an error
    pub async fn query_err(&mut self, query: &str, params: &[SqlParam]) -> EpError {
        let input = QueryInput::new(query.to_string(), params.to_vec());
        self.read_op_err(input).await
    }

    /// Execute query_one, returns exactly one row or errors
    pub async fn query_one(&mut self, query: &str, params: &[SqlParam]) -> Value {
        let input = QueryOneInput::new(query.to_string(), params.to_vec());
        self.read_op(input).await
    }

    /// Execute query_one expecting an error
    pub async fn query_one_err(&mut self, query: &str, params: &[SqlParam]) -> EpError {
        let input = QueryOneInput::new(query.to_string(), params.to_vec());
        self.read_op_err(input).await
    }

    /// Execute query_opt, returns 0 or 1 row (null or object)
    pub async fn query_opt(&mut self, query: &str, params: &[SqlParam]) -> Value {
        let input = QueryOptInput::new(query.to_string(), params.to_vec());
        self.read_op(input).await
    }

    /// Execute query_typed with explicitly typed parameters
    pub async fn query_typed(&mut self, query: &str, params: &[SqlParamType]) -> Value {
        let input = QueryTypedInput::new(query.to_string(), params.to_vec());
        self.read_op(input).await
    }

    /// Execute simple_query (no params)
    pub async fn simple_query(&mut self, query: &str) -> Value {
        let input = SimpleQueryInput::new(query.to_string());
        self.read_op(input).await
    }

    /// Execute simple_query expecting an error
    pub async fn simple_query_err(&mut self, query: &str) -> EpError {
        let input = SimpleQueryInput::new(query.to_string());
        self.read_op_err(input).await
    }

    /// Execute COPY TO STDOUT
    pub async fn copy_out(&mut self, query: &str) -> Value {
        let input = CopyOutInput::new(query.to_string());
        self.read_op(input).await
    }

    /// Execute COPY TO STDOUT expecting an error
    pub async fn copy_out_err(&mut self, query: &str) -> EpError {
        let input = CopyOutInput::new(query.to_string());
        self.read_op_err(input).await
    }

    // ---- Internal helpers ----

    async fn write_op<T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx> + 'static>(&mut self, input: T) -> Value {
        let request = Box::new(PostgresRequest(Box::new(input))) as Box<dyn EpRequest>;
        let raw = self
            .ep
            .write(&self.endpoint_cache_uuid, &*request, EdenSettings::default(), &mut self.telemetry)
            .await
            .expect("Write operation failed");
        Self::unwrap_response(raw)
    }

    async fn write_op_err<T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx> + 'static>(&mut self, input: T) -> EpError {
        let request = Box::new(PostgresRequest(Box::new(input))) as Box<dyn EpRequest>;
        self.ep
            .write(&self.endpoint_cache_uuid, &*request, EdenSettings::default(), &mut self.telemetry)
            .await
            .expect_err("Expected write operation to fail")
    }

    async fn read_op<T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx> + 'static>(&mut self, input: T) -> Value {
        let mut request = Box::new(PostgresRequest(Box::new(input))) as Box<dyn EpRequest>;
        let raw = self
            .ep
            .read(&self.endpoint_cache_uuid, &mut *request, EdenSettings::default(), &mut self.telemetry)
            .await
            .expect("Read operation failed");
        Self::unwrap_response(raw)
    }

    async fn read_op_err<T: Clone + Operation<PostgresAsync, PostgresApi, PostgresTx> + 'static>(&mut self, input: T) -> EpError {
        let mut request = Box::new(PostgresRequest(Box::new(input))) as Box<dyn EpRequest>;
        self.ep
            .read(&self.endpoint_cache_uuid, &mut *request, EdenSettings::default(), &mut self.telemetry)
            .await
            .expect_err("Expected read operation to fail")
    }

    /// Unwrap the EndpointOutput wrapper to get the inner data.
    ///
    /// EndpointOutput serializes as:
    /// - Object response (flattened): `{"kind": "postgres", ...fields}` → strip "kind", return rest
    /// - Non-object response (wrapped): `{"kind": "postgres", "data": <value>}` → extract "data"
    /// - Ok response: `{"kind": "postgres", "ok": "success"}` → extract "ok"
    fn unwrap_response(value: Value) -> Value {
        match value {
            Value::Object(mut map) => {
                map.remove("kind");
                if let Some(data) = map.remove("data") {
                    // Non-object response wrapped in {"data": <value>}
                    data
                } else if let Some(ok) = map.remove("ok") {
                    // EndpointResponse::Ok wrapped in {"ok": "success"}
                    ok
                } else if let Some(error) = map.remove("error") {
                    // EndpointResponse::Err wrapped in {"error": "..."}
                    error
                } else {
                    // Flattened object response — "kind" already removed
                    Value::Object(map)
                }
            }
            other => other,
        }
    }
}
