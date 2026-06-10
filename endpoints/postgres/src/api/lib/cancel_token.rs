use crate::api::lib::PostgresApi;
use crate::api::wrapper::output::CancelTokenAsyncOutput;
use crate::request::PostgresRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::EpOutput;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;
use {
    ep_core::impl_simple_operation,
    postgres_core::{PostgresAsync, PostgresTx},
};

const API_INFO: ApiInfo<PostgresApi, CancelTokenInput> = ApiInfo::new(
    EpKind::Postgres,
    PostgresApi::CancelToken,
    "Cancels a running PostgreSQL query using a cancel token",
    ReqType::Write,
    true,
);

crate::postgres_endpoint! {
    CancelToken,
    API_INFO,
    struct {
        query: String,
    }
}

impl_simple_operation!(SimpleInput, PostgresAsync, PostgresTx, PostgresApi, PostgresRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: PostgresAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("postgres.{}.{}", API_INFO.api, function_name!()));
        let _client = context.get().await.map_err(EpError::request)?;
        Ok(Box::new(CancelTokenAsyncOutput.to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
