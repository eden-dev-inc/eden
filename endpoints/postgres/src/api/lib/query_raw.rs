use crate::api::lib::PostgresApi;
use crate::api::wrapper::input::SqlParam;
use crate::request::PostgresRequest;
use crate::{ApiInfo, ReqType, RunOutput};
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

const API_INFO: ApiInfo<PostgresApi, QueryRawInput> = ApiInfo::new(
    EpKind::Postgres,
    PostgresApi::QueryRaw,
    "Executes a parameterized SQL query and returns raw, unprocessed results from PostgreSQL",
    ReqType::Write,
    true,
);

crate::postgres_endpoint! {
    QueryRaw,
    API_INFO,
    struct {
        query: String,
        params: Vec<SqlParam>
    }
}

impl_simple_operation!(SimpleInput, PostgresAsync, PostgresTx, PostgresApi, PostgresRequest);

#[allow(dead_code)]
impl SimpleInput {
    fn text_params(&self) -> Vec<Option<String>> {
        self.params.iter().map(|p| p.to_pg_text()).collect()
    }

    #[named]
    async fn run_async_generic(&self, _context: PostgresAsync, _telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        Err(EpError::request("streaming is not implemented yet"))
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
