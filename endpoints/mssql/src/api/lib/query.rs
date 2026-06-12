use crate::api::lib::MssqlApi;
use crate::request::MssqlRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mssql_core::{MssqlAsync, MssqlTx};
use serde_json::Value;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MssqlApi, QueryInput> =
    ApiInfo::new(EpKind::Mssql, MssqlApi::Query, "Execute a SQL query against MSSQL", ReqType::Write);

crate::mssql_endpoint! {
    Query,
    API_INFO,
    struct {
        query: String,
        params: Value
    }
}

impl_simple_operation!(SimpleInput, MssqlAsync, MssqlTx, MssqlApi, MssqlRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, _context: MssqlAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let _span = telemetry_wrapper.client_tracer(format!("mssql.{}.{}", API_INFO.api(), function_name!()));

        Err(EpError::request("MSSQL query execution not yet implemented"))
    }

    fn run_transaction_generic(&self, _context: &mut MssqlTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("MSSQL transaction support not implemented")
    }
}
