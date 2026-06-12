use crate::api::lib::PostgresApi;
use crate::api::wrapper::output::PostgresRowsOutput;
use crate::request::PostgresRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use postgres_core::{PostgresAsync, PostgresTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

use super::query_typed::SqlParamType;

const API_INFO: ApiInfo<PostgresApi, QueryTypedReadOnlyInput> = ApiInfo::new(
    EpKind::Postgres,
    PostgresApi::QueryTypedReadOnly,
    "Executes a read-only parameterized SQL query with explicitly typed parameters and returns results from PostgreSQL",
    ReqType::Read,
    true,
);

crate::postgres_endpoint! {
    QueryTypedReadOnly,
    API_INFO,
    struct {
        query: String,
        params: Vec<SqlParamType>,
    }
}

impl_simple_operation!(SimpleInput, PostgresAsync, PostgresTx, PostgresApi, PostgresRequest);

impl SimpleInput {
    fn text_params(&self) -> Vec<Option<String>> {
        self.params.iter().map(|pt| pt.0.to_pg_text()).collect()
    }

    fn type_oids(&self) -> Vec<i32> {
        self.params.iter().map(|pt| pt.1.type_oid()).collect()
    }

    #[named]
    async fn run_async_generic(&self, context: PostgresAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("postgres.{}.{}", API_INFO.api, function_name!()));
        let mut client = context.get().await.map_err(EpError::request)?;

        let start = std::time::SystemTime::now();

        let text_params = self.text_params();
        let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
        let type_oids = self.type_oids();
        let raw = client.query_params_typed_raw(self.query(), &param_refs, &type_oids).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from postgres",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(PostgresRowsOutput(raw).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
