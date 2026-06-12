use crate::api::lib::PostgresApi;
use crate::api::wrapper::input::SqlParam;
use crate::api::wrapper::output::PostgresRowOutput;
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

const API_INFO: ApiInfo<PostgresApi, QueryOneInput> = ApiInfo::new(
    EpKind::Postgres,
    PostgresApi::QueryOne,
    "Executes a parameterized SQL query and returns exactly one row from PostgreSQL",
    ReqType::Write,
    true,
);

crate::postgres_endpoint! {
    QueryOne,
    API_INFO,
    struct {
        query: String,
        params: Vec<SqlParam>
    }
}

impl_simple_operation!(SimpleInput, PostgresAsync, PostgresTx, PostgresApi, PostgresRequest);

impl SimpleInput {
    fn text_params(&self) -> Vec<Option<String>> {
        self.params.iter().map(|p| p.to_pg_text()).collect()
    }

    #[named]
    async fn run_async_generic(&self, context: PostgresAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("postgres.{}.{}", API_INFO.api, function_name!()));
        let mut client = context.get().await.map_err(EpError::request)?;

        let start = std::time::SystemTime::now();

        let text_params = self.text_params();
        let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
        let raw = client.query_params_raw(self.query(), &param_refs).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from postgres",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(PostgresRowOutput(raw).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}

#[test]
fn serialize_test() {
    let ser = QueryOneInput::new("test".to_string(), vec![]);

    println!("{:?}", serde_json::to_value(ser).unwrap_or_default())
}
