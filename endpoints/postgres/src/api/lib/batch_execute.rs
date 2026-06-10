use crate::api::lib::PostgresApi;
use crate::api::wrapper::output::EmptyOutput;
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

const API_INFO: ApiInfo<PostgresApi, BatchExecuteInput> = ApiInfo::new(
    EpKind::Postgres,
    PostgresApi::BatchExecute,
    "Executes multiple SQL statements as a batch operation in PostgreSQL",
    ReqType::Write,
    true,
);

crate::postgres_endpoint! {
    BatchExecute,
    API_INFO,
    struct {
        query: String,
    }
}

impl_simple_operation!(SimpleInput, PostgresAsync, PostgresTx, PostgresApi, PostgresRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: PostgresAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("postgres.{}.{}", API_INFO.api, function_name!()));

        let mut client = context.get().await.map_err(EpError::request)?;

        let start = std::time::SystemTime::now();

        client.batch_execute(self.query()).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from postgres",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(EmptyOutput(()).to_output()) as Box<dyn EpOutput>)
    }
    fn run_transaction_generic(&self, _tx_context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
