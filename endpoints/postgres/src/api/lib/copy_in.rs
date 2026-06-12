use crate::api::lib::PostgresApi;
use crate::api::wrapper::output::CopyInWriterOutput;
use crate::request::PostgresRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use postgres_core::{PostgresAsync, PostgresTx};
use std::time;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<PostgresApi, CopyInInput> = ApiInfo::new(
    EpKind::Postgres,
    PostgresApi::CopyIn,
    "Executes a COPY FROM operation to bulk insert data into PostgreSQL",
    ReqType::Write,
    true,
);

crate::postgres_endpoint! {
    CopyIn,
    API_INFO,
    struct {
        query: String,
        value: String,
    }
}

impl_simple_operation!(SimpleInput, PostgresAsync, PostgresTx, PostgresApi, PostgresRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: PostgresAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("postgres.{}.{}", API_INFO.api, function_name!()));
        let mut client = context.get().await.map_err(EpError::request)?;

        let start = time::SystemTime::now();

        let rows = client.copy_in(self.query(), self.value.as_bytes()).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from postgres",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(CopyInWriterOutput::from(rows).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut PostgresTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
