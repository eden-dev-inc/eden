use crate::api::lib::ClickhouseApi;
use crate::output::{ClickhouseRow, ClickhouseValueOutput};
use crate::request::ClickhouseRequest;
use clickhouse_core::{ClickhouseAsync, ClickhouseTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<ClickhouseApi, InsertInput> =
    ApiInfo::new(EpKind::Clickhouse, ClickhouseApi::Insert, "Inserts rows of data into clickhouse.", ReqType::Write);

crate::clickhouse_endpoint! {
    Insert,
    API_INFO,
    struct {
        table: String,
        rows: Vec<ClickhouseRow>
    }
}

impl_simple_operation!(SimpleInput, ClickhouseAsync, ClickhouseTx, ClickhouseApi, ClickhouseRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: ClickhouseAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("clickhouse.{}.{}", API_INFO.api(), function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let start = std::time::SystemTime::now();

        let mut insert = context.insert(self.table()).map_err(EpError::request)?;
        for row in self.rows() {
            insert.write(row).await.map_err(EpError::request)?;
        }

        insert.end().await.map_err(EpError::request)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from clickhouse",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(ClickhouseValueOutput(serde_json::Value::String("Ok".to_string())).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut ClickhouseTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
