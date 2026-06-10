use crate::api::lib::ClickhouseApi;
use crate::output::ClickhouseValueOutput;
use crate::request::ClickhouseRequest;
use clickhouse_core::{ClickhouseAsync, ClickhouseTx};
use deadpool::unmanaged::Object;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<ClickhouseApi, DdlInput> =
    ApiInfo::new(EpKind::Clickhouse, ClickhouseApi::Ddl, "Drop a table from the database", ReqType::Write);

#[allow(dead_code)]
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct JsonValue(serde_json::Value);

crate::clickhouse_endpoint! {
    Ddl,
    API_INFO,
    struct {
        table: String,
    }
}

impl_simple_operation!(SimpleInput, ClickhouseAsync, ClickhouseTx, ClickhouseApi, ClickhouseRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: ClickhouseAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("clickhouse.{}.{}", API_INFO.api(), function_name!()));

        let context: Object<clickhouse_client::Client> = context.get().await.map_err(EpError::connect)?;

        let start = std::time::SystemTime::now();

        context.query(&format!("DROP TABLE IF EXISTS {}", self.table)).execute().await.map_err(EpError::request)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from clickhouse",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(ClickhouseValueOutput(serde_json::to_value("Ok").map_err(EpError::serde)?).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut ClickhouseTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
