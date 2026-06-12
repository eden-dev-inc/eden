use crate::api::lib::{ClickhouseApi, Param};
use crate::output::ClickhouseValueOutput;
use crate::request::ClickhouseRequest;
use clickhouse_client::query::BytesCursor;
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

const API_INFO: ApiInfo<ClickhouseApi, FetchBytesInput> = ApiInfo::new(
    EpKind::Clickhouse,
    ClickhouseApi::FetchBytes,
    "Executes the query, returning a [`BytesCursor`] to obtain results as raw bytes containing data in the [provided format].",
    ReqType::Read,
);

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct JsonValue(serde_json::Value);

crate::clickhouse_endpoint! {
    FetchBytes,
    API_INFO,
    struct {
        query: String,
        format: String,
        binds: Vec<JsonValue>,
        params: Vec<Param>,
    }
}

impl_simple_operation!(SimpleInput, ClickhouseAsync, ClickhouseTx, ClickhouseApi, ClickhouseRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: ClickhouseAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("clickhouse.{}.{}", API_INFO.api(), function_name!()));

        let context: Object<clickhouse_client::Client> = context.get().await.map_err(EpError::connect)?;

        let start = std::time::SystemTime::now();

        let mut query = context.query(self.query());

        for bind in self.binds() {
            query = query.bind(bind);
        }

        for param in self.params() {
            query = query.param(&param.name, param.value.clone());
        }

        let mut cursor: BytesCursor = query.fetch_bytes(&self.format).map_err(EpError::request)?;

        let mut result = vec![];
        while let Some(bytes) = cursor.next().await.map_err(EpError::request)? {
            result.push(bytes.to_vec())
        }

        let value = serde_json::to_value(result).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from clickhouse",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(ClickhouseValueOutput(value).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut ClickhouseTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
