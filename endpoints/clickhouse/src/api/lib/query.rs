use crate::api::lib::{ClickhouseApi, Param, fetch_all_rows};
use crate::output::{ClickhouseRow, ClickhouseValueOutput};
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

const API_INFO: ApiInfo<ClickhouseApi, QueryInput> = ApiInfo::new(
    EpKind::Clickhouse,
    ClickhouseApi::Query,
    "Executes a SQL query against the ClickHouse endpoint",
    ReqType::Write,
);

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct JsonValue(serde_json::Value);

crate::clickhouse_endpoint! {
    Query,
    API_INFO,
    struct {
        query: String,
        binds: Vec<JsonValue>,
        params: Vec<Param>,
    }
}

impl_simple_operation!(SimpleInput, ClickhouseAsync, ClickhouseTx, ClickhouseApi, ClickhouseRequest);

impl SimpleInput {
    pub(crate) fn new(query: String, binds: Vec<JsonValue>, params: Vec<Param>) -> Self {
        Self { query, params, binds }
    }
    pub(crate) async fn run_query(&self, context: ClickhouseAsync) -> ResultEP<Vec<ClickhouseRow>> {
        let context: Object<clickhouse_client::Client> = context.get().await.map_err(EpError::connect)?;

        let mut query = context.query(self.query());

        for bind in self.binds() {
            query = query.bind(bind);
        }

        for param in self.params() {
            query = query.param(&param.name, param.value.clone());
        }

        fetch_all_rows(query).await.map_err(EpError::request)
    }

    #[named]
    async fn run_async_generic(&self, context: ClickhouseAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("clickhouse.{}.{}", API_INFO.api(), function_name!()));

        let start = std::time::SystemTime::now();

        let value = serde_json::to_value(self.run_query(context).await?).map_err(EpError::serde)?;

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
