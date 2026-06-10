use crate::api::lib::SnowflakeApi;
use crate::output::{SnowflakeRow, SnowflakeValueOutput};
use crate::request::SnowflakeRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use snowflake_core::client::StatementRequest;
use snowflake_core::{SnowflakeAsync, SnowflakeTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::{PartialSchema, ToSchema};

const API_INFO: ApiInfo<SnowflakeApi, QueryInput> = ApiInfo::new(
    EpKind::Snowflake,
    SnowflakeApi::Query,
    "Execute a SQL query on Snowflake and return results",
    ReqType::Read,
);

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct JsonValue(serde_json::Value);

crate::snowflake_endpoint! {
    Query,
    API_INFO,
    struct {
        query: String,
        warehouse: Option<String>,
        database: Option<String>,
        schema: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, SnowflakeAsync, SnowflakeTx, SnowflakeApi, SnowflakeRequest);

impl SimpleInput {
    #[allow(dead_code)]
    pub(crate) fn new(query: String, warehouse: Option<String>, database: Option<String>, schema: Option<String>) -> Self {
        Self { query, warehouse, database, schema }
    }

    pub(crate) async fn run_query(&self, context: SnowflakeAsync) -> ResultEP<Vec<SnowflakeRow>> {
        let client = context.get().await.map_err(EpError::connect)?;

        // Build the request with optional parameters
        let mut request = StatementRequest::new(self.query.clone());

        if let Some(ref wh) = self.warehouse {
            request = request.with_warehouse(wh.clone());
        }
        if let Some(ref db) = self.database {
            request = request.with_database(db.clone());
        }
        if let Some(ref sch) = self.schema {
            request = request.with_schema(sch.clone());
        }

        let result = client.execute_request(request).await?;

        // Convert result set to SnowflakeRow format
        let rows = if let Some(data) = result.data {
            let column_names: Vec<String> =
                result.metadata.as_ref().map(|m| m.row_type.iter().map(|c| c.name.clone()).collect()).unwrap_or_default();

            data.iter()
                .map(|row_data| {
                    let values: Vec<(String, serde_json::Value)> =
                        column_names.iter().zip(row_data.iter()).map(|(name, value)| (name.clone(), value.clone())).collect();
                    SnowflakeRow::from(values)
                })
                .collect()
        } else {
            vec![]
        };

        Ok(rows)
    }

    #[named]
    async fn run_async_generic(&self, context: SnowflakeAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("snowflake.{}.{}", API_INFO.api(), function_name!()));

        let start = std::time::SystemTime::now();

        let value = serde_json::to_value(self.run_query(context).await?).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from snowflake",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(SnowflakeValueOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut SnowflakeTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // Snowflake does not support traditional transactions via the SQL API
        todo!("Snowflake transaction support not implemented")
    }
}
