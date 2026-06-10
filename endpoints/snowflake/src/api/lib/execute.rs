use crate::api::lib::SnowflakeApi;
use crate::output::SnowflakeValueOutput;
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

const API_INFO: ApiInfo<SnowflakeApi, ExecuteInput> = ApiInfo::new(
    EpKind::Snowflake,
    SnowflakeApi::Execute,
    "Execute a SQL statement on Snowflake (for DDL, DML without return values)",
    ReqType::Write,
);

crate::snowflake_endpoint! {
    Execute,
    API_INFO,
    struct {
        statement: String,
        warehouse: Option<String>,
        database: Option<String>,
        schema: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, SnowflakeAsync, SnowflakeTx, SnowflakeApi, SnowflakeRequest);

/// Execution result containing metadata about the executed statement.
#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExecuteResult {
    /// Whether the statement was successful
    pub success: bool,
    /// The statement handle for tracking
    pub statement_handle: String,
    /// Number of rows affected (if applicable)
    pub rows_affected: Option<u64>,
    /// Message from the server
    pub message: String,
}

impl SimpleInput {
    #[allow(dead_code)]
    pub(crate) fn new(statement: String, warehouse: Option<String>, database: Option<String>, schema: Option<String>) -> Self {
        Self { statement, warehouse, database, schema }
    }

    pub(crate) async fn run_execute(&self, context: SnowflakeAsync) -> ResultEP<ExecuteResult> {
        let client = context.get().await.map_err(EpError::connect)?;

        // Build the request with optional parameters
        let mut request = StatementRequest::new(self.statement.clone());

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

        Ok(ExecuteResult {
            success: result.is_success(),
            statement_handle: result.statement_handle,
            rows_affected: result.metadata.as_ref().map(|m| m.num_rows),
            message: result.message,
        })
    }

    #[named]
    async fn run_async_generic(&self, context: SnowflakeAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("snowflake.{}.{}", API_INFO.api(), function_name!()));

        let start = std::time::SystemTime::now();

        let result = self.run_execute(context).await?;
        let value = serde_json::to_value(&result).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "executed statement on snowflake",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("success", result.success.to_string()),
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
