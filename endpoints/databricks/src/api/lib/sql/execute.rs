use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::ToSchema;

const API_INFO: ApiInfo<DatabricksApi, ExecuteInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::Execute,
    "Execute a SQL statement on Databricks (for DDL, DML without return values)",
    ReqType::Write,
);

crate::databricks_endpoint! {
    Execute,
    API_INFO,
    struct {
        statement: String,
        catalog: Option<String>,
        schema: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

#[derive(Debug, Serialize, Deserialize, Clone, ToSchema)]
pub struct ExecuteResult {
    pub success: bool,
    pub statement_id: Option<String>,
    pub rows_affected: Option<u64>,
    pub message: String,
}

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));

        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let result = client.execute_statement_with_params(&self.statement, self.catalog.as_deref(), self.schema.as_deref()).await?;

        let exec_result = ExecuteResult {
            success: result.is_success(),
            statement_id: result.statement_id,
            rows_affected: result.result.as_ref().and_then(|r| r.row_count),
            message: result.status.state.clone(),
        };

        let value = serde_json::to_value(&exec_result).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "executed statement on databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("success", exec_result.success.to_string()),
            ],
        );

        Ok(Box::new(DatabricksJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatabricksTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Databricks transaction support not implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn execute_builder_serde() {
        let input = ExecuteInputBuilder::default()
            .statement("CREATE TABLE test (id INT)")
            .catalog(Some("my_catalog".to_string()))
            .schema(Some("my_schema".to_string()))
            .build()
            .expect("Failed to build ExecuteInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "execute");
        assert_eq!(json["statement"], "CREATE TABLE test (id INT)");
        assert_eq!(json["catalog"], "my_catalog");
        assert_eq!(json["schema"], "my_schema");
    }

    #[test]
    fn execute_builder_no_catalog() {
        let input = ExecuteInputBuilder::default()
            .statement("INSERT INTO t VALUES (1)")
            .catalog(None::<String>)
            .schema(None::<String>)
            .build()
            .expect("Failed to build ExecuteInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "execute");
        assert_eq!(json["statement"], "INSERT INTO t VALUES (1)");
        assert!(json["catalog"].is_null());
    }

    #[test]
    fn execute_deserialize() {
        let json = serde_json::json!({
            "statement": "DROP TABLE old_table",
            "catalog": null,
            "schema": null
        });
        let input: ExecuteInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.statement, "DROP TABLE old_table");
        assert!(input.catalog.is_none());
    }

    #[test]
    fn execute_result_serde() {
        let result = ExecuteResult {
            success: true,
            statement_id: Some("stmt-123".to_string()),
            rows_affected: Some(42),
            message: "SUCCEEDED".to_string(),
        };
        let json = serde_json::to_value(&result).expect("Failed to serialize");
        assert_eq!(json["success"], true);
        assert_eq!(json["statement_id"], "stmt-123");
        assert_eq!(json["rows_affected"], 42);
        assert_eq!(json["message"], "SUCCEEDED");

        let deserialized: ExecuteResult = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(deserialized.success);
        assert_eq!(deserialized.rows_affected, Some(42));
    }
}
