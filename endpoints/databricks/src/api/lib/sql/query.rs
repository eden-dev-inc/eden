use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatabricksApi, QueryInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::Query,
    "Execute a SQL query on Databricks and return results",
    ReqType::Read,
);

crate::databricks_endpoint! {
    Query,
    API_INFO,
    struct {
        query: String,
        catalog: Option<String>,
        schema: Option<String>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));

        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let result = client.execute_statement_with_params(&self.query, self.catalog.as_deref(), self.schema.as_deref()).await?;

        // Convert to row-based JSON output
        let rows = if let Some(ref res) = result.result {
            if let Some(ref data) = res.data_array {
                let column_names: Vec<String> = result
                    .manifest
                    .as_ref()
                    .and_then(|m| m.schema.as_ref())
                    .and_then(|s| s.columns.as_ref())
                    .map(|cols| cols.iter().map(|c| c.name.clone()).collect())
                    .unwrap_or_default();

                data.iter()
                    .map(|row_data| {
                        let mut row = serde_json::Map::new();
                        for (i, val) in row_data.iter().enumerate() {
                            let col_name = column_names.get(i).cloned().unwrap_or_else(|| format!("col_{}", i));
                            row.insert(col_name, val.clone());
                        }
                        serde_json::Value::Object(row)
                    })
                    .collect::<Vec<_>>()
            } else {
                vec![]
            }
        } else {
            vec![]
        };

        let value = serde_json::to_value(&rows).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
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
    fn query_builder_serde() {
        let input = QueryInputBuilder::default()
            .query("SELECT * FROM table1")
            .catalog(Some("my_catalog".to_string()))
            .schema(Some("my_schema".to_string()))
            .build()
            .expect("Failed to build QueryInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "query");
        assert_eq!(json["query"], "SELECT * FROM table1");
        assert_eq!(json["catalog"], "my_catalog");
        assert_eq!(json["schema"], "my_schema");
    }

    #[test]
    fn query_builder_no_catalog() {
        let input = QueryInputBuilder::default()
            .query("SELECT 1")
            .catalog(None::<String>)
            .schema(None::<String>)
            .build()
            .expect("Failed to build QueryInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "query");
        assert_eq!(json["query"], "SELECT 1");
        assert!(json["catalog"].is_null());
        assert!(json["schema"].is_null());
    }

    #[test]
    fn query_deserialize() {
        let json = serde_json::json!({
            "query": "SELECT count(*) FROM events",
            "catalog": "prod",
            "schema": "analytics"
        });
        let input: QueryInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.query, "SELECT count(*) FROM events");
        assert_eq!(input.catalog.as_deref(), Some("prod"));
        assert_eq!(input.schema.as_deref(), Some("analytics"));
    }
}
