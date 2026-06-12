use crate::api::lib::DatabricksApi;
use crate::output::DatabricksJsonOutput;
use crate::request::DatabricksRequest;
use databricks_core::{DatabricksAsync, DatabricksTx};
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::Deserialize;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatabricksApi, ListSchemasInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::ListSchemas,
    "List all schemas in a Databricks Unity Catalog catalog",
    ReqType::Read,
);

#[derive(Debug, Deserialize)]
struct SchemaListResponse {
    #[serde(default)]
    schemas: Vec<SchemaInfo>,
}

#[derive(Debug, Deserialize)]
struct SchemaInfo {
    name: String,
    catalog_name: String,
    #[serde(default)]
    comment: Option<String>,
}

crate::databricks_endpoint! {
    ListSchemas,
    API_INFO,
    struct {
        catalog_name: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        let path = format!("/api/2.1/unity-catalog/schemas?catalog_name={}", self.catalog_name);
        let response = client.get(&path).await?;
        let schema_list: SchemaListResponse = serde_json::from_value(response).map_err(EpError::serde)?;

        let schemas: Vec<serde_json::Value> = schema_list
            .schemas
            .iter()
            .map(|s| {
                serde_json::json!({
                    "name": s.name,
                    "catalog_name": s.catalog_name,
                    "comment": s.comment,
                })
            })
            .collect();

        let value = serde_json::to_value(&schemas).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "listed schemas from databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("count", schemas.len().to_string()),
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
    fn list_schemas_builder_serde() {
        let input = ListSchemasInputBuilder::default().catalog_name("main").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "listschemas");
        assert_eq!(json["catalog_name"], "main");
    }
}
