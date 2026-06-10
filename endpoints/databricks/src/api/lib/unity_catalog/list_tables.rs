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

const API_INFO: ApiInfo<DatabricksApi, ListTablesInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::ListTables,
    "List all tables in a Databricks Unity Catalog schema",
    ReqType::Read,
);

#[derive(Debug, Deserialize)]
struct TableListResponse {
    #[serde(default)]
    tables: Vec<TableInfo>,
}

#[derive(Debug, Deserialize)]
struct TableInfo {
    name: String,
    catalog_name: String,
    schema_name: String,
    #[serde(default)]
    table_type: Option<String>,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    columns: Option<Vec<TableColumnInfo>>,
}

#[derive(Debug, Deserialize)]
struct TableColumnInfo {
    name: String,
    #[serde(default)]
    type_name: Option<String>,
    #[serde(default)]
    position: Option<u32>,
    #[serde(default)]
    comment: Option<String>,
}

crate::databricks_endpoint! {
    ListTables,
    API_INFO,
    struct {
        catalog_name: String,
        schema_name: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        let path = format!("/api/2.1/unity-catalog/tables?catalog_name={}&schema_name={}", self.catalog_name, self.schema_name);
        let response = client.get(&path).await?;
        let table_list: TableListResponse = serde_json::from_value(response).map_err(EpError::serde)?;

        let tables: Vec<serde_json::Value> = table_list
            .tables
            .iter()
            .map(|t| {
                let columns: Vec<serde_json::Value> = t
                    .columns
                    .as_ref()
                    .map(|cols| {
                        cols.iter()
                            .map(|c| {
                                serde_json::json!({
                                    "name": c.name,
                                    "type_name": c.type_name,
                                    "position": c.position,
                                    "comment": c.comment,
                                })
                            })
                            .collect()
                    })
                    .unwrap_or_default();

                serde_json::json!({
                    "name": t.name,
                    "catalog_name": t.catalog_name,
                    "schema_name": t.schema_name,
                    "table_type": t.table_type,
                    "comment": t.comment,
                    "columns": columns,
                })
            })
            .collect();

        let value = serde_json::to_value(&tables).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "listed tables from databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("count", tables.len().to_string()),
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
    fn list_tables_builder_serde() {
        let input = ListTablesInputBuilder::default().catalog_name("main").schema_name("default").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "listtables");
        assert_eq!(json["catalog_name"], "main");
        assert_eq!(json["schema_name"], "default");
    }
}
