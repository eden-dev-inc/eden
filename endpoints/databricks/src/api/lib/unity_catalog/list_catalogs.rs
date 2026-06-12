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

const API_INFO: ApiInfo<DatabricksApi, ListCatalogsInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::ListCatalogs,
    "List all catalogs available in the Databricks Unity Catalog",
    ReqType::Read,
);

#[derive(Debug, Deserialize)]
struct CatalogListResponse {
    #[serde(default)]
    catalogs: Vec<CatalogInfo>,
}

#[derive(Debug, Deserialize)]
struct CatalogInfo {
    name: String,
    #[serde(default)]
    comment: Option<String>,
    #[serde(default)]
    catalog_type: Option<String>,
}

crate::databricks_endpoint! {
    ListCatalogs,
    API_INFO,
    struct {}
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        let response = client.get("/api/2.1/unity-catalog/catalogs").await?;
        let catalog_list: CatalogListResponse = serde_json::from_value(response).map_err(EpError::serde)?;

        let catalogs: Vec<serde_json::Value> = catalog_list
            .catalogs
            .iter()
            .map(|c| {
                serde_json::json!({
                    "name": c.name,
                    "comment": c.comment,
                    "catalog_type": c.catalog_type,
                })
            })
            .collect();

        let value = serde_json::to_value(&catalogs).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "listed catalogs from databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("count", catalogs.len().to_string()),
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
    fn list_catalogs_builder_serde() {
        let input = ListCatalogsInputBuilder::default().build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "listcatalogs");
    }
}
