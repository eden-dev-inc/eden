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

const API_INFO: ApiInfo<DatabricksApi, ListWarehousesInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::ListWarehouses,
    "List all SQL warehouses in the Databricks workspace",
    ReqType::Read,
);

#[derive(Debug, Deserialize)]
struct WarehouseListResponse {
    #[serde(default)]
    warehouses: Vec<WarehouseInfo>,
}

#[derive(Debug, Deserialize)]
struct WarehouseInfo {
    id: String,
    name: String,
    state: String,
    #[serde(default)]
    cluster_size: Option<String>,
    #[serde(default)]
    num_clusters: Option<u32>,
    #[serde(default)]
    num_active_sessions: Option<u64>,
    #[serde(default)]
    auto_stop_mins: Option<u32>,
    #[serde(default)]
    warehouse_type: Option<String>,
}

crate::databricks_endpoint! {
    ListWarehouses,
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
        let response = client.get("/api/2.0/sql/warehouses/").await?;
        let warehouse_list: WarehouseListResponse = serde_json::from_value(response).map_err(EpError::serde)?;

        let warehouses: Vec<serde_json::Value> = warehouse_list
            .warehouses
            .iter()
            .map(|w| {
                serde_json::json!({
                    "id": w.id,
                    "name": w.name,
                    "state": w.state,
                    "cluster_size": w.cluster_size,
                    "num_clusters": w.num_clusters,
                    "num_active_sessions": w.num_active_sessions,
                    "auto_stop_mins": w.auto_stop_mins,
                    "warehouse_type": w.warehouse_type,
                })
            })
            .collect();

        let value = serde_json::to_value(&warehouses).map_err(EpError::serde)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "listed warehouses from databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("count", warehouses.len().to_string()),
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
    fn list_warehouses_builder_serde() {
        let input = ListWarehousesInputBuilder::default().build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "listwarehouses");
    }
}
