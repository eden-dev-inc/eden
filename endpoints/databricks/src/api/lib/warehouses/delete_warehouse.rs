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

const API_INFO: ApiInfo<DatabricksApi, DeleteWarehouseInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::DeleteWarehouse,
    "Delete a Databricks SQL warehouse",
    ReqType::Write,
);

crate::databricks_endpoint! {
    DeleteWarehouse,
    API_INFO,
    struct {
        warehouse_id: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        client.delete(&format!("/api/2.0/sql/warehouses/{}", self.warehouse_id)).await?;

        let value = serde_json::json!({
            "success": true,
            "warehouse_id": self.warehouse_id,
            "message": "Warehouse deleted"
        });

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "deleted warehouse on databricks",
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
    fn delete_warehouse_builder_serde() {
        let input = DeleteWarehouseInputBuilder::default().warehouse_id("wh-456").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "deletewarehouse");
        assert_eq!(json["warehouse_id"], "wh-456");
    }
}
