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

const API_INFO: ApiInfo<DatabricksApi, EditWarehouseInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::EditWarehouse,
    "Edit a Databricks SQL warehouse configuration",
    ReqType::Write,
);

crate::databricks_endpoint! {
    EditWarehouse,
    API_INFO,
    struct {
        warehouse_id: String,
        name: Option<String>,
        cluster_size: Option<String>,
        min_num_clusters: Option<u32>,
        max_num_clusters: Option<u32>,
        auto_stop_mins: Option<u32>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let mut body = serde_json::json!({});
        let map = body.as_object_mut().expect("body is an object");
        if let Some(ref v) = self.name {
            map.insert("name".to_string(), serde_json::json!(v));
        }
        if let Some(ref v) = self.cluster_size {
            map.insert("cluster_size".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.min_num_clusters {
            map.insert("min_num_clusters".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.max_num_clusters {
            map.insert("max_num_clusters".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.auto_stop_mins {
            map.insert("auto_stop_mins".to_string(), serde_json::json!(v));
        }

        let client = context.get().await.map_err(EpError::connect)?;
        client.post(&format!("/api/2.0/sql/warehouses/{}/edit", self.warehouse_id), Some(body)).await?;

        let value = serde_json::json!({
            "success": true,
            "warehouse_id": self.warehouse_id,
            "message": "Warehouse edit requested"
        });

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "edited warehouse on databricks",
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
    fn edit_warehouse_builder_serde() {
        let input = EditWarehouseInputBuilder::default()
            .warehouse_id("wh-789")
            .name(Some("renamed-warehouse".to_string()))
            .cluster_size(None::<String>)
            .min_num_clusters(None::<u32>)
            .max_num_clusters(None::<u32>)
            .auto_stop_mins(None::<u32>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "editwarehouse");
        assert_eq!(json["warehouse_id"], "wh-789");
    }
}
