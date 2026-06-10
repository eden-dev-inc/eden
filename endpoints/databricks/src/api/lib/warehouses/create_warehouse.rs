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

const API_INFO: ApiInfo<DatabricksApi, CreateWarehouseInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::CreateWarehouse,
    "Create a new Databricks SQL warehouse",
    ReqType::Write,
);

crate::databricks_endpoint! {
    CreateWarehouse,
    API_INFO,
    struct {
        name: String,
        cluster_size: String,
        min_num_clusters: Option<u32>,
        max_num_clusters: Option<u32>,
        auto_stop_mins: Option<u32>,
        warehouse_type: Option<String>,
        enable_serverless_compute: Option<bool>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let mut body = serde_json::json!({
            "name": self.name,
            "cluster_size": self.cluster_size,
        });

        let map = body.as_object_mut().expect("body is an object");
        if let Some(v) = self.min_num_clusters {
            map.insert("min_num_clusters".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.max_num_clusters {
            map.insert("max_num_clusters".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.auto_stop_mins {
            map.insert("auto_stop_mins".to_string(), serde_json::json!(v));
        }
        if let Some(ref v) = self.warehouse_type {
            map.insert("warehouse_type".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.enable_serverless_compute {
            map.insert("enable_serverless_compute".to_string(), serde_json::json!(v));
        }

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.post("/api/2.0/sql/warehouses/", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created warehouse on databricks",
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
    fn create_warehouse_builder_serde() {
        let input = CreateWarehouseInputBuilder::default()
            .name("test-warehouse")
            .cluster_size("2X-Small")
            .min_num_clusters(None::<u32>)
            .max_num_clusters(None::<u32>)
            .auto_stop_mins(None::<u32>)
            .warehouse_type(None::<String>)
            .enable_serverless_compute(None::<bool>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createwarehouse");
        assert_eq!(json["name"], "test-warehouse");
        assert_eq!(json["cluster_size"], "2X-Small");
    }
}
