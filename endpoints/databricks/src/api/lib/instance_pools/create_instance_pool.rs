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

const API_INFO: ApiInfo<DatabricksApi, CreateInstancePoolInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::CreateInstancePool,
    "Create a Databricks instance pool",
    ReqType::Write,
);

crate::databricks_endpoint! {
    CreateInstancePool,
    API_INFO,
    struct {
        instance_pool_name: String,
        node_type_id: String,
        min_idle_instances: Option<u32>,
        max_capacity: Option<u32>,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;

        let mut body = serde_json::json!({
            "instance_pool_name": self.instance_pool_name,
            "node_type_id": self.node_type_id,
        });

        let map = body.as_object_mut().expect("body is an object");
        if let Some(v) = self.min_idle_instances {
            map.insert("min_idle_instances".to_string(), serde_json::json!(v));
        }
        if let Some(v) = self.max_capacity {
            map.insert("max_capacity".to_string(), serde_json::json!(v));
        }

        let value = client.post("/api/2.0/instance-pools/create", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created instance pool on databricks",
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
    fn create_instance_pool_builder_serde() {
        let input = CreateInstancePoolInputBuilder::default()
            .instance_pool_name("my-pool")
            .node_type_id("i3.xlarge")
            .min_idle_instances(Some(2u32))
            .max_capacity(Some(10u32))
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createinstancepool");
        assert_eq!(json["instance_pool_name"], "my-pool");
        assert_eq!(json["node_type_id"], "i3.xlarge");
        assert_eq!(json["min_idle_instances"], 2);
        assert_eq!(json["max_capacity"], 10);
    }
}
