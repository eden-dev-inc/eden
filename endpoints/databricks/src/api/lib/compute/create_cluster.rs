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

const API_INFO: ApiInfo<DatabricksApi, CreateClusterInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::CreateCluster, "Create a new Databricks cluster", ReqType::Write);

crate::databricks_endpoint! {
    CreateCluster,
    API_INFO,
    struct {
        cluster_name: String,
        spark_version: String,
        node_type_id: String,
        num_workers: Option<u32>,
        autoscale_min: Option<u32>,
        autoscale_max: Option<u32>,
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
            "cluster_name": self.cluster_name,
            "spark_version": self.spark_version,
            "node_type_id": self.node_type_id,
        });

        if let Some(num_workers) = self.num_workers {
            body["num_workers"] = serde_json::json!(num_workers);
        }

        if let (Some(min), Some(max)) = (self.autoscale_min, self.autoscale_max) {
            body["autoscale"] = serde_json::json!({
                "min_workers": min,
                "max_workers": max,
            });
        }

        let value = client.post("/api/2.0/clusters/create", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created cluster on databricks",
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
    fn create_cluster_builder_serde() {
        let input = CreateClusterInputBuilder::default()
            .cluster_name("test-cluster")
            .spark_version("13.3.x-scala2.12")
            .node_type_id("i3.xlarge")
            .num_workers(None::<u32>)
            .autoscale_min(None::<u32>)
            .autoscale_max(None::<u32>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createcluster");
        assert_eq!(json["cluster_name"], "test-cluster");
        assert_eq!(json["spark_version"], "13.3.x-scala2.12");
        assert_eq!(json["node_type_id"], "i3.xlarge");
    }
}
