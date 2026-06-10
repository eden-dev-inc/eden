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

const API_INFO: ApiInfo<DatabricksApi, EditClusterInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::EditCluster,
    "Edit an existing Databricks cluster configuration",
    ReqType::Write,
);

crate::databricks_endpoint! {
    EditCluster,
    API_INFO,
    struct {
        cluster_id: String,
        cluster_name: Option<String>,
        spark_version: Option<String>,
        node_type_id: Option<String>,
        num_workers: Option<u32>,
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
            "cluster_id": self.cluster_id,
        });

        if let Some(ref cluster_name) = self.cluster_name {
            body["cluster_name"] = serde_json::json!(cluster_name);
        }
        if let Some(ref spark_version) = self.spark_version {
            body["spark_version"] = serde_json::json!(spark_version);
        }
        if let Some(ref node_type_id) = self.node_type_id {
            body["node_type_id"] = serde_json::json!(node_type_id);
        }
        if let Some(num_workers) = self.num_workers {
            body["num_workers"] = serde_json::json!(num_workers);
        }

        let value = client.post("/api/2.0/clusters/edit", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "edited cluster on databricks",
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
    fn edit_cluster_builder_serde() {
        let input = EditClusterInputBuilder::default()
            .cluster_id("cluster-123")
            .cluster_name(None::<String>)
            .spark_version(None::<String>)
            .node_type_id(None::<String>)
            .num_workers(None::<u32>)
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "editcluster");
        assert_eq!(json["cluster_id"], "cluster-123");
    }
}
