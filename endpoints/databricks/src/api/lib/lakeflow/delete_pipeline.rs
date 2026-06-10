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

const API_INFO: ApiInfo<DatabricksApi, DeletePipelineInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::DeletePipeline,
    "Delete a Delta Live Tables pipeline",
    ReqType::Write,
);

crate::databricks_endpoint! {
    DeletePipeline,
    API_INFO,
    struct {
        pipeline_id: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        client.delete(&format!("/api/2.0/pipelines/{}", self.pipeline_id)).await?;

        let value = serde_json::json!({
            "success": true,
            "pipeline_id": self.pipeline_id,
            "message": "Pipeline deleted"
        });

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "deleted pipeline on databricks",
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
    fn delete_pipeline_builder_serde() {
        let input = DeletePipelineInputBuilder::default().pipeline_id("pipe-123").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "deletepipeline");
        assert_eq!(json["pipeline_id"], "pipe-123");
    }
}
