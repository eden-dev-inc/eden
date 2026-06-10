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

const API_INFO: ApiInfo<DatabricksApi, DeleteExperimentInput> =
    ApiInfo::new(EpKind::Databricks, DatabricksApi::DeleteExperiment, "Delete an MLflow experiment", ReqType::Write);

crate::databricks_endpoint! {
    DeleteExperiment,
    API_INFO,
    struct {
        experiment_id: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let body = serde_json::json!({
            "experiment_id": self.experiment_id,
        });

        let client = context.get().await.map_err(EpError::connect)?;
        let _value = client.post("/api/2.0/mlflow/experiments/delete", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "deleted mlflow experiment on databricks",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        let result = serde_json::json!({"success": true, "experiment_id": self.experiment_id, "message": "Experiment deleted"});
        Ok(Box::new(DatabricksJsonOutput(result).to_output()) as Box<dyn EpOutput>)
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
    fn delete_experiment_builder_serde() {
        let input = DeleteExperimentInputBuilder::default().experiment_id("123").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "deleteexperiment");
        assert_eq!(json["experiment_id"], "123");
    }
}
