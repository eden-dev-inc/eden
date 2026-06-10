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

const API_INFO: ApiInfo<DatabricksApi, GetModelVersionInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::GetModelVersion,
    "Get a specific MLflow model version",
    ReqType::Read,
);

crate::databricks_endpoint! {
    GetModelVersion,
    API_INFO,
    struct {
        name: String,
        version: String,
    }
}

impl_simple_operation!(SimpleInput, DatabricksAsync, DatabricksTx, DatabricksApi, DatabricksRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatabricksAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("databricks.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.get(&format!("/api/2.0/mlflow/model-versions/get?name={}&version={}", self.name, self.version)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "retrieved model version from databricks",
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
    fn get_model_version_builder_serde() {
        let input = GetModelVersionInputBuilder::default().name("test-model").version("1").build().expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "getmodelversion");
        assert_eq!(json["name"], "test-model");
        assert_eq!(json["version"], "1");
    }
}
