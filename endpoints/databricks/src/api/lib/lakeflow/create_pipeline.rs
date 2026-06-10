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

const API_INFO: ApiInfo<DatabricksApi, CreatePipelineInput> = ApiInfo::new(
    EpKind::Databricks,
    DatabricksApi::CreatePipeline,
    "Create a Delta Live Tables pipeline",
    ReqType::Write,
);

crate::databricks_endpoint! {
    CreatePipeline,
    API_INFO,
    struct {
        name: String,
        storage: Option<String>,
        target: Option<String>,
        continuous: Option<bool>,
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
            "name": self.name,
        });

        if let Some(storage) = &self.storage {
            body["storage"] = serde_json::Value::String(storage.clone());
        }

        if let Some(target) = &self.target {
            body["target"] = serde_json::Value::String(target.clone());
        }

        if let Some(continuous) = &self.continuous {
            body["continuous"] = serde_json::Value::Bool(*continuous);
        }

        let value = client.post("/api/2.0/pipelines", Some(body)).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();
        span.add_event(
            "created pipeline on databricks",
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
    fn create_pipeline_builder_serde() {
        let input = CreatePipelineInputBuilder::default()
            .name("my-pipeline")
            .storage(Some("/data/storage".to_string()))
            .target(Some("my_target".to_string()))
            .continuous(Some(true))
            .build()
            .expect("Failed to build");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "createpipeline");
        assert_eq!(json["name"], "my-pipeline");
        assert_eq!(json["storage"], "/data/storage");
        assert_eq!(json["target"], "my_target");
        assert_eq!(json["continuous"], true);
    }
}
