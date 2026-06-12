use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_workflow_automation::WorkflowAutomationAPI;
use datadog_api_client::datadogV2::model::WorkflowInstanceCreateRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, RunWorkflowInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::RunWorkflow,
    "Executes a workflow instance in Datadog Workflow Automation",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    RunWorkflow,
    API_INFO,
    struct {
        workflow_id: String,
        body: Value
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = WorkflowAutomationAPI::with_config(client.dd_config.clone());
        let typed_body: WorkflowInstanceCreateRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.create_workflow_instance(self.workflow_id.clone(), typed_body).await.map_err(EpError::request)?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(serde_json::to_value(result).map_err(EpError::serde)?).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatadogTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn run_workflow_builder_serde() {
        let body = serde_json::json!({"meta": {"payload": {}}});
        let input = RunWorkflowInputBuilder::default()
            .workflow_id("abc-123".to_string())
            .body(body.clone())
            .build()
            .expect("Failed to build RunWorkflowInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "run_workflow");
        assert_eq!(json["workflow_id"], "abc-123");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn run_workflow_deserialize() {
        let json = serde_json::json!({"workflow_id": "abc-123", "body": {}});
        let input: RunWorkflowInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.workflow_id, "abc-123");
    }
}
