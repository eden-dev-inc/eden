use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_workflow_automation::{ListWorkflowInstancesOptionalParams, WorkflowAutomationAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListWorkflowsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListWorkflows,
    "Lists all workflow automation workflows in Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListWorkflows,
    API_INFO,
    struct {
        workflow_id: String,
        page_size: Option<i64>,
        page_number: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = WorkflowAutomationAPI::with_config(client.dd_config.clone());
        let mut params = ListWorkflowInstancesOptionalParams::default();
        if let Some(s) = self.page_size {
            params = params.page_size(s);
        }
        if let Some(n) = self.page_number {
            params = params.page_number(n);
        }
        let result = api.list_workflow_instances(self.workflow_id.clone(), params).await.map_err(EpError::request)?;

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
    fn list_workflows_builder_serde() {
        let input = ListWorkflowsInputBuilder::default()
            .workflow_id("abc-123".to_string())
            .page_size(None::<i64>)
            .page_number(None::<i64>)
            .build()
            .expect("Failed to build ListWorkflowsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_workflows");
        assert_eq!(json["workflow_id"], "abc-123");
    }

    #[test]
    fn list_workflows_deserialize() {
        let json = serde_json::json!({"workflow_id": "abc-123"});
        let input: ListWorkflowsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.workflow_id, "abc-123");
    }
}
