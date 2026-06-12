use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_notebooks::NotebooksAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, DeleteNotebookInput> =
    ApiInfo::new(EpKind::Datadog, DatadogApi::DeleteNotebook, "Deletes a notebook from Datadog", ReqType::Write, true);

crate::datadog_endpoint! {
    DeleteNotebook,
    API_INFO,
    struct {
        notebook_id: i64
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = NotebooksAPI::with_config(client.dd_config.clone());
        api.delete_notebook(self.notebook_id).await.map_err(EpError::request)?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(serde_json::json!({"success": true})).to_output()) as Box<dyn EpOutput>)
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
    fn delete_notebook_builder_serde() {
        let input = DeleteNotebookInputBuilder::default().notebook_id(12345i64).build().expect("Failed to build DeleteNotebookInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "delete_notebook");
        assert_eq!(json["notebook_id"], 12345);
    }

    #[test]
    fn delete_notebook_deserialize() {
        let json = serde_json::json!({"notebook_id": 99});
        let input: DeleteNotebookInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.notebook_id, 99);
    }
}
