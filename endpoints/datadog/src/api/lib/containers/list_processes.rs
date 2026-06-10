use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_processes::{ListProcessesOptionalParams, ProcessesAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListProcessesInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListProcesses,
    "Lists processes running in Datadog infrastructure",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListProcesses,
    API_INFO,
    struct {
        filter_tags: Option<String>,
        page_limit: Option<i32>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = ProcessesAPI::with_config(client.dd_config.clone());

        let mut params = ListProcessesOptionalParams::default();
        if let Some(ref v) = self.filter_tags {
            params = params.tags(v.clone());
        }
        if let Some(v) = self.page_limit {
            params = params.page_limit(v);
        }

        let result = api.list_processes(params).await.map_err(EpError::request)?;

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
    fn list_processes_builder_serde() {
        let input = ListProcessesInputBuilder::default()
            .filter_tags(Some("env:prod".to_string()))
            .page_limit(Some(100))
            .build()
            .expect("Failed to build ListProcessesInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_processes");
        assert_eq!(json["filter_tags"], "env:prod");
        assert_eq!(json["page_limit"], 100);
    }

    #[test]
    fn list_processes_deserialize() {
        let json = serde_json::json!({});
        let input: ListProcessesInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.filter_tags.is_none());
        assert!(input.page_limit.is_none());
    }
}
