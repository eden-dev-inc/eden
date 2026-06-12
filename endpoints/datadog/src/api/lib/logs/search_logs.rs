use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_logs::{ListLogsOptionalParams, LogsAPI};
use datadog_api_client::datadogV2::model::LogsListRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, SearchLogsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::SearchLogs,
    "Searches and retrieves log events from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    SearchLogs,
    API_INFO,
    struct {
        body: Value
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = LogsAPI::with_config(client.dd_config.clone());
        let typed_body: LogsListRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.list_logs(ListLogsOptionalParams::default().body(typed_body)).await.map_err(EpError::request)?;

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
    fn search_logs_builder_serde() {
        let body = serde_json::json!({
            "filter": {"query": "service:web", "from": "now-1h", "to": "now"}
        });
        let input = SearchLogsInputBuilder::default().body(body.clone()).build().expect("Failed to build SearchLogsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "search_logs");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn search_logs_deserialize() {
        let json = serde_json::json!({"body": {"filter": {"query": "*"}}});
        let input: SearchLogsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.body["filter"]["query"], "*");
    }
}
