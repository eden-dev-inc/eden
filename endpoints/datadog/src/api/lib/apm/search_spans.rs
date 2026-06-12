use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_spans::SpansAPI;
use datadog_api_client::datadogV2::model::SpansListRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, SearchSpansInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::SearchSpans,
    "Searches and retrieves APM spans from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    SearchSpans,
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
        let api = SpansAPI::with_config(client.dd_config.clone());
        let typed_body: SpansListRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.list_spans(typed_body).await.map_err(EpError::request)?;

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
    fn search_spans_builder_serde() {
        let body = serde_json::json!({
            "filter": {"query": "service:web @duration:>1s", "from": "now-1h", "to": "now"}
        });
        let input = SearchSpansInputBuilder::default().body(body.clone()).build().expect("Failed to build SearchSpansInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "search_spans");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn search_spans_deserialize() {
        let json = serde_json::json!({"body": {"filter": {"query": "*"}}});
        let input: SearchSpansInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.body["filter"]["query"], "*");
    }
}
