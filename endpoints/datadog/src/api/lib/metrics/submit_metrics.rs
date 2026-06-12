use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_metrics::{MetricsAPI, SubmitMetricsOptionalParams};
use datadog_api_client::datadogV1::model::MetricsPayload;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, SubmitMetricsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::SubmitMetrics,
    "Submits timeseries metric data to Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    SubmitMetrics,
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
        let api = MetricsAPI::with_config(client.dd_config.clone());
        let typed_body: MetricsPayload = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.submit_metrics(typed_body, SubmitMetricsOptionalParams::default()).await.map_err(EpError::request)?;

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
    fn submit_metrics_builder_serde() {
        let body = serde_json::json!({
            "series": [{
                "metric": "system.load.1",
                "points": [[1609459200, 1.5]],
                "type": "gauge"
            }]
        });
        let input = SubmitMetricsInputBuilder::default().body(body.clone()).build().expect("Failed to build SubmitMetricsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "submit_metrics");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn submit_metrics_deserialize() {
        let json = serde_json::json!({
            "body": {"series": []}
        });
        let input: SubmitMetricsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.body, serde_json::json!({"series": []}));
    }
}
