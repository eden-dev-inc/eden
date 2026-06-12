use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_metrics::MetricsAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListMetricsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListMetrics,
    "Lists available metric names from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListMetrics,
    API_INFO,
    struct {
        filter: Option<String>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = MetricsAPI::with_config(client.dd_config.clone());
        // `list_metrics` requires a search query; use the filter or a wildcard to return all.
        let q = self.filter.clone().unwrap_or_else(|| "*".to_string());
        let result = api.list_metrics(q).await.map_err(EpError::request)?;

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
    fn list_metrics_builder_serde() {
        let input = ListMetricsInputBuilder::default()
            .filter(Some("system.cpu".to_string()))
            .build()
            .expect("Failed to build ListMetricsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_metrics");
        assert_eq!(json["filter"], "system.cpu");
    }

    #[test]
    fn list_metrics_deserialize() {
        let json = serde_json::json!({});
        let input: ListMetricsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.filter.is_none());
    }
}
