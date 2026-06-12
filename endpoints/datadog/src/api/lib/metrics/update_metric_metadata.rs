use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_metrics::MetricsAPI;
use datadog_api_client::datadogV1::model::MetricMetadata;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, UpdateMetricMetadataInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::UpdateMetricMetadata,
    "Updates metadata for a specific metric in Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    UpdateMetricMetadata,
    API_INFO,
    struct {
        metric_name: String,
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
        let typed_body: MetricMetadata = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.update_metric_metadata(self.metric_name.clone(), typed_body).await.map_err(EpError::request)?;

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
    fn update_metric_metadata_builder_serde() {
        let body = serde_json::json!({"description": "CPU usage"});
        let input = UpdateMetricMetadataInputBuilder::default()
            .metric_name("system.cpu.user".to_string())
            .body(body.clone())
            .build()
            .expect("Failed to build UpdateMetricMetadataInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "update_metric_metadata");
        assert_eq!(json["metric_name"], "system.cpu.user");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn update_metric_metadata_deserialize() {
        let json = serde_json::json!({"metric_name": "system.cpu.user", "body": {"description": "CPU usage"}});
        let input: UpdateMetricMetadataInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.metric_name, "system.cpu.user");
        assert_eq!(input.body["description"], "CPU usage");
    }
}
