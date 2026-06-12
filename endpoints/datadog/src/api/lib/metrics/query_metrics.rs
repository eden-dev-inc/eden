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

const API_INFO: ApiInfo<DatadogApi, QueryMetricsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::QueryMetrics,
    "Queries metric data points over a time period from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    QueryMetrics,
    API_INFO,
    struct {
        from: i64,
        to: i64,
        query: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = MetricsAPI::with_config(client.dd_config.clone());
        let result = api.query_metrics(self.from, self.to, self.query.clone()).await.map_err(EpError::request)?;

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
    fn query_metrics_builder_serde() {
        let input = QueryMetricsInputBuilder::default()
            .from(1609459200i64)
            .to(1609545600i64)
            .query("avg:system.cpu.user{*}".to_string())
            .build()
            .expect("Failed to build QueryMetricsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "query_metrics");
        assert_eq!(json["from"], 1609459200);
        assert_eq!(json["to"], 1609545600);
        assert_eq!(json["query"], "avg:system.cpu.user{*}");
    }

    #[test]
    fn query_metrics_deserialize() {
        let json = serde_json::json!({
            "from": 1609459200,
            "to": 1609545600,
            "query": "avg:system.cpu.user{*}"
        });
        let input: QueryMetricsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.from, 1609459200);
        assert_eq!(input.to, 1609545600);
        assert_eq!(input.query, "avg:system.cpu.user{*}");
    }
}
