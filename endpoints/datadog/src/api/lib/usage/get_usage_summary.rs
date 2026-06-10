use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_usage_metering::{GetUsageSummaryOptionalParams, UsageMeteringAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetUsageSummaryInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetUsageSummary,
    "Gets usage summary for the Datadog organization",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetUsageSummary,
    API_INFO,
    struct {
        start_month: String,
        end_month: Option<String>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = UsageMeteringAPI::with_config(client.dd_config.clone());

        let start_month: chrono::DateTime<chrono::Utc> =
            self.start_month.parse().map_err(|e: chrono::ParseError| EpError::parse(e.to_string()))?;

        let mut params = GetUsageSummaryOptionalParams::default();
        if let Some(ref end) = self.end_month {
            let end_month: chrono::DateTime<chrono::Utc> = end.parse().map_err(|e: chrono::ParseError| EpError::parse(e.to_string()))?;
            params = params.end_month(end_month);
        }

        let result = api.get_usage_summary(start_month, params).await.map_err(EpError::request)?;

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
    fn get_usage_summary_builder_serde() {
        let input = GetUsageSummaryInputBuilder::default()
            .start_month("2024-01-01T00:00:00Z".to_string())
            .end_month(None::<String>)
            .build()
            .expect("Failed to build GetUsageSummaryInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_usage_summary");
        assert_eq!(json["start_month"], "2024-01-01T00:00:00Z");
    }

    #[test]
    fn get_usage_summary_deserialize() {
        let json = serde_json::json!({"start_month": "2024-01-01T00:00:00Z"});
        let input: GetUsageSummaryInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.start_month, "2024-01-01T00:00:00Z");
    }

    #[test]
    fn get_usage_summary_with_end_month() {
        let json = serde_json::json!({"start_month": "2024-01-01T00:00:00Z", "end_month": "2024-03-01T00:00:00Z"});
        let input: GetUsageSummaryInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.end_month, Some("2024-03-01T00:00:00Z".to_string()));
    }
}
