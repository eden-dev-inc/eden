use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_usage_metering::{GetMonthlyUsageAttributionOptionalParams, UsageMeteringAPI};
use datadog_api_client::datadogV1::model::MonthlyUsageAttributionSupportedMetrics;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetMonthlyUsageAttributionInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetMonthlyUsageAttribution,
    "Gets monthly usage attribution data from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetMonthlyUsageAttribution,
    API_INFO,
    struct {
        start_month: String,
        fields: String
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

        let fields: MonthlyUsageAttributionSupportedMetrics =
            serde_json::from_value(serde_json::Value::String(self.fields.clone())).map_err(EpError::serde)?;

        let result = api
            .get_monthly_usage_attribution(start_month, fields, GetMonthlyUsageAttributionOptionalParams::default())
            .await
            .map_err(EpError::request)?;

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
    fn get_monthly_usage_attribution_builder_serde() {
        let input = GetMonthlyUsageAttributionInputBuilder::default()
            .start_month("2024-01-01T00:00:00Z".to_string())
            .fields("infra_host_usage".to_string())
            .build()
            .expect("Failed to build GetMonthlyUsageAttributionInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_monthly_usage_attribution");
        assert_eq!(json["start_month"], "2024-01-01T00:00:00Z");
        assert_eq!(json["fields"], "infra_host_usage");
    }

    #[test]
    fn get_monthly_usage_attribution_deserialize() {
        let json = serde_json::json!({"start_month": "2024-01-01T00:00:00Z", "fields": "infra_host_usage"});
        let input: GetMonthlyUsageAttributionInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.start_month, "2024-01-01T00:00:00Z");
        assert_eq!(input.fields, "infra_host_usage");
    }
}
