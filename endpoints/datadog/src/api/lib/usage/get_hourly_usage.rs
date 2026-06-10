use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_usage_metering::{GetHourlyUsageOptionalParams, UsageMeteringAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetHourlyUsageInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetHourlyUsage,
    "Gets hourly usage data from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetHourlyUsage,
    API_INFO,
    struct {
        product_families: String,
        start_hr: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = UsageMeteringAPI::with_config(client.dd_config.clone());

        let start_hr: chrono::DateTime<chrono::Utc> =
            self.start_hr.parse().map_err(|e: chrono::ParseError| EpError::parse(e.to_string()))?;

        let result = api
            .get_hourly_usage(start_hr, self.product_families.clone(), GetHourlyUsageOptionalParams::default())
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
    fn get_hourly_usage_builder_serde() {
        let input = GetHourlyUsageInputBuilder::default()
            .product_families("infra_hosts".to_string())
            .start_hr("2024-01-01T00:00:00Z".to_string())
            .build()
            .expect("Failed to build GetHourlyUsageInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_hourly_usage");
        assert_eq!(json["product_families"], "infra_hosts");
        assert_eq!(json["start_hr"], "2024-01-01T00:00:00Z");
    }

    #[test]
    fn get_hourly_usage_deserialize() {
        let json = serde_json::json!({"product_families": "logs", "start_hr": "2024-01-01T00:00:00Z"});
        let input: GetHourlyUsageInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.product_families, "logs");
        assert_eq!(input.start_hr, "2024-01-01T00:00:00Z");
    }
}
