use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_synthetics::SyntheticsAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetSyntheticTestInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetSyntheticTest,
    "Gets details of a Synthetic test from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetSyntheticTest,
    API_INFO,
    struct {
        public_id: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = SyntheticsAPI::with_config(client.dd_config.clone());
        let result = api.get_test(self.public_id.clone()).await.map_err(EpError::request)?;

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
    fn get_synthetic_test_builder_serde() {
        let input = GetSyntheticTestInputBuilder::default()
            .public_id("abc-123".to_string())
            .build()
            .expect("Failed to build GetSyntheticTestInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_synthetic_test");
        assert_eq!(json["public_id"], "abc-123");
    }

    #[test]
    fn get_synthetic_test_deserialize() {
        let json = serde_json::json!({"public_id": "abc-123"});
        let input: GetSyntheticTestInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.public_id, "abc-123");
    }
}
