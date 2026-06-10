use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV1::api_key_management::KeyManagementAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, GetAppKeyInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::GetAppKey,
    "Retrieves a specific application key from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    GetAppKey,
    API_INFO,
    struct {
        key: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = KeyManagementAPI::with_config(client.dd_config.clone());
        let result = api.get_application_key(self.key.clone()).await.map_err(EpError::request)?;

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
    fn get_app_key_builder_serde() {
        let input = GetAppKeyInputBuilder::default().key("xyz789".to_string()).build().expect("Failed to build GetAppKeyInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "get_app_key");
        assert_eq!(json["key"], "xyz789");
    }

    #[test]
    fn get_app_key_deserialize() {
        let json = serde_json::json!({"key": "xyz789"});
        let input: GetAppKeyInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.key, "xyz789");
    }
}
