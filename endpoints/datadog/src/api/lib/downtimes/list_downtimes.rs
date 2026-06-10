use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_downtimes::{DowntimesAPI, ListDowntimesOptionalParams};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListDowntimesInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListDowntimes,
    "Lists all scheduled downtimes from Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListDowntimes,
    API_INFO,
    struct {
        current_only: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = DowntimesAPI::with_config(client.dd_config.clone());
        let mut params = ListDowntimesOptionalParams::default();
        if let Some(current) = self.current_only {
            params = params.current_only(current);
        }
        let result = api.list_downtimes(params).await.map_err(EpError::request)?;

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
    fn list_downtimes_builder_serde() {
        let input = ListDowntimesInputBuilder::default().current_only(Some(true)).build().expect("Failed to build ListDowntimesInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_downtimes");
        assert_eq!(json["current_only"], true);
    }

    #[test]
    fn list_downtimes_deserialize() {
        let json = serde_json::json!({});
        let input: ListDowntimesInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.current_only.is_none());
    }
}
