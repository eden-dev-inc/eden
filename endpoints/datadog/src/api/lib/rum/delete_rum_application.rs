use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_rum::RUMAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, DeleteRumApplicationInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::DeleteRumApplication,
    "Deletes a RUM application from Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    DeleteRumApplication,
    API_INFO,
    struct {
        id: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = RUMAPI::with_config(client.dd_config.clone());
        api.delete_rum_application(self.id.clone()).await.map_err(EpError::request)?;

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(serde_json::json!({"success": true})).to_output()) as Box<dyn EpOutput>)
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
    fn delete_rum_application_builder_serde() {
        let input = DeleteRumApplicationInputBuilder::default()
            .id("app-123".to_string())
            .build()
            .expect("Failed to build DeleteRumApplicationInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "delete_rum_application");
        assert_eq!(json["id"], "app-123");
    }

    #[test]
    fn delete_rum_application_deserialize() {
        let json = serde_json::json!({"id": "app-456"});
        let input: DeleteRumApplicationInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.id, "app-456");
    }
}
