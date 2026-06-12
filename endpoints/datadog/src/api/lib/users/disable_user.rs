use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_users::UsersAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, DisableUserInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::DisableUser,
    "Disables a user in the Datadog organization",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    DisableUser,
    API_INFO,
    struct {
        user_id: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = UsersAPI::with_config(client.dd_config.clone());
        api.disable_user(self.user_id.clone()).await.map_err(EpError::request)?;

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
    fn disable_user_builder_serde() {
        let input = DisableUserInputBuilder::default().user_id("user-123").build().expect("Failed to build DisableUserInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "disable_user");
        assert_eq!(json["user_id"], "user-123");
    }

    #[test]
    fn disable_user_deserialize() {
        let json = serde_json::json!({"user_id": "user-456"});
        let input: DisableUserInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.user_id, "user-456");
    }
}
