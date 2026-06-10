use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_monitors::MonitorsAPI;
use datadog_api_client::datadogV2::model::MonitorConfigPolicyCreateRequest;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, CreateMonitorConfigPolicyInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::CreateMonitorConfigPolicy,
    "Creates a monitor configuration policy in Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    CreateMonitorConfigPolicy,
    API_INFO,
    struct {
        body: Value
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = MonitorsAPI::with_config(client.dd_config.clone());
        let typed_body: MonitorConfigPolicyCreateRequest = serde_json::from_value(self.body.clone()).map_err(EpError::serde)?;
        let result = api.create_monitor_config_policy(typed_body).await.map_err(EpError::request)?;

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
    fn create_monitor_config_policy_builder_serde() {
        let body = serde_json::json!({
            "data": {
                "type": "monitor-config-policy",
                "attributes": {"policy_type": "tag", "policy": {"tag_key": "env", "tag_key_required": true, "valid_tag_values": ["prod"]}}
            }
        });
        let input = CreateMonitorConfigPolicyInputBuilder::default()
            .body(body.clone())
            .build()
            .expect("Failed to build CreateMonitorConfigPolicyInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "create_monitor_config_policy");
        assert_eq!(json["body"], body);
    }

    #[test]
    fn create_monitor_config_policy_deserialize() {
        let json = serde_json::json!({"body": {"data": {"type": "monitor-config-policy"}}});
        let input: CreateMonitorConfigPolicyInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.body["data"]["type"], "monitor-config-policy");
    }
}
