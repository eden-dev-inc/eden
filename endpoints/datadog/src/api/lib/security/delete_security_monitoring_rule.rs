use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_security_monitoring::SecurityMonitoringAPI;
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, DeleteSecurityMonitoringRuleInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::DeleteSecurityMonitoringRule,
    "Deletes a security monitoring rule from Datadog",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    DeleteSecurityMonitoringRule,
    API_INFO,
    struct {
        rule_id: String
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = SecurityMonitoringAPI::with_config(client.dd_config.clone());
        api.delete_security_monitoring_rule(self.rule_id.clone()).await.map_err(EpError::request)?;

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
    fn delete_security_monitoring_rule_builder_serde() {
        let input = DeleteSecurityMonitoringRuleInputBuilder::default()
            .rule_id("abc-123".to_string())
            .build()
            .expect("Failed to build DeleteSecurityMonitoringRuleInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "delete_security_monitoring_rule");
        assert_eq!(json["rule_id"], "abc-123");
    }

    #[test]
    fn delete_security_monitoring_rule_deserialize() {
        let json = serde_json::json!({"rule_id": "xyz-456"});
        let input: DeleteSecurityMonitoringRuleInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.rule_id, "xyz-456");
    }
}
