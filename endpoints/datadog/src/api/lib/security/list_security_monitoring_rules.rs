use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_security_monitoring::{ListSecurityMonitoringRulesOptionalParams, SecurityMonitoringAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListSecurityMonitoringRulesInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListSecurityMonitoringRules,
    "Lists all security monitoring rules in Datadog",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListSecurityMonitoringRules,
    API_INFO,
    struct {
        page_size: Option<i64>,
        page_number: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = SecurityMonitoringAPI::with_config(client.dd_config.clone());
        let mut params = ListSecurityMonitoringRulesOptionalParams::default();
        if let Some(page_size) = self.page_size {
            params = params.page_size(page_size);
        }
        if let Some(page_number) = self.page_number {
            params = params.page_number(page_number);
        }
        let result = api.list_security_monitoring_rules(params).await.map_err(EpError::request)?;

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
    fn list_security_monitoring_rules_builder_serde() {
        let input = ListSecurityMonitoringRulesInputBuilder::default()
            .page_size(Some(10i64))
            .page_number(Some(0i64))
            .build()
            .expect("Failed to build ListSecurityMonitoringRulesInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_security_monitoring_rules");
        assert_eq!(json["page_size"], 10);
        assert_eq!(json["page_number"], 0);
    }

    #[test]
    fn list_security_monitoring_rules_deserialize() {
        let json = serde_json::json!({});
        let input: ListSecurityMonitoringRulesInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.page_size.is_none());
        assert!(input.page_number.is_none());
    }
}
