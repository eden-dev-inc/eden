use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_api_client::datadogV2::api_security_monitoring::{ListFindingsOptionalParams, SecurityMonitoringAPI};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, ListSecurityMonitoringFindingsInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::ListSecurityMonitoringFindings,
    "Lists security findings from Datadog Cloud Security",
    ReqType::Read,
    true,
);

crate::datadog_endpoint! {
    ListSecurityMonitoringFindings,
    API_INFO,
    struct {
        page_limit: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;
        let api = SecurityMonitoringAPI::with_config(client.dd_config.clone());
        let mut params = ListFindingsOptionalParams::default();
        if let Some(page_limit) = self.page_limit {
            params = params.page_limit(page_limit);
        }
        let result = api.list_findings(params).await.map_err(EpError::request)?;

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
    fn list_security_monitoring_findings_builder_serde() {
        let input = ListSecurityMonitoringFindingsInputBuilder::default()
            .page_limit(Some(50i64))
            .build()
            .expect("Failed to build ListSecurityMonitoringFindingsInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "list_security_monitoring_findings");
        assert_eq!(json["page_limit"], 50);
    }

    #[test]
    fn list_security_monitoring_findings_deserialize() {
        let json = serde_json::json!({});
        let input: ListSecurityMonitoringFindingsInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert!(input.page_limit.is_none());
    }
}
