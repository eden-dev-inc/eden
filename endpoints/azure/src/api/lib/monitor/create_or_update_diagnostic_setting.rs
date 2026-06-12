use crate::api::lib::AzureApi;
use crate::api::wrapper::output::AzureJsonOutput;
use crate::request::AzureRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use azure_core::{AzureAsync, AzureTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_VERSION: &str = "2021-05-01-preview";

const API_INFO: ApiInfo<AzureApi, MonitorCreateOrUpdateDiagnosticSettingInput> = ApiInfo::new(
    EpKind::Azure,
    AzureApi::MonitorCreateOrUpdateDiagnosticSetting,
    "Create or update a diagnostic setting",
    ReqType::Write,
    true,
);

crate::azure_endpoint! {
    MonitorCreateOrUpdateDiagnosticSetting,
    API_INFO,
    struct {
        resource_uri: String,
        setting_name: String,
        properties: Value
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!(
            "/{}/providers/Microsoft.Insights/diagnosticSettings/{}",
            self.resource_uri.trim_start_matches('/'),
            self.setting_name
        );

        let body = serde_json::json!({
            "properties": self.properties
        });

        let result = client.execute("PUT", &path, API_VERSION, Some(&body), None).await?;

        span.add_event("received result from azure", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AzureJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AzureTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = MonitorCreateOrUpdateDiagnosticSettingInputBuilder::default()
            .resource_uri("/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1")
            .setting_name("my-setting")
            .properties(serde_json::json!({"storageAccountId": "/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Storage/storageAccounts/sa1"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "monitor_create_or_update_diagnostic_setting");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "resource_uri": "/subscriptions/sub1/resourceGroups/rg1/providers/Microsoft.Compute/virtualMachines/vm1",
            "setting_name": "my-setting",
            "properties": {"storageAccountId": "test"}
        });
        let _: MonitorCreateOrUpdateDiagnosticSettingInput = serde_json::from_value(json).unwrap();
    }
}
