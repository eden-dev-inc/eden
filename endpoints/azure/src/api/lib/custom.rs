use crate::api::lib::AzureApi;
use crate::api::wrapper::output::AzureJsonOutput;
use crate::request::AzureRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use azure_core::{AzureAsync, AzureTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AzureApi, CustomInput> =
    ApiInfo::new(EpKind::Azure, AzureApi::Custom, "Execute a custom Azure REST API request", ReqType::Write, true);

crate::azure_endpoint! {
    Custom,
    API_INFO,
    struct {
        method: String,
        path: String,
        api_version: String,
        body: Option<serde_json::Value>,
        base_url: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let result = if let Some(base) = &self.base_url {
            client.execute_data_plane(base, &self.method, &self.path, &self.api_version, self.body.as_ref()).await?
        } else {
            client.execute(&self.method, &self.path, &self.api_version, self.body.as_ref(), None).await?
        };

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
        let input = CustomInputBuilder::default().method("GET").path("/subscriptions").api_version("2022-12-01").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "custom");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"method": "GET", "path": "/subscriptions", "api_version": "2022-12-01"});
        let _: CustomInput = serde_json::from_value(json).unwrap();
    }
}
