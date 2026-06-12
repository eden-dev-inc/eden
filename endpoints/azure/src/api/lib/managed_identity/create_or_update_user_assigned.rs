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

const API_VERSION: &str = "2023-01-31";

const API_INFO: ApiInfo<AzureApi, ManagedIdentityCreateOrUpdateUserAssignedInput> = ApiInfo::new(
    EpKind::Azure,
    AzureApi::ManagedIdentityCreateOrUpdateUserAssigned,
    "Create or update user assigned managed identity",
    ReqType::Write,
    true,
);

crate::azure_endpoint! {
    ManagedIdentityCreateOrUpdateUserAssigned,
    API_INFO,
    struct {
        subscription_id: Option<String>,
        resource_group: String,
        identity_name: String,
        location: String,
        properties: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AzureAsync, AzureTx, AzureApi, AzureRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AzureAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("azure.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let sub = self
            .subscription_id
            .as_deref()
            .or(client.subscription_id())
            .ok_or_else(|| EpError::request("subscription_id required"))?;

        let path = format!(
            "/subscriptions/{}/resourceGroups/{}/providers/Microsoft.ManagedIdentity/userAssignedIdentities/{}",
            sub, self.resource_group, self.identity_name
        );

        let mut body = serde_json::json!({
            "location": self.location
        });
        if let Some(props) = &self.properties {
            body["properties"] = props.clone();
        }

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
        let input = ManagedIdentityCreateOrUpdateUserAssignedInputBuilder::default()
            .resource_group("my-rg")
            .identity_name("my-identity")
            .location("eastus")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "managed_identity_create_or_update_user_assigned");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_group": "my-rg", "identity_name": "my-identity", "location": "eastus"});
        let _: ManagedIdentityCreateOrUpdateUserAssignedInput = serde_json::from_value(json).unwrap();
    }
}
