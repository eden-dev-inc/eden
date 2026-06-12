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

const API_VERSION: &str = "2022-04-01";

const API_INFO: ApiInfo<AzureApi, AuthorizationCreateRoleAssignmentInput> = ApiInfo::new(
    EpKind::Azure,
    AzureApi::AuthorizationCreateRoleAssignment,
    "Create a role assignment",
    ReqType::Write,
    true,
);

crate::azure_endpoint! {
    AuthorizationCreateRoleAssignment,
    API_INFO,
    struct {
        scope: String,
        role_assignment_name: String,
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
            "/{}/providers/Microsoft.Authorization/roleAssignments/{}",
            self.scope.trim_start_matches('/'),
            self.role_assignment_name
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
        let input = AuthorizationCreateRoleAssignmentInputBuilder::default()
            .scope("/subscriptions/00000000-0000-0000-0000-000000000000")
            .role_assignment_name("my-assignment")
            .properties(serde_json::json!({"roleDefinitionId": "/providers/Microsoft.Authorization/roleDefinitions/acdd72a7"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "authorization_create_role_assignment");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "scope": "/subscriptions/00000000-0000-0000-0000-000000000000",
            "role_assignment_name": "my-assignment",
            "properties": {"roleDefinitionId": "test"}
        });
        let _: AuthorizationCreateRoleAssignmentInput = serde_json::from_value(json).unwrap();
    }
}
