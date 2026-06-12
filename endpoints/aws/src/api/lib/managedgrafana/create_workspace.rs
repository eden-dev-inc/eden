use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, ManagedGrafanaCreateWorkspaceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ManagedGrafanaCreateWorkspace,
    "managedgrafana_create_workspace",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ManagedGrafanaCreateWorkspace,
    API_INFO,
    struct {
        account_access_type: String,
        authentication_providers: Vec<String>,
        permission_type: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "accountAccessType": self.account_access_type,
            "authenticationProviders": self.authentication_providers,
            "permissionType": self.permission_type
        });
        let result = client.execute("grafana", "POST", "/workspaces", None, Some(&body), None).await?;

        span.add_event("received result from aws grafana", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = ManagedGrafanaCreateWorkspaceInputBuilder::default()
            .account_access_type("CURRENT_ACCOUNT")
            .authentication_providers(vec!["AWS_SSO".to_string()])
            .permission_type("SERVICE_MANAGED")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "managedgrafana_create_workspace");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "account_access_type": "CURRENT_ACCOUNT",
            "authentication_providers": ["AWS_SSO"],
            "permission_type": "SERVICE_MANAGED"
        });
        let _: ManagedGrafanaCreateWorkspaceInput = serde_json::from_value(json).unwrap();
    }
}
