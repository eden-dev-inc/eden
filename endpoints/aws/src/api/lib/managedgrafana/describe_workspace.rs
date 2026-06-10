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

const API_INFO: ApiInfo<AwsApi, ManagedGrafanaDescribeWorkspaceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ManagedGrafanaDescribeWorkspace,
    "managedgrafana_describe_workspace",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ManagedGrafanaDescribeWorkspace,
    API_INFO,
    struct {
        workspace_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/workspaces/{}", self.workspace_id);
        let result = client.execute("grafana", "GET", &path, None, None, None).await?;

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
        let input = ManagedGrafanaDescribeWorkspaceInputBuilder::default().workspace_id("g-abc123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "managedgrafana_describe_workspace");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({ "workspace_id": "g-abc123" });
        let _: ManagedGrafanaDescribeWorkspaceInput = serde_json::from_value(json).unwrap();
    }
}
