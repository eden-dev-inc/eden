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

const API_INFO: ApiInfo<AwsApi, IotTwinMakerCreateWorkspaceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::IotTwinMakerCreateWorkspace,
    "iottwinmaker_create_workspace",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    IotTwinMakerCreateWorkspace,
    API_INFO,
    struct {
        workspace_id: String,
        s3_location: String,
        role: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/workspaces/{}", self.workspace_id);
        let body_val = serde_json::json!({"s3Location": self.s3_location, "role": self.role});
        let result = client.execute("iottwinmaker", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws iottwinmaker",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = IotTwinMakerCreateWorkspaceInputBuilder::default()
            .workspace_id("my-workspace")
            .s3_location("arn:aws:s3:::my-bucket")
            .role("arn:aws:iam::123456789012:role/my-role")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iottwinmaker_create_workspace");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"workspace_id": "my-workspace", "s3_location": "arn:aws:s3:::my-bucket", "role": "arn:aws:iam::123456789012:role/my-role"});
        let _: IotTwinMakerCreateWorkspaceInput = serde_json::from_value(json).unwrap();
    }
}
