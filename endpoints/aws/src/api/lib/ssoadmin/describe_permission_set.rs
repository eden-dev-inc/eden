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

const API_INFO: ApiInfo<AwsApi, SsoAdminDescribePermissionSetInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SsoAdminDescribePermissionSet,
    "ssoadmin_describe_permission_set",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SsoAdminDescribePermissionSet,
    API_INFO,
    struct {
        instance_arn: String,
        permission_set_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"InstanceArn": self.instance_arn, "PermissionSetArn": self.permission_set_arn});
        let result = client.execute("sso-admin", "POST", "/permissionSet", None, Some(&body_val), None).await?;

        span.add_event("received result from aws sso-admin", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SsoAdminDescribePermissionSetInputBuilder::default()
            .instance_arn("arn:aws:sso:::instance/test")
            .permission_set_arn("arn:aws:sso:::permissionSet/test/ps-abc")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ssoadmin_describe_permission_set");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"instance_arn": "arn:aws:sso:::instance/test", "permission_set_arn": "arn:aws:sso:::permissionSet/test/ps-abc"});
        let _: SsoAdminDescribePermissionSetInput = serde_json::from_value(json).unwrap();
    }
}
