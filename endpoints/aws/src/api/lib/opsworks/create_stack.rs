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

const API_INFO: ApiInfo<AwsApi, OpsWorksCreateStackInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::OpsWorksCreateStack, "opsworks_create_stack", ReqType::Write, true);

crate::aws_endpoint! {
    OpsWorksCreateStack,
    API_INFO,
    struct {
        name: String,
        region: String,
        service_role_arn: String,
        default_instance_profile_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "Name": self.name,
            "Region": self.region,
            "ServiceRoleArn": self.service_role_arn,
            "DefaultInstanceProfileArn": self.default_instance_profile_arn
        });
        let result = client.execute_json_target("opsworks", "OpsWorks_20130218.CreateStack", Some(&body), "1.1").await?;

        span.add_event("received result from aws opsworks", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = OpsWorksCreateStackInputBuilder::default()
            .name("my-stack")
            .region("us-east-1")
            .service_role_arn("arn:aws:iam::123:role/role")
            .default_instance_profile_arn("arn:aws:iam::123:instance-profile/profile")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "opsworks_create_stack");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-stack",
            "region": "us-east-1",
            "service_role_arn": "arn:aws:iam::123:role/role",
            "default_instance_profile_arn": "arn:aws:iam::123:instance-profile/profile"
        });
        let _: OpsWorksCreateStackInput = serde_json::from_value(json).unwrap();
    }
}
