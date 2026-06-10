use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, EksCreateClusterInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EksCreateCluster, "Creates an EKS cluster", ReqType::Write, true);

crate::aws_endpoint! {
    EksCreateCluster,
    API_INFO,
    struct {
        name: String,
        role_arn: String,
        resources_vpc_config: Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "name": self.name,
            "roleArn": self.role_arn,
            "resourcesVpcConfig": self.resources_vpc_config
        });
        let result = client.execute("eks", "POST", "/clusters", None, Some(&body), None).await?;

        span.add_event("received result from aws eks", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = EksCreateClusterInputBuilder::default()
            .name("c")
            .role_arn("arn:aws:iam::123:role/r")
            .resources_vpc_config(serde_json::json!({"subnetIds": ["s1"]}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eks_create_cluster");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "c", "role_arn": "arn", "resources_vpc_config": {}});
        let _: EksCreateClusterInput = serde_json::from_value(json).unwrap();
    }
}
