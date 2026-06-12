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

const API_INFO: ApiInfo<AwsApi, CloudHsmV2CreateClusterInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CloudHsmV2CreateCluster, "Creates a CloudHSM v2 cluster", ReqType::Write, true);

crate::aws_endpoint! {
    CloudHsmV2CreateCluster,
    API_INFO,
    struct {
        hsm_type: String,
        subnet_ids: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"HsmType": self.hsm_type, "SubnetIds": self.subnet_ids});
        let result = client.execute("cloudhsmv2", "POST", "/v2/clusters", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws cloudhsmv2",
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
        let input = CloudHsmV2CreateClusterInputBuilder::default()
            .hsm_type("hsm1.medium")
            .subnet_ids(vec!["subnet-abc123".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudhsmv2_create_cluster");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"hsm_type": "hsm1.medium", "subnet_ids": ["subnet-abc123"]});
        let _: CloudHsmV2CreateClusterInput = serde_json::from_value(json).unwrap();
    }
}
