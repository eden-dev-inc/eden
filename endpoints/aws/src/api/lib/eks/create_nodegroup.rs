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

const API_INFO: ApiInfo<AwsApi, EksCreateNodegroupInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::EksCreateNodegroup,
    "Creates a managed node group for an EKS cluster",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    EksCreateNodegroup,
    API_INFO,
    struct {
        cluster_name: String,
        nodegroup_name: String,
        node_role: String,
        subnets: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "nodegroupName": self.nodegroup_name,
            "nodeRole": self.node_role,
            "subnets": self.subnets
        });
        let path = format!("/clusters/{}/node-groups", self.cluster_name);
        let result = client.execute("eks", "POST", &path, None, Some(&body), None).await?;

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
        let input = EksCreateNodegroupInputBuilder::default()
            .cluster_name("c")
            .nodegroup_name("ng")
            .node_role("arn:aws:iam::123:role/r")
            .subnets(vec!["s1".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eks_create_nodegroup");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cluster_name": "c", "nodegroup_name": "ng", "node_role": "r", "subnets": ["s1"]});
        let _: EksCreateNodegroupInput = serde_json::from_value(json).unwrap();
    }
}
