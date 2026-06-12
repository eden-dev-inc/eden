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

const API_INFO: ApiInfo<AwsApi, EksUpdateNodegroupConfigInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::EksUpdateNodegroupConfig,
    "Updates an EKS managed node group configuration",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    EksUpdateNodegroupConfig,
    API_INFO,
    struct {
        cluster_name: String,
        nodegroup_name: String,
        scaling_config: Option<serde_json::Value>,
        labels: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::json!({});
        if let Some(sc) = &self.scaling_config {
            body["scalingConfig"] = sc.clone();
        }
        if let Some(l) = &self.labels {
            body["labels"] = l.clone();
        }
        let path = format!("/clusters/{}/node-groups/{}/update-config", self.cluster_name, self.nodegroup_name);
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
        let input = EksUpdateNodegroupConfigInputBuilder::default().cluster_name("c").nodegroup_name("ng").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "eks_update_nodegroup_config");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cluster_name": "c", "nodegroup_name": "ng"});
        let _: EksUpdateNodegroupConfigInput = serde_json::from_value(json).unwrap();
    }
}
