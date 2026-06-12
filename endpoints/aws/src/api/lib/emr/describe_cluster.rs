use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, EmrDescribeClusterInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EmrDescribeCluster, "emr_describe_cluster", ReqType::Read, true);

crate::aws_endpoint! {
    EmrDescribeCluster,
    API_INFO,
    struct {
        cluster_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ClusterId".to_string(), self.cluster_id.clone());
        let form_body = build_query_body("DescribeCluster", "2009-03-31", &params);
        let result = client.execute_form("emr", &form_body).await?;

        span.add_event("received result from aws emr", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = EmrDescribeClusterInputBuilder::default().cluster_id("j-1234567890").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "emr_describe_cluster");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"cluster_id": "j-1234567890"});
        let _: EmrDescribeClusterInput = serde_json::from_value(json).unwrap();
    }
}
