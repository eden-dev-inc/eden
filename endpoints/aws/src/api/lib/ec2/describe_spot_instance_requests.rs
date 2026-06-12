use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, filters_to_params, indexed_list_params};
use crate::api::lib::types::AwsFilter;
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

const API_INFO: ApiInfo<AwsApi, Ec2DescribeSpotInstanceRequestsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Ec2DescribeSpotInstanceRequests,
    "ec2_describe_spot_instance_requests",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    Ec2DescribeSpotInstanceRequests,
    API_INFO,
    struct {
        spot_instance_request_ids: Option<Vec<String>>,
        filters: Option<Vec<AwsFilter>>,
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(ids) = &self.spot_instance_request_ids {
            params.extend(indexed_list_params("SpotInstanceRequestId", ids));
        }
        if let Some(filters) = &self.filters {
            params.extend(filters_to_params(filters));
        }
        if let Some(max) = self.max_results {
            params.insert("MaxResults".to_string(), max.to_string());
        }
        if let Some(token) = &self.next_token {
            params.insert("NextToken".to_string(), token.clone());
        }
        let form_body = build_query_body("DescribeSpotInstanceRequests", "2016-11-15", &params);
        let result = client.execute_form("ec2", &form_body).await?;

        span.add_event("received result from aws ec2", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = Ec2DescribeSpotInstanceRequestsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_describe_spot_instance_requests");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: Ec2DescribeSpotInstanceRequestsInput = serde_json::from_value(json).unwrap();
    }
}
