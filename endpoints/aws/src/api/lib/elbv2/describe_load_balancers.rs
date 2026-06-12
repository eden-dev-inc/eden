use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
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

const API_INFO: ApiInfo<AwsApi, ElbV2DescribeLoadBalancersInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElbV2DescribeLoadBalancers,
    "elbv2_describe_load_balancers",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ElbV2DescribeLoadBalancers,
    API_INFO,
    struct {
        load_balancer_arns: Option<Vec<String>>,
        names: Option<Vec<String>>,
        marker: Option<String>,
        page_size: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(arns) = &self.load_balancer_arns {
            params.extend(indexed_list_params("LoadBalancerArns.member", arns));
        }
        if let Some(names) = &self.names {
            params.extend(indexed_list_params("Names.member", names));
        }
        if let Some(marker) = &self.marker {
            params.insert("Marker".to_string(), marker.clone());
        }
        if let Some(size) = self.page_size {
            params.insert("PageSize".to_string(), size.to_string());
        }
        let form_body = build_query_body("DescribeLoadBalancers", "2015-12-01", &params);
        let result = client.execute_form("elasticloadbalancing", &form_body).await?;

        span.add_event(
            "received result from aws elasticloadbalancing",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = ElbV2DescribeLoadBalancersInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elbv2_describe_load_balancers");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: ElbV2DescribeLoadBalancersInput = serde_json::from_value(json).unwrap();
    }
}
