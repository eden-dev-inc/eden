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

const API_INFO: ApiInfo<AwsApi, AutoScalingDescribeAutoScalingGroupsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::AutoScalingDescribeAutoScalingGroups,
    "autoscaling_describe_auto_scaling_groups",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    AutoScalingDescribeAutoScalingGroups,
    API_INFO,
    struct {
        auto_scaling_group_names: Option<Vec<String>>,
        next_token: Option<String>,
        max_records: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(names) = &self.auto_scaling_group_names {
            params.extend(indexed_list_params("AutoScalingGroupName", names));
        }
        if let Some(token) = &self.next_token {
            params.insert("NextToken".to_string(), token.clone());
        }
        if let Some(max) = self.max_records {
            params.insert("MaxRecords".to_string(), max.to_string());
        }
        let form_body = build_query_body("DescribeAutoScalingGroups", "2011-01-01", &params);
        let result = client.execute_form("autoscaling", &form_body).await?;

        span.add_event(
            "received result from aws autoscaling",
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
        let input = AutoScalingDescribeAutoScalingGroupsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "autoscaling_describe_auto_scaling_groups");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: AutoScalingDescribeAutoScalingGroupsInput = serde_json::from_value(json).unwrap();
    }
}
