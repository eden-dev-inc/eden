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

const API_INFO: ApiInfo<AwsApi, AutoScalingUpdateAutoScalingGroupInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::AutoScalingUpdateAutoScalingGroup,
    "autoscaling_update_auto_scaling_group",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    AutoScalingUpdateAutoScalingGroup,
    API_INFO,
    struct {
        auto_scaling_group_name: String,
        min_size: Option<i64>,
        max_size: Option<i64>,
        desired_capacity: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("AutoScalingGroupName".to_string(), self.auto_scaling_group_name.clone());
        if let Some(min) = self.min_size {
            params.insert("MinSize".to_string(), min.to_string());
        }
        if let Some(max) = self.max_size {
            params.insert("MaxSize".to_string(), max.to_string());
        }
        if let Some(desired) = self.desired_capacity {
            params.insert("DesiredCapacity".to_string(), desired.to_string());
        }
        let form_body = build_query_body("UpdateAutoScalingGroup", "2011-01-01", &params);
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
        let input = AutoScalingUpdateAutoScalingGroupInputBuilder::default().auto_scaling_group_name("my-asg").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "autoscaling_update_auto_scaling_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"auto_scaling_group_name": "my-asg"});
        let _: AutoScalingUpdateAutoScalingGroupInput = serde_json::from_value(json).unwrap();
    }
}
