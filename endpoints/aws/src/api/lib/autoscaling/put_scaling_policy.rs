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

const API_INFO: ApiInfo<AwsApi, AutoScalingPutScalingPolicyInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::AutoScalingPutScalingPolicy,
    "autoscaling_put_scaling_policy",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    AutoScalingPutScalingPolicy,
    API_INFO,
    struct {
        auto_scaling_group_name: String,
        policy_name: String,
        policy_type: Option<String>,
        adjustment_type: Option<String>,
        scaling_adjustment: Option<i64>
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
        params.insert("PolicyName".to_string(), self.policy_name.clone());
        if let Some(v) = &self.policy_type {
            params.insert("PolicyType".to_string(), v.clone());
        }
        if let Some(v) = &self.adjustment_type {
            params.insert("AdjustmentType".to_string(), v.clone());
        }
        if let Some(v) = self.scaling_adjustment {
            params.insert("ScalingAdjustment".to_string(), v.to_string());
        }
        let form_body = build_query_body("PutScalingPolicy", "2011-01-01", &params);
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
        let input = AutoScalingPutScalingPolicyInputBuilder::default()
            .auto_scaling_group_name("my-asg")
            .policy_name("my-policy")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "autoscaling_put_scaling_policy");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"auto_scaling_group_name": "my-asg", "policy_name": "my-policy"});
        let _: AutoScalingPutScalingPolicyInput = serde_json::from_value(json).unwrap();
    }
}
