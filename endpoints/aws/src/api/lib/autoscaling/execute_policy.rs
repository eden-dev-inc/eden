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

const API_INFO: ApiInfo<AwsApi, AutoScalingExecutePolicyInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::AutoScalingExecutePolicy, "autoscaling_execute_policy", ReqType::Write, true);

crate::aws_endpoint! {
    AutoScalingExecutePolicy,
    API_INFO,
    struct {
        auto_scaling_group_name: Option<String>,
        policy_name: String,
        honor_cooldown: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("PolicyName".to_string(), self.policy_name.clone());
        if let Some(v) = &self.auto_scaling_group_name {
            params.insert("AutoScalingGroupName".to_string(), v.clone());
        }
        if let Some(v) = self.honor_cooldown {
            params.insert("HonorCooldown".to_string(), v.to_string());
        }
        let form_body = build_query_body("ExecutePolicy", "2011-01-01", &params);
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
        let input = AutoScalingExecutePolicyInputBuilder::default().policy_name("my-policy").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "autoscaling_execute_policy");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"policy_name": "my-policy"});
        let _: AutoScalingExecutePolicyInput = serde_json::from_value(json).unwrap();
    }
}
