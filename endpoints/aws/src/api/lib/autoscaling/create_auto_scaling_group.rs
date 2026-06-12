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

const API_INFO: ApiInfo<AwsApi, AutoScalingCreateAutoScalingGroupInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::AutoScalingCreateAutoScalingGroup,
    "autoscaling_create_auto_scaling_group",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    AutoScalingCreateAutoScalingGroup,
    API_INFO,
    struct {
        auto_scaling_group_name: String,
        min_size: i64,
        max_size: i64,
        desired_capacity: Option<i64>,
        launch_template_id: Option<String>,
        vpc_zone_identifier: Option<String>
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
        params.insert("MinSize".to_string(), self.min_size.to_string());
        params.insert("MaxSize".to_string(), self.max_size.to_string());
        if let Some(v) = self.desired_capacity {
            params.insert("DesiredCapacity".to_string(), v.to_string());
        }
        if let Some(v) = &self.launch_template_id {
            params.insert("LaunchTemplate.LaunchTemplateId".to_string(), v.clone());
        }
        if let Some(v) = &self.vpc_zone_identifier {
            params.insert("VPCZoneIdentifier".to_string(), v.clone());
        }
        let form_body = build_query_body("CreateAutoScalingGroup", "2011-01-01", &params);
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
        let input = AutoScalingCreateAutoScalingGroupInputBuilder::default()
            .auto_scaling_group_name("my-asg")
            .min_size(1)
            .max_size(10)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "autoscaling_create_auto_scaling_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"auto_scaling_group_name": "my-asg", "min_size": 1, "max_size": 10});
        let _: AutoScalingCreateAutoScalingGroupInput = serde_json::from_value(json).unwrap();
    }
}
