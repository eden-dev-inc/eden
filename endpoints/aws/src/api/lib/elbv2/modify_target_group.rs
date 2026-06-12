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

const API_INFO: ApiInfo<AwsApi, ElbV2ModifyTargetGroupInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ElbV2ModifyTargetGroup, "elbv2_modify_target_group", ReqType::Write, true);

crate::aws_endpoint! {
    ElbV2ModifyTargetGroup,
    API_INFO,
    struct {
        target_group_arn: String,
        health_check_protocol: Option<String>,
        health_check_port: Option<String>,
        health_check_path: Option<String>,
        health_check_interval_seconds: Option<i64>,
        healthy_threshold_count: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("TargetGroupArn".to_string(), self.target_group_arn.clone());
        if let Some(v) = &self.health_check_protocol {
            params.insert("HealthCheckProtocol".to_string(), v.clone());
        }
        if let Some(v) = &self.health_check_port {
            params.insert("HealthCheckPort".to_string(), v.clone());
        }
        if let Some(v) = &self.health_check_path {
            params.insert("HealthCheckPath".to_string(), v.clone());
        }
        if let Some(v) = self.health_check_interval_seconds {
            params.insert("HealthCheckIntervalSeconds".to_string(), v.to_string());
        }
        if let Some(v) = self.healthy_threshold_count {
            params.insert("HealthyThresholdCount".to_string(), v.to_string());
        }
        let form_body = build_query_body("ModifyTargetGroup", "2015-12-01", &params);
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
        let input = ElbV2ModifyTargetGroupInputBuilder::default().target_group_arn("arn").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elbv2_modify_target_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"target_group_arn": "arn"});
        let _: ElbV2ModifyTargetGroupInput = serde_json::from_value(json).unwrap();
    }
}
