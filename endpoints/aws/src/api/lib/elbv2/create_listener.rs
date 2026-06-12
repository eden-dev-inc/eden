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

const API_INFO: ApiInfo<AwsApi, ElbV2CreateListenerInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ElbV2CreateListener, "elbv2_create_listener", ReqType::Write, true);

crate::aws_endpoint! {
    ElbV2CreateListener,
    API_INFO,
    struct {
        load_balancer_arn: String,
        protocol: String,
        port: i64,
        default_action_type: String,
        default_action_target_group_arn: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("LoadBalancerArn".to_string(), self.load_balancer_arn.clone());
        params.insert("Protocol".to_string(), self.protocol.clone());
        params.insert("Port".to_string(), self.port.to_string());
        params.insert("DefaultActions.member.1.Type".to_string(), self.default_action_type.clone());
        if let Some(v) = &self.default_action_target_group_arn {
            params.insert("DefaultActions.member.1.TargetGroupArn".to_string(), v.clone());
        }
        let form_body = build_query_body("CreateListener", "2015-12-01", &params);
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
        let input = ElbV2CreateListenerInputBuilder::default()
            .load_balancer_arn("arn")
            .protocol("HTTP")
            .port(80)
            .default_action_type("forward")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elbv2_create_listener");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"load_balancer_arn": "arn", "protocol": "HTTP", "port": 80, "default_action_type": "forward"});
        let _: ElbV2CreateListenerInput = serde_json::from_value(json).unwrap();
    }
}
