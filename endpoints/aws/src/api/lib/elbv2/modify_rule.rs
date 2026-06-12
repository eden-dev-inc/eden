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

const API_INFO: ApiInfo<AwsApi, ElbV2ModifyRuleInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ElbV2ModifyRule, "elbv2_modify_rule", ReqType::Write, true);

crate::aws_endpoint! {
    ElbV2ModifyRule,
    API_INFO,
    struct {
        rule_arn: String,
        conditions: Option<Vec<serde_json::Value>>,
        actions: Option<Vec<serde_json::Value>>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("RuleArn".to_string(), self.rule_arn.clone());
        if let Some(conditions) = &self.conditions {
            for (i, cond) in conditions.iter().enumerate() {
                let idx = i + 1;
                if let Some(field) = cond.get("Field").and_then(|v| v.as_str()) {
                    params.insert(format!("Conditions.member.{}.Field", idx), field.to_string());
                }
                if let Some(values) = cond.get("Values").and_then(|v| v.as_array()) {
                    for (j, val) in values.iter().enumerate() {
                        if let Some(s) = val.as_str() {
                            params.insert(format!("Conditions.member.{}.Values.member.{}", idx, j + 1), s.to_string());
                        }
                    }
                }
            }
        }
        if let Some(actions) = &self.actions {
            for (i, action) in actions.iter().enumerate() {
                let idx = i + 1;
                if let Some(t) = action.get("Type").and_then(|v| v.as_str()) {
                    params.insert(format!("Actions.member.{}.Type", idx), t.to_string());
                }
                if let Some(tga) = action.get("TargetGroupArn").and_then(|v| v.as_str()) {
                    params.insert(format!("Actions.member.{}.TargetGroupArn", idx), tga.to_string());
                }
            }
        }
        let form_body = build_query_body("ModifyRule", "2015-12-01", &params);
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
        let input = ElbV2ModifyRuleInputBuilder::default().rule_arn("arn").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elbv2_modify_rule");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"rule_arn": "arn"});
        let _: ElbV2ModifyRuleInput = serde_json::from_value(json).unwrap();
    }
}
