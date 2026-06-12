use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, WafV2CreateWebAclInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::WafV2CreateWebAcl, "wafv2_create_web_acl", ReqType::Write, true);

crate::aws_endpoint! {
    WafV2CreateWebAcl,
    API_INFO,
    struct {
        name: String,
        scope: String,
        default_action: serde_json::Value,
        visibility_config: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("Name".to_string(), Value::String(self.name.clone()));
        body.insert("Scope".to_string(), Value::String(self.scope.clone()));
        body.insert("DefaultAction".to_string(), self.default_action.clone());
        body.insert("VisibilityConfig".to_string(), self.visibility_config.clone());
        let body_val = Value::Object(body);
        let result = client.execute_json_target("wafv2", "AWSWAF_20190729.CreateWebACL", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws wafv2", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = WafV2CreateWebAclInputBuilder::default()
            .name("my-acl")
            .scope("REGIONAL")
            .default_action(serde_json::json!({"Allow": {}}))
            .visibility_config(
                serde_json::json!({"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": "my-acl"}),
            )
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "wafv2_create_web_acl");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-acl",
            "scope": "REGIONAL",
            "default_action": {"Allow": {}},
            "visibility_config": {"SampledRequestsEnabled": true, "CloudWatchMetricsEnabled": true, "MetricName": "my-acl"}
        });
        let _: WafV2CreateWebAclInput = serde_json::from_value(json).unwrap();
    }
}
