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

const API_INFO: ApiInfo<AwsApi, WafV2DeleteWebAclInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::WafV2DeleteWebAcl, "wafv2_delete_web_acl", ReqType::Write, true);

crate::aws_endpoint! {
    WafV2DeleteWebAcl,
    API_INFO,
    struct {
        name: String,
        scope: String,
        id: String,
        lock_token: String
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
        body.insert("Id".to_string(), Value::String(self.id.clone()));
        body.insert("LockToken".to_string(), Value::String(self.lock_token.clone()));
        let body_val = Value::Object(body);
        let result = client.execute_json_target("wafv2", "AWSWAF_20190729.DeleteWebACL", Some(&body_val), "1.1").await?;

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
        let input = WafV2DeleteWebAclInputBuilder::default()
            .name("my-acl")
            .scope("REGIONAL")
            .id("acl-id")
            .lock_token("token")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "wafv2_delete_web_acl");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "my-acl", "scope": "REGIONAL", "id": "acl-id", "lock_token": "token"});
        let _: WafV2DeleteWebAclInput = serde_json::from_value(json).unwrap();
    }
}
