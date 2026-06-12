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

const API_INFO: ApiInfo<AwsApi, ConfigGetResourceConfigHistoryInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ConfigGetResourceConfigHistory,
    "config_get_resource_config_history",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ConfigGetResourceConfigHistory,
    API_INFO,
    struct {
        resource_type: String,
        resource_id: String,
        limit: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("resourceType".to_string(), Value::String(self.resource_type.clone()));
        body.insert("resourceId".to_string(), Value::String(self.resource_id.clone()));
        if let Some(l) = self.limit {
            body.insert("limit".to_string(), serde_json::json!(l));
        }
        if let Some(token) = &self.next_token {
            body.insert("nextToken".to_string(), Value::String(token.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("config", "StarlingDoveService.GetResourceConfigHistory", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws config", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = ConfigGetResourceConfigHistoryInputBuilder::default()
            .resource_type("AWS::EC2::Instance")
            .resource_id("i-1234567890abcdef0")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "config_get_resource_config_history");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_type": "AWS::EC2::Instance", "resource_id": "i-1234567890abcdef0"});
        let _: ConfigGetResourceConfigHistoryInput = serde_json::from_value(json).unwrap();
    }
}
