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

const API_INFO: ApiInfo<AwsApi, KmsCreateKeyInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::KmsCreateKey, "kms_create_key", ReqType::Write, true);

crate::aws_endpoint! {
    KmsCreateKey,
    API_INFO,
    struct {
        description: Option<String>,
        key_usage: Option<String>,
        key_spec: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(d) = &self.description {
            body.insert("Description".to_string(), Value::String(d.clone()));
        }
        if let Some(k) = &self.key_usage {
            body.insert("KeyUsage".to_string(), Value::String(k.clone()));
        }
        if let Some(k) = &self.key_spec {
            body.insert("KeySpec".to_string(), Value::String(k.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("kms", "TrentService.CreateKey", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws kms", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = KmsCreateKeyInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "kms_create_key");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: KmsCreateKeyInput = serde_json::from_value(json).unwrap();
    }
}
