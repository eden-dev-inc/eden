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

const API_INFO: ApiInfo<AwsApi, KmsGenerateDataKeyInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::KmsGenerateDataKey, "kms_generate_data_key", ReqType::Write, true);

crate::aws_endpoint! {
    KmsGenerateDataKey,
    API_INFO,
    struct {
        key_id: String,
        key_spec: Option<String>,
        number_of_bytes: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("KeyId".to_string(), Value::String(self.key_id.clone()));
        if let Some(k) = &self.key_spec {
            body.insert("KeySpec".to_string(), Value::String(k.clone()));
        }
        if let Some(n) = self.number_of_bytes {
            body.insert("NumberOfBytes".to_string(), Value::Number(n.into()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("kms", "TrentService.GenerateDataKey", Some(&body_val), "1.1").await?;

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
        let input = KmsGenerateDataKeyInputBuilder::default().key_id("key-123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "kms_generate_data_key");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"key_id": "key-123"});
        let _: KmsGenerateDataKeyInput = serde_json::from_value(json).unwrap();
    }
}
