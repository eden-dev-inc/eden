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

const API_INFO: ApiInfo<AwsApi, KmsDecryptInput> = ApiInfo::new(EpKind::Aws, AwsApi::KmsDecrypt, "kms_decrypt", ReqType::Write, true);

crate::aws_endpoint! {
    KmsDecrypt,
    API_INFO,
    struct {
        ciphertext_blob: String,
        key_id: Option<String>,
        encryption_algorithm: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("CiphertextBlob".to_string(), Value::String(self.ciphertext_blob.clone()));
        if let Some(k) = &self.key_id {
            body.insert("KeyId".to_string(), Value::String(k.clone()));
        }
        if let Some(a) = &self.encryption_algorithm {
            body.insert("EncryptionAlgorithm".to_string(), Value::String(a.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("kms", "TrentService.Decrypt", Some(&body_val), "1.1").await?;

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
        let input = KmsDecryptInputBuilder::default().ciphertext_blob("abc123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "kms_decrypt");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"ciphertext_blob": "abc123"});
        let _: KmsDecryptInput = serde_json::from_value(json).unwrap();
    }
}
