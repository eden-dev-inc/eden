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

const API_INFO: ApiInfo<AwsApi, SecretsManagerUntagResourceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SecretsManagerUntagResource,
    "secretsmanager_untag_resource",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SecretsManagerUntagResource,
    API_INFO,
    struct {
        secret_id: String,
        tag_keys: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("SecretId".to_string(), Value::String(self.secret_id.clone()));
        body.insert(
            "TagKeys".to_string(),
            Value::Array(self.tag_keys.iter().map(|k| Value::String(k.clone())).collect()),
        );
        let body_val = Value::Object(body);
        let result = client.execute_json_target("secretsmanager", "secretsmanager.UntagResource", Some(&body_val), "1.1").await?;

        span.add_event(
            "received result from aws secretsmanager",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = SecretsManagerUntagResourceInputBuilder::default().secret_id("my-secret").tag_keys(vec![]).build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "secretsmanager_untag_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"secret_id": "my-secret", "tag_keys": []});
        let _: SecretsManagerUntagResourceInput = serde_json::from_value(json).unwrap();
    }
}
