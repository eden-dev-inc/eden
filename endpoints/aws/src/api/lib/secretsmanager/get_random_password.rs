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

const API_INFO: ApiInfo<AwsApi, SecretsManagerGetRandomPasswordInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SecretsManagerGetRandomPassword,
    "secretsmanager_get_random_password",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SecretsManagerGetRandomPassword,
    API_INFO,
    struct {
        password_length: Option<i64>,
        exclude_characters: Option<String>,
        exclude_numbers: Option<bool>,
        exclude_punctuation: Option<bool>,
        exclude_uppercase: Option<bool>,
        exclude_lowercase: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(len) = self.password_length {
            body.insert("PasswordLength".to_string(), Value::Number(len.into()));
        }
        if let Some(c) = &self.exclude_characters {
            body.insert("ExcludeCharacters".to_string(), Value::String(c.clone()));
        }
        if let Some(n) = self.exclude_numbers {
            body.insert("ExcludeNumbers".to_string(), Value::Bool(n));
        }
        if let Some(p) = self.exclude_punctuation {
            body.insert("ExcludePunctuation".to_string(), Value::Bool(p));
        }
        if let Some(u) = self.exclude_uppercase {
            body.insert("ExcludeUppercase".to_string(), Value::Bool(u));
        }
        if let Some(l) = self.exclude_lowercase {
            body.insert("ExcludeLowercase".to_string(), Value::Bool(l));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("secretsmanager", "secretsmanager.GetRandomPassword", Some(&body_val), "1.1").await?;

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
        let input = SecretsManagerGetRandomPasswordInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "secretsmanager_get_random_password");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: SecretsManagerGetRandomPasswordInput = serde_json::from_value(json).unwrap();
    }
}
