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

const API_INFO: ApiInfo<AwsApi, CognitoInitiateAuthInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CognitoInitiateAuth, "cognito_initiate_auth", ReqType::Write, true);

crate::aws_endpoint! {
    CognitoInitiateAuth,
    API_INFO,
    struct {
        auth_flow: String,
        client_id: String,
        auth_parameters: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("AuthFlow".to_string(), Value::String(self.auth_flow.clone()));
        body.insert("ClientId".to_string(), Value::String(self.client_id.clone()));
        if let Some(p) = &self.auth_parameters {
            body.insert("AuthParameters".to_string(), p.clone());
        }
        let body_val = Value::Object(body);
        let result = client
            .execute_json_target("cognito-idp", "AWSCognitoIdentityProviderService.InitiateAuth", Some(&body_val), "1.1")
            .await?;

        span.add_event(
            "received result from aws cognito-idp",
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
        let input = CognitoInitiateAuthInputBuilder::default().auth_flow("USER_PASSWORD_AUTH").client_id("c").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cognito_initiate_auth");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"auth_flow": "USER_PASSWORD_AUTH", "client_id": "c"});
        let _: CognitoInitiateAuthInput = serde_json::from_value(json).unwrap();
    }
}
