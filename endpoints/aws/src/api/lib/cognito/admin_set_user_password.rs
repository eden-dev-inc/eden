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

const API_INFO: ApiInfo<AwsApi, CognitoAdminSetUserPasswordInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CognitoAdminSetUserPassword,
    "cognito_admin_set_user_password",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    CognitoAdminSetUserPassword,
    API_INFO,
    struct {
        user_pool_id: String,
        username: String,
        password: String,
        permanent: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("UserPoolId".to_string(), Value::String(self.user_pool_id.clone()));
        body.insert("Username".to_string(), Value::String(self.username.clone()));
        body.insert("Password".to_string(), Value::String(self.password.clone()));
        if let Some(perm) = self.permanent {
            body.insert("Permanent".to_string(), serde_json::json!(perm));
        }
        let body_val = Value::Object(body);
        let result = client
            .execute_json_target("cognito-idp", "AWSCognitoIdentityProviderService.AdminSetUserPassword", Some(&body_val), "1.1")
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
        let input = CognitoAdminSetUserPasswordInputBuilder::default().user_pool_id("p").username("u").password("pw").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cognito_admin_set_user_password");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"user_pool_id": "p", "username": "u", "password": "pw"});
        let _: CognitoAdminSetUserPasswordInput = serde_json::from_value(json).unwrap();
    }
}
