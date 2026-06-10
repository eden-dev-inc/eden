use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, IamCreateLoginProfileInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::IamCreateLoginProfile, "iam_create_login_profile", ReqType::Write, true);

crate::aws_endpoint! {
    IamCreateLoginProfile,
    API_INFO,
    struct {
        user_name: String,
        password: String,
        password_reset_required: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("UserName".to_string(), self.user_name.clone());
        params.insert("Password".to_string(), self.password.clone());
        if let Some(r) = &self.password_reset_required {
            params.insert("PasswordResetRequired".to_string(), r.to_string());
        }
        let form_body = build_query_body("CreateLoginProfile", "2010-05-08", &params);
        let result = client.execute_form("iam", &form_body).await?;

        span.add_event("received result from aws iam", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = IamCreateLoginProfileInputBuilder::default().user_name("u").password("p").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iam_create_login_profile");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"user_name": "u", "password": "p"});
        let _: IamCreateLoginProfileInput = serde_json::from_value(json).unwrap();
    }
}
