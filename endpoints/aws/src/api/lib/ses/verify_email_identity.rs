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

const API_INFO: ApiInfo<AwsApi, SesVerifyEmailIdentityInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SesVerifyEmailIdentity,
    "Adds an email address to the list of identities and attempts to verify it",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SesVerifyEmailIdentity,
    API_INFO,
    struct {
        email_address: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("EmailAddress".to_string(), self.email_address.clone());
        let form_body = build_query_body("VerifyEmailIdentity", "2010-12-01", &params);
        let result = client.execute_form("email", &form_body).await?;

        span.add_event("received result from aws ses", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SesVerifyEmailIdentityInputBuilder::default().email_address("test@example.com").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ses_verify_email_identity");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"email_address": "test@example.com"});
        let _: SesVerifyEmailIdentityInput = serde_json::from_value(json).unwrap();
    }
}
