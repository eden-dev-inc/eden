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

const API_INFO: ApiInfo<AwsApi, StsAssumeRoleWithSamlInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::StsAssumeRoleWithSaml,
    "Returns temporary security credentials for users authenticated via SAML",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    StsAssumeRoleWithSaml,
    API_INFO,
    struct {
        role_arn: String,
        principal_arn: String,
        saml_assertion: String,
        duration_seconds: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("RoleArn".to_string(), self.role_arn.clone());
        params.insert("PrincipalArn".to_string(), self.principal_arn.clone());
        params.insert("SAMLAssertion".to_string(), self.saml_assertion.clone());
        if let Some(d) = self.duration_seconds {
            params.insert("DurationSeconds".to_string(), d.to_string());
        }
        let form_body = build_query_body("AssumeRoleWithSAML", "2011-06-15", &params);
        let result = client.execute_form("sts", &form_body).await?;

        span.add_event("received result from aws", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = StsAssumeRoleWithSamlInputBuilder::default()
            .role_arn("arn:aws:iam::123:role/r")
            .principal_arn("arn:aws:iam::123:saml-provider/p")
            .saml_assertion("assertion")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sts_assume_role_with_saml");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"role_arn": "arn", "principal_arn": "arn", "saml_assertion": "a"});
        let _: StsAssumeRoleWithSamlInput = serde_json::from_value(json).unwrap();
    }
}
