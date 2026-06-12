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

const API_INFO: ApiInfo<AwsApi, IamUpdateAccountPasswordPolicyInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::IamUpdateAccountPasswordPolicy,
    "iam_update_account_password_policy",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    IamUpdateAccountPasswordPolicy,
    API_INFO,
    struct {
        minimum_password_length: Option<i64>,
        require_symbols: Option<bool>,
        require_numbers: Option<bool>,
        require_uppercase_characters: Option<bool>,
        require_lowercase_characters: Option<bool>,
        max_password_age: Option<i64>,
        password_reuse_prevention: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(v) = self.minimum_password_length {
            params.insert("MinimumPasswordLength".to_string(), v.to_string());
        }
        if let Some(v) = self.require_symbols {
            params.insert("RequireSymbols".to_string(), v.to_string());
        }
        if let Some(v) = self.require_numbers {
            params.insert("RequireNumbers".to_string(), v.to_string());
        }
        if let Some(v) = self.require_uppercase_characters {
            params.insert("RequireUppercaseCharacters".to_string(), v.to_string());
        }
        if let Some(v) = self.require_lowercase_characters {
            params.insert("RequireLowercaseCharacters".to_string(), v.to_string());
        }
        if let Some(v) = self.max_password_age {
            params.insert("MaxPasswordAge".to_string(), v.to_string());
        }
        if let Some(v) = self.password_reuse_prevention {
            params.insert("PasswordReusePrevention".to_string(), v.to_string());
        }
        let form_body = build_query_body("UpdateAccountPasswordPolicy", "2010-05-08", &params);
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
        let input = IamUpdateAccountPasswordPolicyInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iam_update_account_password_policy");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: IamUpdateAccountPasswordPolicyInput = serde_json::from_value(json).unwrap();
    }
}
