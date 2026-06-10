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

const API_INFO: ApiInfo<AwsApi, IamChangePasswordInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::IamChangePassword, "iam_change_password", ReqType::Write, true);

crate::aws_endpoint! {
    IamChangePassword,
    API_INFO,
    struct {
        old_password: String,
        new_password: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("OldPassword".to_string(), self.old_password.clone());
        params.insert("NewPassword".to_string(), self.new_password.clone());
        let form_body = build_query_body("ChangePassword", "2010-05-08", &params);
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
        let input = IamChangePasswordInputBuilder::default().old_password("old").new_password("new").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iam_change_password");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"old_password": "old", "new_password": "new"});
        let _: IamChangePasswordInput = serde_json::from_value(json).unwrap();
    }
}
