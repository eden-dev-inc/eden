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

const API_INFO: ApiInfo<AwsApi, StsGetSessionTokenInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::StsGetSessionToken,
    "Returns a set of temporary credentials for an AWS account or IAM user",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    StsGetSessionToken,
    API_INFO,
    struct {
        duration_seconds: Option<i64>,
        serial_number: Option<String>,
        token_code: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        if let Some(d) = self.duration_seconds {
            params.insert("DurationSeconds".to_string(), d.to_string());
        }
        if let Some(s) = &self.serial_number {
            params.insert("SerialNumber".to_string(), s.clone());
        }
        if let Some(t) = &self.token_code {
            params.insert("TokenCode".to_string(), t.clone());
        }
        let form_body = build_query_body("GetSessionToken", "2011-06-15", &params);
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
        let input = StsGetSessionTokenInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sts_get_session_token");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: StsGetSessionTokenInput = serde_json::from_value(json).unwrap();
    }
}
