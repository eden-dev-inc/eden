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

const API_INFO: ApiInfo<AwsApi, StsAssumeRoleInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::StsAssumeRole,
    "Returns a set of temporary security credentials for a role",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    StsAssumeRole,
    API_INFO,
    struct {
        role_arn: String,
        role_session_name: String,
        duration_seconds: Option<i64>,
        external_id: Option<String>,
        policy: Option<String>
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
        params.insert("RoleSessionName".to_string(), self.role_session_name.clone());
        if let Some(d) = self.duration_seconds {
            params.insert("DurationSeconds".to_string(), d.to_string());
        }
        if let Some(e) = &self.external_id {
            params.insert("ExternalId".to_string(), e.clone());
        }
        if let Some(p) = &self.policy {
            params.insert("Policy".to_string(), p.clone());
        }
        let form_body = build_query_body("AssumeRole", "2011-06-15", &params);
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
        let input = StsAssumeRoleInputBuilder::default().role_arn("arn:aws:iam::123:role/r").role_session_name("s").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sts_assume_role");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"role_arn": "arn", "role_session_name": "s"});
        let _: StsAssumeRoleInput = serde_json::from_value(json).unwrap();
    }
}
