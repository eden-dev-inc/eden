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

const API_INFO: ApiInfo<AwsApi, IamCreatePolicyInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::IamCreatePolicy, "iam_create_policy", ReqType::Write, true);

crate::aws_endpoint! {
    IamCreatePolicy,
    API_INFO,
    struct {
        policy_name: String,
        policy_document: String,
        path: Option<String>,
        description: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("PolicyName".to_string(), self.policy_name.clone());
        params.insert("PolicyDocument".to_string(), self.policy_document.clone());
        if let Some(p) = &self.path {
            params.insert("Path".to_string(), p.clone());
        }
        if let Some(d) = &self.description {
            params.insert("Description".to_string(), d.clone());
        }
        let form_body = build_query_body("CreatePolicy", "2010-05-08", &params);
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
        let input = IamCreatePolicyInputBuilder::default().policy_name("my-policy").policy_document("{}").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iam_create_policy");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"policy_name": "p", "policy_document": "{}"});
        let _: IamCreatePolicyInput = serde_json::from_value(json).unwrap();
    }
}
