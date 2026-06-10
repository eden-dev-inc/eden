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

const API_INFO: ApiInfo<AwsApi, IamDeleteGroupPolicyInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::IamDeleteGroupPolicy, "iam_delete_group_policy", ReqType::Write, true);

crate::aws_endpoint! {
    IamDeleteGroupPolicy,
    API_INFO,
    struct {
        group_name: String,
        policy_name: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("GroupName".to_string(), self.group_name.clone());
        params.insert("PolicyName".to_string(), self.policy_name.clone());
        let form_body = build_query_body("DeleteGroupPolicy", "2010-05-08", &params);
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
        let input = IamDeleteGroupPolicyInputBuilder::default().group_name("my-group").policy_name("my-policy").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "iam_delete_group_policy");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"group_name": "g", "policy_name": "p"});
        let _: IamDeleteGroupPolicyInput = serde_json::from_value(json).unwrap();
    }
}
