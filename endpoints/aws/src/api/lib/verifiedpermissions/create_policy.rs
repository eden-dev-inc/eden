use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, VerifiedPermissionsCreatePolicyInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::VerifiedPermissionsCreatePolicy,
    "verifiedpermissions_create_policy",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    VerifiedPermissionsCreatePolicy,
    API_INFO,
    struct {
        policy_store_id: String,
        definition: serde_json::Value,
        client_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/policyStores/{}/policies", self.policy_store_id);
        let body = serde_json::json!({
            "definition": self.definition
        });
        let result = client.execute("verifiedpermissions", "POST", &path, None, Some(&body), None).await?;

        span.add_event(
            "received result from aws verifiedpermissions",
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
        let input = VerifiedPermissionsCreatePolicyInputBuilder::default()
            .policy_store_id("ps123")
            .definition(serde_json::json!({"static": {"statement": "permit(...)"}}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "verifiedpermissions_create_policy");
    }

    #[test]
    fn deserialize_minimal() {
        let _: VerifiedPermissionsCreatePolicyInput = serde_json::from_value(serde_json::json!({
            "policy_store_id": "ps123",
            "definition": {"static": {"statement": "permit(...)"}}
        }))
        .unwrap();
    }
}
