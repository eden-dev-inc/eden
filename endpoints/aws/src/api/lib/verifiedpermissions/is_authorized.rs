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

const API_INFO: ApiInfo<AwsApi, VerifiedPermissionsIsAuthorizedInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::VerifiedPermissionsIsAuthorized,
    "verifiedpermissions_is_authorized",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    VerifiedPermissionsIsAuthorized,
    API_INFO,
    struct {
        policy_store_id: String,
        principal: Option<serde_json::Value>,
        action: serde_json::Value,
        resource: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/policyStores/{}/isAuthorized", self.policy_store_id);
        let body = serde_json::json!({
            "action": self.action
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
        let input = VerifiedPermissionsIsAuthorizedInputBuilder::default()
            .policy_store_id("ps123")
            .action(serde_json::json!({"actionType": "ns::Action", "actionId": "view"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "verifiedpermissions_is_authorized");
    }

    #[test]
    fn deserialize_minimal() {
        let _: VerifiedPermissionsIsAuthorizedInput = serde_json::from_value(serde_json::json!({
            "policy_store_id": "ps123",
            "action": {"actionType": "ns::Action", "actionId": "view"}
        }))
        .unwrap();
    }
}
