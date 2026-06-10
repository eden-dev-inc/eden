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

const API_INFO: ApiInfo<AwsApi, VerifiedPermissionsListPolicyStoresInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::VerifiedPermissionsListPolicyStores,
    "verifiedpermissions_list_policy_stores",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    VerifiedPermissionsListPolicyStores,
    API_INFO,
    struct {
        next_token: Option<String>,
        max_results: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({});
        let result = client.execute("verifiedpermissions", "POST", "/listPolicyStores", None, Some(&body), None).await?;

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
        let input = VerifiedPermissionsListPolicyStoresInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "verifiedpermissions_list_policy_stores");
    }

    #[test]
    fn deserialize_minimal() {
        let _: VerifiedPermissionsListPolicyStoresInput = serde_json::from_value(serde_json::json!({})).unwrap();
    }
}
