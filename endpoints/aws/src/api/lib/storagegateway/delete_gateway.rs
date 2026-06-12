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

const API_INFO: ApiInfo<AwsApi, StorageGatewayDeleteGatewayInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::StorageGatewayDeleteGateway,
    "storagegateway_delete_gateway",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    StorageGatewayDeleteGateway,
    API_INFO,
    struct {
        gateway_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({ "GatewayARN": self.gateway_arn });
        let result = client.execute_json_target("storagegateway", "StorageGateway_20130630.DeleteGateway", Some(&body_val), "1.1").await?;

        span.add_event(
            "received result from aws storagegateway",
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
        let input = StorageGatewayDeleteGatewayInputBuilder::default()
            .gateway_arn("arn:aws:storagegateway:us-east-1:123456789012:gateway/sgw-12345678")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "storagegateway_delete_gateway");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"gateway_arn": "arn:aws:storagegateway:us-east-1:123456789012:gateway/sgw-12345678"});
        let _: StorageGatewayDeleteGatewayInput = serde_json::from_value(json).unwrap();
    }
}
