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

const API_INFO: ApiInfo<AwsApi, StorageGatewayActivateGatewayInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::StorageGatewayActivateGateway,
    "storagegateway_activate_gateway",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    StorageGatewayActivateGateway,
    API_INFO,
    struct {
        activation_key: String,
        gateway_name: String,
        gateway_timezone: String,
        gateway_region: String,
        gateway_type: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "ActivationKey": self.activation_key,
            "GatewayName": self.gateway_name,
            "GatewayTimezone": self.gateway_timezone,
            "GatewayRegion": self.gateway_region
        });
        let result =
            client.execute_json_target("storagegateway", "StorageGateway_20130630.ActivateGateway", Some(&body_val), "1.1").await?;

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
        let input = StorageGatewayActivateGatewayInputBuilder::default()
            .activation_key("key")
            .gateway_name("name")
            .gateway_timezone("UTC")
            .gateway_region("us-east-1")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "storagegateway_activate_gateway");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "activation_key": "key",
            "gateway_name": "name",
            "gateway_timezone": "UTC",
            "gateway_region": "us-east-1"
        });
        let _: StorageGatewayActivateGatewayInput = serde_json::from_value(json).unwrap();
    }
}
