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

const API_INFO: ApiInfo<AwsApi, DirectConnectDeleteConnectionInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::DirectConnectDeleteConnection,
    "directconnect_delete_connection",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    DirectConnectDeleteConnection,
    API_INFO,
    struct {
        connection_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({ "connectionId": self.connection_id });
        let result = client.execute_json_target("directconnect", "OvertureService.DeleteConnection", Some(&body), "1.1").await?;

        span.add_event(
            "received result from aws directconnect",
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
        let input = DirectConnectDeleteConnectionInputBuilder::default().connection_id("dxcon-abc123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "directconnect_delete_connection");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({ "connection_id": "dxcon-abc123" });
        let _: DirectConnectDeleteConnectionInput = serde_json::from_value(json).unwrap();
    }
}
