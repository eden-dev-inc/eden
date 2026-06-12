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

const API_INFO: ApiInfo<AwsApi, CodeStarConnectionsGetConnectionInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CodeStarConnectionsGetConnection,
    "codestarconnections_get_connection",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CodeStarConnectionsGetConnection,
    API_INFO,
    struct {
        connection_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/connections/{}", self.connection_arn);
        let result = client.execute("codestar-connections", "GET", &path, None, None, None).await?;

        span.add_event(
            "received result from aws codestar-connections",
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
        let input = CodeStarConnectionsGetConnectionInputBuilder::default()
            .connection_arn("arn:aws:codestar-connections:us-east-1:123456789012:connection/abc")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "codestarconnections_get_connection");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"connection_arn": "arn:aws:codestar-connections:us-east-1:123456789012:connection/abc"});
        let _: CodeStarConnectionsGetConnectionInput = serde_json::from_value(json).unwrap();
    }
}
