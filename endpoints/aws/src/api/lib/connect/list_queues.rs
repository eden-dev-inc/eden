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

const API_INFO: ApiInfo<AwsApi, ConnectListQueuesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ConnectListQueues, "connect_list_queues", ReqType::Read, true);

crate::aws_endpoint! {
    ConnectListQueues,
    API_INFO,
    struct {
        instance_id: String,
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
        let path = format!("/queues-summary/{}", self.instance_id);
        let result = client.execute("connect", "GET", &path, None, None, None).await?;
        span.add_event("received result from aws connect", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = ConnectListQueuesInputBuilder::default().instance_id("i-123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "connect_list_queues");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"instance_id": "i-123"});
        let _: ConnectListQueuesInput = serde_json::from_value(json).unwrap();
    }
}
