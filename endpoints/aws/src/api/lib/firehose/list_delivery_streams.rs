use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, FirehoseListDeliveryStreamsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::FirehoseListDeliveryStreams,
    "firehose_list_delivery_streams",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    FirehoseListDeliveryStreams,
    API_INFO,
    struct {
        limit: Option<i64>,
        exclusive_start_delivery_stream_name: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(l) = self.limit {
            body.insert("Limit".to_string(), serde_json::json!(l));
        }
        if let Some(n) = &self.exclusive_start_delivery_stream_name {
            body.insert("ExclusiveStartDeliveryStreamName".to_string(), Value::String(n.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("firehose", "Firehose_20150804.ListDeliveryStreams", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws firehose", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = FirehoseListDeliveryStreamsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "firehose_list_delivery_streams");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: FirehoseListDeliveryStreamsInput = serde_json::from_value(json).unwrap();
    }
}
