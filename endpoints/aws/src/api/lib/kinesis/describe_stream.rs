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

const API_INFO: ApiInfo<AwsApi, KinesisDescribeStreamInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::KinesisDescribeStream, "kinesis_describe_stream", ReqType::Read, true);

crate::aws_endpoint! {
    KinesisDescribeStream,
    API_INFO,
    struct {
        stream_name: String,
        limit: Option<i64>,
        exclusive_start_shard_id: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("StreamName".to_string(), Value::String(self.stream_name.clone()));
        if let Some(l) = self.limit {
            body.insert("Limit".to_string(), serde_json::json!(l));
        }
        if let Some(id) = &self.exclusive_start_shard_id {
            body.insert("ExclusiveStartShardId".to_string(), Value::String(id.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("kinesis", "Kinesis_20131202.DescribeStream", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws kinesis", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = KinesisDescribeStreamInputBuilder::default().stream_name("s").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "kinesis_describe_stream");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"stream_name": "s"});
        let _: KinesisDescribeStreamInput = serde_json::from_value(json).unwrap();
    }
}
