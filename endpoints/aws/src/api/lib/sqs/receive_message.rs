use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, SqsReceiveMessageInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SqsReceiveMessage,
    "Retrieves one or more messages (up to 10) from the specified SQS queue",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SqsReceiveMessage,
    API_INFO,
    struct {
        queue_url: String,
        max_number_of_messages: Option<i64>,
        visibility_timeout: Option<i64>,
        wait_time_seconds: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("QueueUrl".to_string(), self.queue_url.clone());
        if let Some(m) = self.max_number_of_messages {
            params.insert("MaxNumberOfMessages".to_string(), m.to_string());
        }
        if let Some(v) = self.visibility_timeout {
            params.insert("VisibilityTimeout".to_string(), v.to_string());
        }
        if let Some(w) = self.wait_time_seconds {
            params.insert("WaitTimeSeconds".to_string(), w.to_string());
        }
        let form_body = build_query_body("ReceiveMessage", "2012-11-05", &params);
        let result = client.execute_form("sqs", &form_body).await?;

        span.add_event("received result from aws sqs", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = SqsReceiveMessageInputBuilder::default().queue_url("https://sqs.us-east-1.amazonaws.com/123/q").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sqs_receive_message");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"queue_url": "url"});
        let _: SqsReceiveMessageInput = serde_json::from_value(json).unwrap();
    }
}
