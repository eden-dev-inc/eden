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

const API_INFO: ApiInfo<AwsApi, SqsChangeMessageVisibilityInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SqsChangeMessageVisibility,
    "Changes the visibility timeout of a message in a queue",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SqsChangeMessageVisibility,
    API_INFO,
    struct {
        queue_url: String,
        receipt_handle: String,
        visibility_timeout: i64
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
        params.insert("ReceiptHandle".to_string(), self.receipt_handle.clone());
        params.insert("VisibilityTimeout".to_string(), self.visibility_timeout.to_string());
        let form_body = build_query_body("ChangeMessageVisibility", "2012-11-05", &params);
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
        let input = SqsChangeMessageVisibilityInputBuilder::default()
            .queue_url("https://sqs.us-east-1.amazonaws.com/123/q")
            .receipt_handle("handle")
            .visibility_timeout(60i64)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sqs_change_message_visibility");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"queue_url": "url", "receipt_handle": "h", "visibility_timeout": 30});
        let _: SqsChangeMessageVisibilityInput = serde_json::from_value(json).unwrap();
    }
}
