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

const API_INFO: ApiInfo<AwsApi, SqsCreateQueueInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SqsCreateQueue,
    "Creates a new standard or FIFO SQS queue",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SqsCreateQueue,
    API_INFO,
    struct {
        queue_name: String,
        fifo_queue: Option<bool>,
        visibility_timeout: Option<i64>,
        message_retention_period: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("QueueName".to_string(), self.queue_name.clone());
        if let Some(v) = self.visibility_timeout {
            params.insert("Attribute.1.Name".to_string(), "VisibilityTimeout".to_string());
            params.insert("Attribute.1.Value".to_string(), v.to_string());
        }
        if let Some(v) = self.message_retention_period {
            params.insert("Attribute.2.Name".to_string(), "MessageRetentionPeriod".to_string());
            params.insert("Attribute.2.Value".to_string(), v.to_string());
        }
        if let Some(true) = self.fifo_queue {
            params.insert("Attribute.3.Name".to_string(), "FifoQueue".to_string());
            params.insert("Attribute.3.Value".to_string(), "true".to_string());
        }
        let form_body = build_query_body("CreateQueue", "2012-11-05", &params);
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
        let input = SqsCreateQueueInputBuilder::default().queue_name("q").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sqs_create_queue");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"queue_name": "q"});
        let _: SqsCreateQueueInput = serde_json::from_value(json).unwrap();
    }
}
