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

const API_INFO: ApiInfo<AwsApi, SqsGetQueueUrlInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SqsGetQueueUrl, "Returns the URL of an existing SQS queue", ReqType::Read, true);

crate::aws_endpoint! {
    SqsGetQueueUrl,
    API_INFO,
    struct {
        queue_name: String,
        queue_owner_aws_account_id: Option<String>
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
        if let Some(owner) = &self.queue_owner_aws_account_id {
            params.insert("QueueOwnerAWSAccountId".to_string(), owner.clone());
        }
        let form_body = build_query_body("GetQueueUrl", "2012-11-05", &params);
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
        let input = SqsGetQueueUrlInputBuilder::default().queue_name("my-queue").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sqs_get_queue_url");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"queue_name": "my-queue"});
        let _: SqsGetQueueUrlInput = serde_json::from_value(json).unwrap();
    }
}
