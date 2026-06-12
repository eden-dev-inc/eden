use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
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

const API_INFO: ApiInfo<AwsApi, SqsGetQueueAttributesInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SqsGetQueueAttributes,
    "Gets attributes for the specified SQS queue",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SqsGetQueueAttributes,
    API_INFO,
    struct {
        queue_url: String,
        attribute_names: Option<Vec<String>>
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
        if let Some(names) = &self.attribute_names {
            params.extend(indexed_list_params("AttributeName", names));
        }
        let form_body = build_query_body("GetQueueAttributes", "2012-11-05", &params);
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
        let input = SqsGetQueueAttributesInputBuilder::default().queue_url("https://sqs.us-east-1.amazonaws.com/123/q").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sqs_get_queue_attributes");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"queue_url": "url"});
        let _: SqsGetQueueAttributesInput = serde_json::from_value(json).unwrap();
    }
}
