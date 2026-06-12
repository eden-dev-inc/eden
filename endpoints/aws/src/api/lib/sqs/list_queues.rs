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

const API_INFO: ApiInfo<AwsApi, SqsListQueuesInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SqsListQueues,
    "Lists the SQS queues in the current account and region",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SqsListQueues,
    API_INFO,
    struct {
        queue_name_prefix: Option<String>,
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

        let mut params = HashMap::new();
        if let Some(p) = &self.queue_name_prefix {
            params.insert("QueueNamePrefix".to_string(), p.clone());
        }
        if let Some(t) = &self.next_token {
            params.insert("NextToken".to_string(), t.clone());
        }
        if let Some(max) = self.max_results {
            params.insert("MaxResults".to_string(), max.to_string());
        }
        let form_body = build_query_body("ListQueues", "2012-11-05", &params);
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
        let input = SqsListQueuesInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sqs_list_queues");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: SqsListQueuesInput = serde_json::from_value(json).unwrap();
    }
}
