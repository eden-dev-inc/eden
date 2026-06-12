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

const API_INFO: ApiInfo<AwsApi, SnsListSubscriptionsByTopicInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SnsListSubscriptionsByTopic,
    "sns_list_subscriptions_by_topic",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SnsListSubscriptionsByTopic,
    API_INFO,
    struct {
        topic_arn: String,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("TopicArn".to_string(), self.topic_arn.clone());
        if let Some(v) = &self.next_token {
            params.insert("NextToken".to_string(), v.clone());
        }
        let form_body = build_query_body("ListSubscriptionsByTopic", "2010-03-31", &params);
        let result = client.execute_form("sns", &form_body).await?;

        span.add_event("received result from aws sns", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SnsListSubscriptionsByTopicInputBuilder::default()
            .topic_arn("arn:aws:sns:us-east-1:123456789012:my-topic")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sns_list_subscriptions_by_topic");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"topic_arn": "arn:aws:sns:us-east-1:123456789012:my-topic"});
        let _: SnsListSubscriptionsByTopicInput = serde_json::from_value(json).unwrap();
    }
}
