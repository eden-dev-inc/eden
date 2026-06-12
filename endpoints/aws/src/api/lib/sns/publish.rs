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

const API_INFO: ApiInfo<AwsApi, SnsPublishInput> = ApiInfo::new(EpKind::Aws, AwsApi::SnsPublish, "sns_publish", ReqType::Write, true);

crate::aws_endpoint! {
    SnsPublish,
    API_INFO,
    struct {
        message: String,
        topic_arn: Option<String>,
        subject: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("Message".to_string(), self.message.clone());
        if let Some(arn) = &self.topic_arn {
            params.insert("TopicArn".to_string(), arn.clone());
        }
        if let Some(subject) = &self.subject {
            params.insert("Subject".to_string(), subject.clone());
        }
        let form_body = build_query_body("Publish", "2010-03-31", &params);
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
        let input = SnsPublishInputBuilder::default().message("hello world").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sns_publish");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"message": "hello world"});
        let _: SnsPublishInput = serde_json::from_value(json).unwrap();
    }
}
