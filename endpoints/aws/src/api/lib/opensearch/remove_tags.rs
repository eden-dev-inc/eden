use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, OpenSearchRemoveTagsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::OpenSearchRemoveTags,
    "Removes tags from an OpenSearch domain",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    OpenSearchRemoveTags,
    API_INFO,
    struct {
        arn: String,
        tag_keys: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"ARN": self.arn, "TagKeys": self.tag_keys});
        let result = client.execute("es", "POST", "/2021-01-01/tags-removal", None, Some(&body_val), None).await?;

        span.add_event("received result from aws es", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = OpenSearchRemoveTagsInputBuilder::default()
            .arn("arn:aws:es:us-east-1:123456789012:domain/my-domain")
            .tag_keys(vec!["env".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "opensearch_remove_tags");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"arn": "arn:aws:es:us-east-1:123456789012:domain/my-domain", "tag_keys": ["env"]});
        let _: OpenSearchRemoveTagsInput = serde_json::from_value(json).unwrap();
    }
}
