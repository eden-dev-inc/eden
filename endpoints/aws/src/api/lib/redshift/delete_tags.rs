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

const API_INFO: ApiInfo<AwsApi, RedshiftDeleteTagsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RedshiftDeleteTags, "redshift_delete_tags", ReqType::Write, true);

crate::aws_endpoint! {
    RedshiftDeleteTags,
    API_INFO,
    struct {
        resource_name: String,
        tag_keys: Vec<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ResourceName".to_string(), self.resource_name.clone());
        for (i, key) in self.tag_keys.iter().enumerate() {
            let idx = i + 1;
            params.insert(format!("TagKeys.TagKey.{}", idx), key.clone());
        }
        let form_body = build_query_body("DeleteTags", "2012-12-01", &params);
        let result = client.execute_form("redshift", &form_body).await?;

        span.add_event("received result from aws redshift", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RedshiftDeleteTagsInputBuilder::default()
            .resource_name("arn:aws:redshift:us-east-1:123456789012:cluster:my-cluster")
            .tag_keys(vec!["env".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "redshift_delete_tags");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_name": "arn:aws:redshift:us-east-1:123456789012:cluster:my-cluster", "tag_keys": ["env"]});
        let _: RedshiftDeleteTagsInput = serde_json::from_value(json).unwrap();
    }
}
