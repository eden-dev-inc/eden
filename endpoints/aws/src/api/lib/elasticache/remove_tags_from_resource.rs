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

const API_INFO: ApiInfo<AwsApi, ElastiCacheRemoveTagsFromResourceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElastiCacheRemoveTagsFromResource,
    "elasticache_remove_tags_from_resource",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ElastiCacheRemoveTagsFromResource,
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
            params.insert(format!("TagKeys.member.{}", i + 1), key.clone());
        }
        let form_body = build_query_body("RemoveTagsFromResource", "2015-02-02", &params);
        let result = client.execute_form("elasticache", &form_body).await?;

        span.add_event(
            "received result from aws elasticache",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = ElastiCacheRemoveTagsFromResourceInputBuilder::default()
            .resource_name("arn:aws:elasticache:us-east-1:123:cluster:my-cache")
            .tag_keys(vec!["env".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elasticache_remove_tags_from_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_name": "arn", "tag_keys": ["k"]});
        let _: ElastiCacheRemoveTagsFromResourceInput = serde_json::from_value(json).unwrap();
    }
}
