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

const API_INFO: ApiInfo<AwsApi, RdsRemoveTagsFromResourceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::RdsRemoveTagsFromResource,
    "rds_remove_tags_from_resource",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    RdsRemoveTagsFromResource,
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
        params.extend(indexed_list_params("TagKeys.member", &self.tag_keys));
        let form_body = build_query_body("RemoveTagsFromResource", "2014-10-31", &params);
        let result = client.execute_form("rds", &form_body).await?;

        span.add_event("received result from aws rds", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RdsRemoveTagsFromResourceInputBuilder::default()
            .resource_name("arn:aws:rds:us-east-1:123456789012:db:my-db")
            .tag_keys(vec!["env".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "rds_remove_tags_from_resource");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_name": "arn", "tag_keys": ["k"]});
        let _: RdsRemoveTagsFromResourceInput = serde_json::from_value(json).unwrap();
    }
}
