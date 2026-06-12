use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
use crate::api::lib::types::AwsTag;
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

const API_INFO: ApiInfo<AwsApi, Ec2CreateTagsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2CreateTags, "ec2_create_tags", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2CreateTags,
    API_INFO,
    struct {
        resources: Vec<String>,
        tags: Vec<AwsTag>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.extend(indexed_list_params("ResourceId", &self.resources));
        for (i, tag) in self.tags.iter().enumerate() {
            let idx = i + 1;
            params.insert(format!("Tag.{idx}.Key"), tag.key.clone());
            params.insert(format!("Tag.{idx}.Value"), tag.value.clone());
        }
        let form_body = build_query_body("CreateTags", "2016-11-15", &params);
        let result = client.execute_form("ec2", &form_body).await?;

        span.add_event("received result from aws ec2", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = Ec2CreateTagsInputBuilder::default()
            .resources(vec!["i-12345".to_string()])
            .tags(vec![AwsTag::new("Name", "my-instance")])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_create_tags");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resources": ["i-12345"], "tags": [{"key": "Name", "value": "test"}]});
        let _: Ec2CreateTagsInput = serde_json::from_value(json).unwrap();
    }
}
