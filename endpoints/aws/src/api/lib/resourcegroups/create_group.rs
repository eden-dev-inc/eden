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

const API_INFO: ApiInfo<AwsApi, ResourceGroupsCreateGroupInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ResourceGroupsCreateGroup, "resourcegroups_create_group", ReqType::Write, true);

crate::aws_endpoint! {
    ResourceGroupsCreateGroup,
    API_INFO,
    struct {
        name: String,
        resource_query: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"Name": self.name, "ResourceQuery": self.resource_query});
        let result = client.execute("resource-groups", "POST", "/groups", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws resource-groups",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = ResourceGroupsCreateGroupInputBuilder::default()
            .name("my-group")
            .resource_query(serde_json::json!({"Type": "TAG_FILTERS_1_0"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "resourcegroups_create_group");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "my-group", "resource_query": {"Type": "TAG_FILTERS_1_0"}});
        let _: ResourceGroupsCreateGroupInput = serde_json::from_value(json).unwrap();
    }
}
