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

const API_INFO: ApiInfo<AwsApi, RamListResourcesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::RamListResources, "ram_list_resources", ReqType::Read, true);

crate::aws_endpoint! {
    RamListResources,
    API_INFO,
    struct {
        resource_owner: String,
        resource_type: Option<String>,
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

        let body_val = serde_json::json!({"resourceOwner": self.resource_owner});
        let result = client.execute("ram", "POST", "/listresources", None, Some(&body_val), None).await?;

        span.add_event("received result from aws ram", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RamListResourcesInputBuilder::default()
            .resource_owner("SELF")
            .resource_type(None::<String>)
            .next_token(None::<String>)
            .max_results(None::<i64>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ram_list_resources");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"resource_owner": "SELF"});
        let _: RamListResourcesInput = serde_json::from_value(json).unwrap();
    }
}
