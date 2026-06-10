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

const API_INFO: ApiInfo<AwsApi, CloudMapDeleteNamespaceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CloudMapDeleteNamespace, "cloudmap_delete_namespace", ReqType::Write, true);

crate::aws_endpoint! {
    CloudMapDeleteNamespace,
    API_INFO,
    struct {
        id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;
        let body = serde_json::json!({"Id": self.id});
        let result = client
            .execute_json_target("servicediscovery", "Route53AutoNaming_v20170314.DeleteNamespace", Some(&body), "1.1")
            .await?;
        span.add_event(
            "received result from aws servicediscovery",
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
        let input = CloudMapDeleteNamespaceInputBuilder::default().id("ns-abc123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudmap_delete_namespace");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"id": "ns-abc123"});
        let _: CloudMapDeleteNamespaceInput = serde_json::from_value(json).unwrap();
    }
}
