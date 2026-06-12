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

const API_INFO: ApiInfo<AwsApi, ResourceExplorerCreateIndexInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ResourceExplorerCreateIndex,
    "resourceexplorer_create_index",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ResourceExplorerCreateIndex,
    API_INFO,
    struct {
        index_type: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"Type": self.index_type});
        let result = client.execute("resource-explorer-2", "POST", "/index", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws resource-explorer-2",
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
        let input = ResourceExplorerCreateIndexInputBuilder::default().index_type("LOCAL").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "resourceexplorer_create_index");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"index_type": "LOCAL"});
        let _: ResourceExplorerCreateIndexInput = serde_json::from_value(json).unwrap();
    }
}
