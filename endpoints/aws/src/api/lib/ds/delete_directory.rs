use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, DsDeleteDirectoryInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::DsDeleteDirectory, "ds_delete_directory", ReqType::Write, true);

crate::aws_endpoint! {
    DsDeleteDirectory,
    API_INFO,
    struct {
        directory_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("DirectoryId".to_string(), Value::String(self.directory_id.clone()));
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ds", "DirectoryService_20150416.DeleteDirectory", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws ds", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = DsDeleteDirectoryInputBuilder::default().directory_id("d-1234567890").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ds_delete_directory");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"directory_id": "d-1234567890"});
        let _: DsDeleteDirectoryInput = serde_json::from_value(json).unwrap();
    }
}
