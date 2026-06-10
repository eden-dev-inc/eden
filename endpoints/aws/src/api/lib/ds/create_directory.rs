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

const API_INFO: ApiInfo<AwsApi, DsCreateDirectoryInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::DsCreateDirectory, "ds_create_directory", ReqType::Write, true);

crate::aws_endpoint! {
    DsCreateDirectory,
    API_INFO,
    struct {
        name: String,
        short_name: Option<String>,
        password: String,
        size: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("Name".to_string(), Value::String(self.name.clone()));
        body.insert("Password".to_string(), Value::String(self.password.clone()));
        body.insert("Size".to_string(), Value::String(self.size.clone()));
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ds", "DirectoryService_20150416.CreateDirectory", Some(&body_val), "1.1").await?;

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
        let input = DsCreateDirectoryInputBuilder::default().name("corp.example.com").password("P@ssw0rd").size("Small").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ds_create_directory");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "corp.example.com", "password": "P@ssw0rd", "size": "Small"});
        let _: DsCreateDirectoryInput = serde_json::from_value(json).unwrap();
    }
}
