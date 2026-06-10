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

const API_INFO: ApiInfo<AwsApi, GlueCreateJobInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::GlueCreateJob, "glue_create_job", ReqType::Write, true);

crate::aws_endpoint! {
    GlueCreateJob,
    API_INFO,
    struct {
        name: String,
        role: String,
        command: serde_json::Value,
        default_arguments: Option<serde_json::Value>
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
        body.insert("Role".to_string(), Value::String(self.role.clone()));
        body.insert("Command".to_string(), self.command.clone());
        if let Some(args) = &self.default_arguments {
            body.insert("DefaultArguments".to_string(), args.clone());
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("glue", "AmazonWebServiceGlue.CreateJob", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws glue", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = GlueCreateJobInputBuilder::default()
            .name("job")
            .role("role")
            .command(serde_json::json!({"Name": "glueetl"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "glue_create_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "job", "role": "role", "command": {"Name": "glueetl"}});
        let _: GlueCreateJobInput = serde_json::from_value(json).unwrap();
    }
}
