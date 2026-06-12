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

const API_INFO: ApiInfo<AwsApi, SsmSendCommandInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SsmSendCommand, "ssm_send_command", ReqType::Write, true);

crate::aws_endpoint! {
    SsmSendCommand,
    API_INFO,
    struct {
        document_name: String,
        instance_ids: Option<Vec<String>>,
        parameters: Option<serde_json::Value>,
        comment: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("DocumentName".to_string(), Value::String(self.document_name.clone()));
        if let Some(ids) = &self.instance_ids {
            body.insert("InstanceIds".to_string(), serde_json::json!(ids));
        }
        if let Some(p) = &self.parameters {
            body.insert("Parameters".to_string(), p.clone());
        }
        if let Some(c) = &self.comment {
            body.insert("Comment".to_string(), Value::String(c.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("ssm", "AmazonSSM.SendCommand", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws ssm", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SsmSendCommandInputBuilder::default().document_name("AWS-RunShellScript").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ssm_send_command");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"document_name": "AWS-RunShellScript"});
        let _: SsmSendCommandInput = serde_json::from_value(json).unwrap();
    }
}
