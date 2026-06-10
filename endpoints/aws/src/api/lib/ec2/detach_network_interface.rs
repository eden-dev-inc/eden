use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
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

const API_INFO: ApiInfo<AwsApi, Ec2DetachNetworkInterfaceInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2DetachNetworkInterface, "ec2_detach_network_interface", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2DetachNetworkInterface,
    API_INFO,
    struct {
        attachment_id: String,
        force: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("AttachmentId".to_string(), self.attachment_id.clone());
        if let Some(v) = &self.force {
            params.insert("Force".to_string(), v.to_string());
        }
        let form_body = build_query_body("DetachNetworkInterface", "2016-11-15", &params);
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
        let input = Ec2DetachNetworkInterfaceInputBuilder::default().attachment_id("eni-attach-123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_detach_network_interface");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"attachment_id": "eni-attach-123"});
        let _: Ec2DetachNetworkInterfaceInput = serde_json::from_value(json).unwrap();
    }
}
