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

const API_INFO: ApiInfo<AwsApi, ConnectCreateContactFlowInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ConnectCreateContactFlow, "connect_create_contact_flow", ReqType::Write, true);

crate::aws_endpoint! {
    ConnectCreateContactFlow,
    API_INFO,
    struct {
        instance_id: String,
        name: String,
        contact_flow_type: String,
        content: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;
        let path = format!("/contact-flows/{}", self.instance_id);
        let mut body_map = serde_json::Map::new();
        body_map.insert("Name".to_string(), serde_json::json!(self.name));
        body_map.insert("Type".to_string(), serde_json::json!(self.contact_flow_type));
        body_map.insert("Content".to_string(), serde_json::json!(self.content));
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("connect", "POST", &path, None, Some(&body), None).await?;
        span.add_event("received result from aws connect", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = ConnectCreateContactFlowInputBuilder::default()
            .instance_id("i-123")
            .name("n")
            .contact_flow_type("CONTACT_FLOW")
            .content("{}")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "connect_create_contact_flow");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"instance_id": "i-123", "name": "n", "contact_flow_type": "CONTACT_FLOW", "content": "{}"});
        let _: ConnectCreateContactFlowInput = serde_json::from_value(json).unwrap();
    }
}
