use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
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

const API_INFO: ApiInfo<AwsApi, CfCreateStackInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::CfCreateStack, "Creates a CloudFormation stack", ReqType::Write, true);

crate::aws_endpoint! {
    CfCreateStack,
    API_INFO,
    struct {
        stack_name: String,
        template_url: Option<String>,
        template_body: Option<String>,
        parameters: Option<serde_json::Value>,
        capabilities: Option<Vec<String>>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("StackName".to_string(), self.stack_name.clone());
        if let Some(url) = &self.template_url {
            params.insert("TemplateURL".to_string(), url.clone());
        }
        if let Some(body_tmpl) = &self.template_body {
            params.insert("TemplateBody".to_string(), body_tmpl.clone());
        }
        if let Some(caps) = &self.capabilities {
            params.extend(indexed_list_params("Capabilities.member", caps));
        }
        let form_body = build_query_body("CreateStack", "2010-05-15", &params);
        let result = client.execute_form("cloudformation", &form_body).await?;

        span.add_event(
            "received result from aws cloudformation",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = CfCreateStackInputBuilder::default().stack_name("s").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudformation_create_stack");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"stack_name": "s"});
        let _: CfCreateStackInput = serde_json::from_value(json).unwrap();
    }
}
