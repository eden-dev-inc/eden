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

const API_INFO: ApiInfo<AwsApi, Ec2CreateLaunchTemplateInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Ec2CreateLaunchTemplate, "ec2_create_launch_template", ReqType::Write, true);

crate::aws_endpoint! {
    Ec2CreateLaunchTemplate,
    API_INFO,
    struct {
        launch_template_name: String,
        launch_template_data: serde_json::Value,
        version_description: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("LaunchTemplateName".to_string(), self.launch_template_name.clone());
        params.insert(
            "LaunchTemplateData".to_string(),
            serde_json::to_string(&self.launch_template_data).unwrap_or_default(),
        );
        if let Some(v) = &self.version_description {
            params.insert("VersionDescription".to_string(), v.clone());
        }
        let form_body = build_query_body("CreateLaunchTemplate", "2016-11-15", &params);
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
        let input = Ec2CreateLaunchTemplateInputBuilder::default()
            .launch_template_name("my-template")
            .launch_template_data(serde_json::json!({}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ec2_create_launch_template");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"launch_template_name": "my-template", "launch_template_data": {}});
        let _: Ec2CreateLaunchTemplateInput = serde_json::from_value(json).unwrap();
    }
}
