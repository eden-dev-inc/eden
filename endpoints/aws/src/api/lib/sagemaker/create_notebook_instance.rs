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

const API_INFO: ApiInfo<AwsApi, SageMakerCreateNotebookInstanceInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SageMakerCreateNotebookInstance,
    "sagemaker_create_notebook_instance",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SageMakerCreateNotebookInstance,
    API_INFO,
    struct {
        notebook_instance_name: String,
        instance_type: String,
        role_arn: String,
        subnet_id: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("NotebookInstanceName".to_string(), serde_json::Value::String(self.notebook_instance_name.clone()));
        body.insert("InstanceType".to_string(), serde_json::Value::String(self.instance_type.clone()));
        body.insert("RoleArn".to_string(), serde_json::Value::String(self.role_arn.clone()));
        if let Some(subnet) = &self.subnet_id {
            body.insert("SubnetId".to_string(), serde_json::Value::String(subnet.clone()));
        }
        let body_val = serde_json::Value::Object(body);
        let result = client.execute_json_target("sagemaker", "SageMaker.CreateNotebookInstance", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws sagemaker", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SageMakerCreateNotebookInstanceInputBuilder::default()
            .notebook_instance_name("nb")
            .instance_type("ml.t2.medium")
            .role_arn("arn:aws:iam::123456789012:role/role")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "sagemaker_create_notebook_instance");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "notebook_instance_name": "nb",
            "instance_type": "ml.t2.medium",
            "role_arn": "arn:aws:iam::123456789012:role/role"
        });
        let _: SageMakerCreateNotebookInstanceInput = serde_json::from_value(json).unwrap();
    }
}
