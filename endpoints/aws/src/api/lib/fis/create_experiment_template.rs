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

const API_INFO: ApiInfo<AwsApi, FisCreateExperimentTemplateInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::FisCreateExperimentTemplate,
    "fis_create_experiment_template",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    FisCreateExperimentTemplate,
    API_INFO,
    struct {
        description: String,
        stop_conditions: Vec<serde_json::Value>,
        targets: serde_json::Value,
        actions: serde_json::Value,
        role_arn: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "description": self.description,
            "stopConditions": self.stop_conditions,
            "targets": self.targets,
            "actions": self.actions,
            "roleArn": self.role_arn
        });
        let result = client.execute("fis", "POST", "/experimentTemplates", None, Some(&body_val), None).await?;

        span.add_event("received result from aws fis", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = FisCreateExperimentTemplateInputBuilder::default()
            .description("My experiment")
            .stop_conditions(vec![serde_json::json!({"source": "none"})])
            .targets(serde_json::json!({}))
            .actions(serde_json::json!({}))
            .role_arn("arn:aws:iam::123456789012:role/FISRole")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "fis_create_experiment_template");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "description": "My experiment",
            "stop_conditions": [{"source": "none"}],
            "targets": {},
            "actions": {},
            "role_arn": "arn:aws:iam::123456789012:role/FISRole"
        });
        let _: FisCreateExperimentTemplateInput = serde_json::from_value(json).unwrap();
    }
}
