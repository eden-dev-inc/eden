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

const API_INFO: ApiInfo<AwsApi, SwfRegisterDomainInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SwfRegisterDomain, "swf_register_domain", ReqType::Write, true);

crate::aws_endpoint! {
    SwfRegisterDomain,
    API_INFO,
    struct {
        name: String,
        workflow_execution_retention_period_in_days: String,
        description: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "name": self.name,
            "workflowExecutionRetentionPeriodInDays": self.workflow_execution_retention_period_in_days
        });
        let result = client.execute_json_target("swf", "SimpleWorkflowService.RegisterDomain", Some(&body), "1.1").await?;

        span.add_event("received result from aws swf", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SwfRegisterDomainInputBuilder::default()
            .name("my-domain")
            .workflow_execution_retention_period_in_days("7")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "swf_register_domain");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "name": "my-domain",
            "workflow_execution_retention_period_in_days": "7"
        });
        let _: SwfRegisterDomainInput = serde_json::from_value(json).unwrap();
    }
}
