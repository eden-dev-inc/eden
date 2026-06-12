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

const API_INFO: ApiInfo<AwsApi, SupportCreateCaseInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SupportCreateCase, "support_create_case", ReqType::Write, true);

crate::aws_endpoint! {
    SupportCreateCase,
    API_INFO,
    struct {
        subject: String,
        service_code: String,
        severity_code: String,
        category_code: String,
        communication_body: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "subject": self.subject,
            "serviceCode": self.service_code,
            "severityCode": self.severity_code,
            "categoryCode": self.category_code,
            "communicationBody": self.communication_body
        });
        let result = client.execute_json_target("support", "AWSSupport_20130415.CreateCase", Some(&body), "1.1").await?;

        span.add_event("received result from aws support", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SupportCreateCaseInputBuilder::default()
            .subject("Test subject")
            .service_code("amazon-s3")
            .severity_code("low")
            .category_code("general-guidance")
            .communication_body("Test body")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "support_create_case");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "subject": "Test subject",
            "service_code": "amazon-s3",
            "severity_code": "low",
            "category_code": "general-guidance",
            "communication_body": "Test body"
        });
        let _: SupportCreateCaseInput = serde_json::from_value(json).unwrap();
    }
}
