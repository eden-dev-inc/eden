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

const API_INFO: ApiInfo<AwsApi, AcmRequestCertificateInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::AcmRequestCertificate,
    "Requests an ACM certificate for use with other AWS services",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    AcmRequestCertificate,
    API_INFO,
    struct {
        domain_name: String,
        subject_alternative_names: Option<Vec<String>>,
        validation_method: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("DomainName".to_string(), Value::String(self.domain_name.clone()));
        if let Some(names) = &self.subject_alternative_names {
            body.insert(
                "SubjectAlternativeNames".to_string(),
                Value::Array(names.iter().map(|s| Value::String(s.clone())).collect()),
            );
        }
        if let Some(method) = &self.validation_method {
            body.insert("ValidationMethod".to_string(), Value::String(method.clone()));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("acm", "CertificateManager.RequestCertificate", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws acm", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = AcmRequestCertificateInputBuilder::default().domain_name("example.com").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "acm_request_certificate");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"domain_name": "example.com"});
        let _: AcmRequestCertificateInput = serde_json::from_value(json).unwrap();
    }
}
