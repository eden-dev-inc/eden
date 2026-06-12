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

const API_INFO: ApiInfo<AwsApi, AcmPcaIssueCertificateInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::AcmPcaIssueCertificate, "acmpca_issue_certificate", ReqType::Write, true);

crate::aws_endpoint! {
    AcmPcaIssueCertificate,
    API_INFO,
    struct {
        certificate_authority_arn: String,
        csr: String,
        signing_algorithm: String,
        validity: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "Csr": self.csr,
            "SigningAlgorithm": self.signing_algorithm,
            "Validity": self.validity
        });
        let path = format!("/certificateauthority/{}/certificate/issuance", self.certificate_authority_arn);
        let result = client.execute("acm-pca", "POST", &path, None, Some(&body), None).await?;

        span.add_event("received result from aws acm-pca", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = AcmPcaIssueCertificateInputBuilder::default()
            .certificate_authority_arn("arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-id")
            .csr("base64csr")
            .signing_algorithm("SHA256WITHRSA")
            .validity(serde_json::json!({"Type": "DAYS", "Value": 365}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "acmpca_issue_certificate");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "certificate_authority_arn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/ca-id",
            "csr": "base64csr",
            "signing_algorithm": "SHA256WITHRSA",
            "validity": {"Type": "DAYS", "Value": 365}
        });
        let _: AcmPcaIssueCertificateInput = serde_json::from_value(json).unwrap();
    }
}
