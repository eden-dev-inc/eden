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

const API_INFO: ApiInfo<AwsApi, AcmPcaDeleteCertificateAuthorityInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::AcmPcaDeleteCertificateAuthority,
    "acmpca_delete_certificate_authority",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    AcmPcaDeleteCertificateAuthority,
    API_INFO,
    struct {
        certificate_authority_arn: String,
        permanent_deletion_time_in_days: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;
        let result = client.execute("acm-pca", "DELETE", "/certificateauthority", None, None, None).await?;
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
        let input = AcmPcaDeleteCertificateAuthorityInputBuilder::default()
            .certificate_authority_arn("arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/abc")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "acmpca_delete_certificate_authority");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"certificate_authority_arn": "arn:aws:acm-pca:us-east-1:123456789012:certificate-authority/abc"});
        let _: AcmPcaDeleteCertificateAuthorityInput = serde_json::from_value(json).unwrap();
    }
}
