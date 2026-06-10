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

const API_INFO: ApiInfo<AwsApi, AcmPcaCreateCertificateAuthorityInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::AcmPcaCreateCertificateAuthority,
    "acmpca_create_certificate_authority",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    AcmPcaCreateCertificateAuthority,
    API_INFO,
    struct {
        certificate_authority_configuration: serde_json::Value,
        certificate_authority_type: String,
        revocation_configuration: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;
        let mut body_map = serde_json::Map::new();
        body_map.insert("CertificateAuthorityConfiguration".to_string(), self.certificate_authority_configuration.clone());
        body_map.insert("CertificateAuthorityType".to_string(), serde_json::json!(self.certificate_authority_type));
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("acm-pca", "POST", "/certificateauthority", None, Some(&body), None).await?;
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
        let input = AcmPcaCreateCertificateAuthorityInputBuilder::default()
            .certificate_authority_configuration(serde_json::json!({}))
            .certificate_authority_type("ROOT")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "acmpca_create_certificate_authority");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"certificate_authority_configuration": {}, "certificate_authority_type": "ROOT"});
        let _: AcmPcaCreateCertificateAuthorityInput = serde_json::from_value(json).unwrap();
    }
}
