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

const API_INFO: ApiInfo<AwsApi, SesV2GetEmailIdentityInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SesV2GetEmailIdentity,
    "Gets details of an email identity in SES v2",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    SesV2GetEmailIdentity,
    API_INFO,
    struct {
        email_identity: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let result = client.execute("email", "GET", &format!("/v2/email/identities/{}", self.email_identity), None, None, None).await?;

        span.add_event("received result from aws email", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SesV2GetEmailIdentityInputBuilder::default().email_identity("test@example.com").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ses_v2_get_email_identity");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"email_identity": "test@example.com"});
        let _: SesV2GetEmailIdentityInput = serde_json::from_value(json).unwrap();
    }
}
