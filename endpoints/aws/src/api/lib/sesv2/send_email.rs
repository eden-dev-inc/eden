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

const API_INFO: ApiInfo<AwsApi, SesV2SendEmailInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SesV2SendEmail, "Sends an email via SES v2", ReqType::Write, true);

crate::aws_endpoint! {
    SesV2SendEmail,
    API_INFO,
    struct {
        from_email_address: Option<String>,
        destination: serde_json::Value,
        content: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(from) = &self.from_email_address {
            body.insert("FromEmailAddress".to_string(), Value::String(from.clone()));
        }
        body.insert("Destination".to_string(), self.destination.clone());
        body.insert("Content".to_string(), self.content.clone());
        let body = Value::Object(body);

        let result = client.execute("email", "POST", "/v2/email/outbound-emails", None, Some(&body), None).await?;

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
        let input = SesV2SendEmailInputBuilder::default()
            .destination(serde_json::json!({"ToAddresses": ["test@example.com"]}))
            .content(serde_json::json!({"Simple": {"Subject": {"Data": "Hello"}}}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ses_v2_send_email");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"destination": {"ToAddresses": ["a@b.com"]}, "content": {"Simple": {}}});
        let _: SesV2SendEmailInput = serde_json::from_value(json).unwrap();
    }
}
