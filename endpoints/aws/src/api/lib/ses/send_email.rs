use crate::api::lib::AwsApi;
use crate::api::lib::params::{build_query_body, indexed_list_params};
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, SesSendEmailInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SesSendEmail, "Sends an email via SES", ReqType::Write, true);

crate::aws_endpoint! {
    SesSendEmail,
    API_INFO,
    struct {
        source: String,
        to_addresses: Vec<String>,
        subject: String,
        body_text: Option<String>,
        body_html: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("Source".to_string(), self.source.clone());
        params.extend(indexed_list_params("Destination.ToAddresses.member", &self.to_addresses));
        params.insert("Message.Subject.Data".to_string(), self.subject.clone());
        if let Some(text) = &self.body_text {
            params.insert("Message.Body.Text.Data".to_string(), text.clone());
        }
        if let Some(html) = &self.body_html {
            params.insert("Message.Body.Html.Data".to_string(), html.clone());
        }
        let form_body = build_query_body("SendEmail", "2010-12-01", &params);
        let result = client.execute_form("email", &form_body).await?;

        span.add_event("received result from aws ses", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = SesSendEmailInputBuilder::default()
            .source("sender@example.com")
            .to_addresses(vec!["recipient@example.com".to_string()])
            .subject("Hello")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ses_send_email");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"source": "a@b.com", "to_addresses": ["c@d.com"], "subject": "Hi"});
        let _: SesSendEmailInput = serde_json::from_value(json).unwrap();
    }
}
