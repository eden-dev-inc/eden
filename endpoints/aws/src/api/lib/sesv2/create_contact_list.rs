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

const API_INFO: ApiInfo<AwsApi, SesV2CreateContactListInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::SesV2CreateContactList,
    "Creates a contact list in SES v2",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    SesV2CreateContactList,
    API_INFO,
    struct {
        contact_list_name: String,
        description: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("ContactListName".to_string(), Value::String(self.contact_list_name.clone()));
        if let Some(desc) = &self.description {
            body.insert("Description".to_string(), Value::String(desc.clone()));
        }
        let body = Value::Object(body);

        let result = client.execute("email", "POST", "/v2/email/contact-lists", None, Some(&body), None).await?;

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
        let input = SesV2CreateContactListInputBuilder::default().contact_list_name("my-list").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "ses_v2_create_contact_list");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"contact_list_name": "my-list"});
        let _: SesV2CreateContactListInput = serde_json::from_value(json).unwrap();
    }
}
