use crate::api::lib::SalesforceApi;
use crate::output::SalesforceJsonOutput;
use crate::request::SalesforceRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use salesforce_core::{SalesforceAsync, SalesforceTx};
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<SalesforceApi, CreateRecordInput> =
    ApiInfo::new(EpKind::Salesforce, SalesforceApi::CreateRecord, "Create a new Salesforce record", ReqType::Write);

crate::salesforce_endpoint! {
    CreateRecord,
    API_INFO,
    struct {
        body: Value
    }
}

impl_simple_operation!(SimpleInput, SalesforceAsync, SalesforceTx, SalesforceApi, SalesforceRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: SalesforceAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("salesforce.{}.{}", API_INFO.api(), function_name!()));

        let object_name = self.body["object_name"]
            .as_str()
            .ok_or_else(|| EpError::request("Salesforce create_record requires an 'object_name' field"))?;
        let fields =
            self.body["fields"].as_object().ok_or_else(|| EpError::request("Salesforce create_record requires a 'fields' object"))?;

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.post(&format!("/sobjects/{}", object_name), Value::Object(fields.clone())).await?;

        span.add_event("received result from salesforce", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(SalesforceJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut SalesforceTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Salesforce transaction support not implemented")
    }
}
