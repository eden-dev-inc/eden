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

const API_INFO: ApiInfo<AwsApi, SwfListDomainsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SwfListDomains, "swf_list_domains", ReqType::Read, true);

crate::aws_endpoint! {
    SwfListDomains,
    API_INFO,
    struct {
        registration_status: String,
        next_page_token: Option<String>,
        maximum_page_size: Option<i64>,
        reverse_order: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({"registrationStatus": self.registration_status});
        let result = client.execute_json_target("swf", "SimpleWorkflowService.ListDomains", Some(&body), "1.1").await?;

        span.add_event("received result from aws swf", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SwfListDomainsInputBuilder::default().registration_status("REGISTERED").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "swf_list_domains");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"registration_status": "REGISTERED"});
        let _: SwfListDomainsInput = serde_json::from_value(json).unwrap();
    }
}
