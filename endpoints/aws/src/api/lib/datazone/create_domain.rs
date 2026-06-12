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

const API_INFO: ApiInfo<AwsApi, DataZoneCreateDomainInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::DataZoneCreateDomain, "datazone_create_domain", ReqType::Write, true);

crate::aws_endpoint! {
    DataZoneCreateDomain,
    API_INFO,
    struct {
        name: String,
        domain_execution_role: String,
        description: Option<String>,
        tags: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({
            "name": self.name,
            "domainExecutionRole": self.domain_execution_role
        });
        let result = client.execute("datazone", "POST", "/v2/domains", None, Some(&body_val), None).await?;

        span.add_event("received result from aws service", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = DataZoneCreateDomainInputBuilder::default().name("n").domain_execution_role("arn:aws:iam::123:role/r").build().unwrap();
        assert_eq!(serde_json::to_value(&input).unwrap()["type"], "datazone_create_domain");
    }

    #[test]
    fn deserialize_minimal() {
        let _: DataZoneCreateDomainInput = serde_json::from_value(serde_json::json!({
            "name": "n",
            "domain_execution_role": "arn:aws:iam::123:role/r"
        }))
        .unwrap();
    }
}
