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

const API_INFO: ApiInfo<AwsApi, DataZoneDeleteDomainInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::DataZoneDeleteDomain, "datazone_delete_domain", ReqType::Write, true);

crate::aws_endpoint! {
    DataZoneDeleteDomain,
    API_INFO,
    struct {
        identifier: String,
        skip_deletion_check: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/v2/domains/{}", self.identifier);
        let result = client.execute("datazone", "DELETE", &path, None, None, None).await?;

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
        let input = DataZoneDeleteDomainInputBuilder::default().identifier("id").build().unwrap();
        assert_eq!(serde_json::to_value(&input).unwrap()["type"], "datazone_delete_domain");
    }

    #[test]
    fn deserialize_minimal() {
        let _: DataZoneDeleteDomainInput = serde_json::from_value(serde_json::json!({ "identifier": "id" })).unwrap();
    }
}
