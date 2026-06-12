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

const API_INFO: ApiInfo<AwsApi, OpenSearchDeleteDomainInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::OpenSearchDeleteDomain, "Deletes an OpenSearch domain", ReqType::Write, true);

crate::aws_endpoint! {
    OpenSearchDeleteDomain,
    API_INFO,
    struct {
        domain_name: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/2021-01-01/domain/{}", self.domain_name);
        let result = client.execute("es", "DELETE", &path, None, None, None).await?;

        span.add_event("received result from aws es", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = OpenSearchDeleteDomainInputBuilder::default().domain_name("my-domain").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "opensearch_delete_domain");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"domain_name": "my-domain"});
        let _: OpenSearchDeleteDomainInput = serde_json::from_value(json).unwrap();
    }
}
