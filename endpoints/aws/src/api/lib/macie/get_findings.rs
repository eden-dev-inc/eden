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

const API_INFO: ApiInfo<AwsApi, MacieGetFindingsInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::MacieGetFindings, "macie_get_findings", ReqType::Read, true);

crate::aws_endpoint! {
    MacieGetFindings,
    API_INFO,
    struct {
        finding_ids: Vec<String>,
        sort_criteria: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({ "findingIds": self.finding_ids });
        let result = client.execute("macie2", "POST", "/findings/describe", None, Some(&body_val), None).await?;

        span.add_event("received result from aws macie2", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = MacieGetFindingsInputBuilder::default()
            .finding_ids(vec!["f1".to_string()])
            .sort_criteria(None::<serde_json::Value>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "macie_get_findings");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({ "finding_ids": ["f1"] });
        let _: MacieGetFindingsInput = serde_json::from_value(json).unwrap();
    }
}
