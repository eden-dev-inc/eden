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

const API_INFO: ApiInfo<AwsApi, GuardDutyListFindingsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::GuardDutyListFindings,
    "Lists GuardDuty findings for a detector",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    GuardDutyListFindings,
    API_INFO,
    struct {
        detector_id: String,
        finding_criteria: Option<serde_json::Value>,
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/detector/{}/findings", self.detector_id);
        let mut body_map = serde_json::Map::new();
        if let Some(fc) = &self.finding_criteria {
            body_map.insert("FindingCriteria".to_string(), fc.clone());
        }
        if let Some(m) = self.max_results {
            body_map.insert("MaxResults".to_string(), serde_json::json!(m));
        }
        if let Some(t) = &self.next_token {
            body_map.insert("NextToken".to_string(), serde_json::json!(t));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("guardduty", "POST", &path, None, Some(&body), None).await?;

        span.add_event("received result from aws guardduty", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = GuardDutyListFindingsInputBuilder::default().detector_id("d123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "guardduty_list_findings");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"detector_id": "d123"});
        let _: GuardDutyListFindingsInput = serde_json::from_value(json).unwrap();
    }
}
