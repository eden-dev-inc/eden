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

const API_INFO: ApiInfo<AwsApi, XRayGetTraceSummariesInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::XRayGetTraceSummaries, "Gets X-Ray trace summaries", ReqType::Read, true);

crate::aws_endpoint! {
    XRayGetTraceSummaries,
    API_INFO,
    struct {
        start_time: String,
        end_time: String,
        sampling: Option<bool>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        body_map.insert("StartTime".to_string(), serde_json::json!(self.start_time));
        body_map.insert("EndTime".to_string(), serde_json::json!(self.end_time));
        if let Some(s) = self.sampling {
            body_map.insert("Sampling".to_string(), serde_json::json!(s));
        }
        if let Some(t) = &self.next_token {
            body_map.insert("NextToken".to_string(), serde_json::json!(t));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("xray", "POST", "/TraceSummaries", None, Some(&body), None).await?;

        span.add_event("received result from aws xray", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = XRayGetTraceSummariesInputBuilder::default()
            .start_time("2024-01-01T00:00:00Z")
            .end_time("2024-01-01T01:00:00Z")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "xray_get_trace_summaries");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"start_time": "2024-01-01T00:00:00Z", "end_time": "2024-01-01T01:00:00Z"});
        let _: XRayGetTraceSummariesInput = serde_json::from_value(json).unwrap();
    }
}
