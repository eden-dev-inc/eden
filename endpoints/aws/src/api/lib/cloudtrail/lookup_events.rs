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

const API_INFO: ApiInfo<AwsApi, CloudTrailLookupEventsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudTrailLookupEvents,
    "Looks up management events or CloudTrail Insights events captured by CloudTrail",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CloudTrailLookupEvents,
    API_INFO,
    struct {
        lookup_attributes: Option<Vec<serde_json::Value>>,
        start_time: Option<String>,
        end_time: Option<String>,
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

        let mut body = serde_json::Map::new();
        if let Some(attrs) = &self.lookup_attributes {
            body.insert("LookupAttributes".to_string(), Value::Array(attrs.clone()));
        }
        if let Some(start) = &self.start_time {
            body.insert("StartTime".to_string(), Value::String(start.clone()));
        }
        if let Some(end) = &self.end_time {
            body.insert("EndTime".to_string(), Value::String(end.clone()));
        }
        if let Some(max) = self.max_results {
            body.insert("MaxResults".to_string(), serde_json::json!(max));
        }
        if let Some(token) = &self.next_token {
            body.insert("NextToken".to_string(), Value::String(token.clone()));
        }
        let body_val = Value::Object(body);
        let result = client
            .execute_json_target(
                "cloudtrail",
                "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.LookupEvents",
                Some(&body_val),
                "1.1",
            )
            .await?;

        span.add_event(
            "received result from aws cloudtrail",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = CloudTrailLookupEventsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudtrail_lookup_events");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: CloudTrailLookupEventsInput = serde_json::from_value(json).unwrap();
    }
}
