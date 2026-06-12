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

const API_INFO: ApiInfo<AwsApi, CloudTrailDescribeTrailsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudTrailDescribeTrails,
    "Retrieves settings for one or more trails associated with the current region",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CloudTrailDescribeTrails,
    API_INFO,
    struct {
        trail_name_list: Option<Vec<String>>,
        include_shadow_trails: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(names) = &self.trail_name_list {
            body.insert("trailNameList".to_string(), Value::Array(names.iter().map(|s| Value::String(s.clone())).collect()));
        }
        if let Some(b) = self.include_shadow_trails {
            body.insert("includeShadowTrails".to_string(), serde_json::json!(b));
        }
        let body_val = Value::Object(body);
        let result = client
            .execute_json_target(
                "cloudtrail",
                "com.amazonaws.cloudtrail.v20131101.CloudTrail_20131101.DescribeTrails",
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
        let input = CloudTrailDescribeTrailsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudtrail_describe_trails");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: CloudTrailDescribeTrailsInput = serde_json::from_value(json).unwrap();
    }
}
