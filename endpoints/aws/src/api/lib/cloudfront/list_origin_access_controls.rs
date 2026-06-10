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

const API_INFO: ApiInfo<AwsApi, CloudFrontListOriginAccessControlsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudFrontListOriginAccessControls,
    "cloudfront_list_origin_access_controls",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CloudFrontListOriginAccessControls,
    API_INFO,
    struct {
        marker: Option<String>,
        max_items: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut query_parts = Vec::new();
        if let Some(m) = &self.marker {
            query_parts.push(format!("Marker={}", m));
        }
        if let Some(m) = &self.max_items {
            query_parts.push(format!("MaxItems={}", m));
        }
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let result = client.execute("cloudfront", "GET", "/2020-05-31/origin-access-control", query.as_deref(), None, None).await?;

        span.add_event(
            "received result from aws cloudfront",
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
        let input = CloudFrontListOriginAccessControlsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudfront_list_origin_access_controls");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: CloudFrontListOriginAccessControlsInput = serde_json::from_value(json).unwrap();
    }
}
