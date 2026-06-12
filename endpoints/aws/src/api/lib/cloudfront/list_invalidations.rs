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

const API_INFO: ApiInfo<AwsApi, CloudFrontListInvalidationsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudFrontListInvalidations,
    "cloudfront_list_invalidations",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    CloudFrontListInvalidations,
    API_INFO,
    struct {
        distribution_id: String,
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

        let path = format!("/2020-05-31/distribution/{}/invalidation", self.distribution_id);
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
        let result = client.execute("cloudfront", "GET", &path, query.as_deref(), None, None).await?;

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
        let input = CloudFrontListInvalidationsInputBuilder::default().distribution_id("dist-id").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudfront_list_invalidations");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"distribution_id": "dist-id"});
        let _: CloudFrontListInvalidationsInput = serde_json::from_value(json).unwrap();
    }
}
