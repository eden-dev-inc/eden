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

const API_INFO: ApiInfo<AwsApi, CloudFrontUpdateDistributionInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::CloudFrontUpdateDistribution,
    "cloudfront_update_distribution",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    CloudFrontUpdateDistribution,
    API_INFO,
    struct {
        id: String,
        distribution_config: Value,
        if_match: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/2020-05-31/distribution/{}/config", self.id);
        let query = Some(format!("If-Match={}", self.if_match));
        let result = client.execute("cloudfront", "PUT", &path, query.as_deref(), Some(&self.distribution_config), None).await?;

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
        let input = CloudFrontUpdateDistributionInputBuilder::default()
            .id("dist-id")
            .distribution_config(serde_json::json!({}))
            .if_match("etag")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "cloudfront_update_distribution");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"id": "dist-id", "distribution_config": {}, "if_match": "etag"});
        let _: CloudFrontUpdateDistributionInput = serde_json::from_value(json).unwrap();
    }
}
