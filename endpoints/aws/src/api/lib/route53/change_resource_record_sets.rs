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

const API_INFO: ApiInfo<AwsApi, Route53ChangeResourceRecordSetsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Route53ChangeResourceRecordSets,
    "route53_change_resource_record_sets",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    Route53ChangeResourceRecordSets,
    API_INFO,
    struct {
        hosted_zone_id: String,
        change_batch: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/2013-04-01/hostedzone/{}/rrset", self.hosted_zone_id);
        let body = self.change_batch.clone().unwrap_or_else(|| serde_json::json!({}));
        let result = client.execute("route53", "POST", &path, None, Some(&body), None).await?;

        span.add_event("received result from aws route53", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = Route53ChangeResourceRecordSetsInputBuilder::default().hosted_zone_id("Z123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "route53_change_resource_record_sets");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"hosted_zone_id": "Z123"});
        let _: Route53ChangeResourceRecordSetsInput = serde_json::from_value(json).unwrap();
    }
}
