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

const API_INFO: ApiInfo<AwsApi, Route53ListResourceRecordSetsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Route53ListResourceRecordSets,
    "route53_list_resource_record_sets",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    Route53ListResourceRecordSets,
    API_INFO,
    struct {
        hosted_zone_id: String,
        start_record_name: Option<String>,
        start_record_type: Option<String>,
        max_items: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/2013-04-01/hostedzone/{}/rrset", self.hosted_zone_id);
        let mut query_parts = Vec::new();
        if let Some(n) = &self.start_record_name {
            query_parts.push(format!("name={}", n));
        }
        if let Some(t) = &self.start_record_type {
            query_parts.push(format!("type={}", t));
        }
        if let Some(m) = &self.max_items {
            query_parts.push(format!("maxitems={}", m));
        }
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let result = client.execute("route53", "GET", &path, query.as_deref(), None, None).await?;

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
        let input = Route53ListResourceRecordSetsInputBuilder::default().hosted_zone_id("Z123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "route53_list_resource_record_sets");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"hosted_zone_id": "Z123"});
        let _: Route53ListResourceRecordSetsInput = serde_json::from_value(json).unwrap();
    }
}
