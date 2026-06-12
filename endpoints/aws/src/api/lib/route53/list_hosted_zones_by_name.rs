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

const API_INFO: ApiInfo<AwsApi, Route53ListHostedZonesByNameInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::Route53ListHostedZonesByName,
    "route53_list_hosted_zones_by_name",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    Route53ListHostedZonesByName,
    API_INFO,
    struct {
        dns_name: Option<String>,
        hosted_zone_id: Option<String>,
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
        if let Some(d) = &self.dns_name {
            query_parts.push(format!("dnsname={}", d));
        }
        if let Some(h) = &self.hosted_zone_id {
            query_parts.push(format!("hostedzoneid={}", h));
        }
        if let Some(m) = &self.max_items {
            query_parts.push(format!("maxitems={}", m));
        }
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let result = client.execute("route53", "GET", "/2013-04-01/hostedzonesbyname", query.as_deref(), None, None).await?;

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
        let input = Route53ListHostedZonesByNameInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "route53_list_hosted_zones_by_name");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: Route53ListHostedZonesByNameInput = serde_json::from_value(json).unwrap();
    }
}
