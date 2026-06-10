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

const API_INFO: ApiInfo<AwsApi, Route53CreateHostedZoneInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Route53CreateHostedZone, "route53_create_hosted_zone", ReqType::Write, true);

crate::aws_endpoint! {
    Route53CreateHostedZone,
    API_INFO,
    struct {
        name: String,
        caller_reference: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({"Name": self.name, "CallerReference": self.caller_reference});
        let result = client.execute("route53", "POST", "/2013-04-01/hostedzone", None, Some(&body), None).await?;

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
        let input = Route53CreateHostedZoneInputBuilder::default().name("example.com").caller_reference("ref-001").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "route53_create_hosted_zone");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "example.com", "caller_reference": "ref-001"});
        let _: Route53CreateHostedZoneInput = serde_json::from_value(json).unwrap();
    }
}
