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

const API_INFO: ApiInfo<AwsApi, Route53TestDnsAnswerInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Route53TestDnsAnswer, "route53_test_dns_answer", ReqType::Read, true);

crate::aws_endpoint! {
    Route53TestDnsAnswer,
    API_INFO,
    struct {
        hosted_zone_id: String,
        record_name: String,
        record_type: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let query = format!(
            "hostedzoneid={}&recordname={}&recordtype={}",
            self.hosted_zone_id, self.record_name, self.record_type
        );
        let result = client.execute("route53", "GET", "/2013-04-01/testdnsanswer", Some(&query), None, None).await?;

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
        let input = Route53TestDnsAnswerInputBuilder::default()
            .hosted_zone_id("Z123")
            .record_name("example.com")
            .record_type("A")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "route53_test_dns_answer");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "hosted_zone_id": "Z123",
            "record_name": "example.com",
            "record_type": "A"
        });
        let _: Route53TestDnsAnswerInput = serde_json::from_value(json).unwrap();
    }
}
