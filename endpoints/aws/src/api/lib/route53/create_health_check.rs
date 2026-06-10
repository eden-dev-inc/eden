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

const API_INFO: ApiInfo<AwsApi, Route53CreateHealthCheckInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::Route53CreateHealthCheck, "route53_create_health_check", ReqType::Write, true);

crate::aws_endpoint! {
    Route53CreateHealthCheck,
    API_INFO,
    struct {
        caller_reference: String,
        health_check_config: serde_json::Value
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({
            "CallerReference": self.caller_reference,
            "HealthCheckConfig": self.health_check_config
        });
        let result = client.execute("route53", "POST", "/2013-04-01/healthcheck", None, Some(&body), None).await?;

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
        let input = Route53CreateHealthCheckInputBuilder::default()
            .caller_reference("ref-001")
            .health_check_config(serde_json::json!({"Type": "HTTP", "FullyQualifiedDomainName": "example.com"}))
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "route53_create_health_check");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "caller_reference": "ref-001",
            "health_check_config": {"Type": "HTTP", "FullyQualifiedDomainName": "example.com"}
        });
        let _: Route53CreateHealthCheckInput = serde_json::from_value(json).unwrap();
    }
}
