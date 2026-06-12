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

const API_INFO: ApiInfo<AwsApi, HealthDescribeEventDetailsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::HealthDescribeEventDetails,
    "health_describe_event_details",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    HealthDescribeEventDetails,
    API_INFO,
    struct {
        event_arns: Vec<String>,
        locale: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({"eventArns": self.event_arns});
        let result = client.execute_json_target("health", "AWSHealth_20160804.DescribeEventDetails", Some(&body), "1.1").await?;

        span.add_event("received result from aws health", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = HealthDescribeEventDetailsInputBuilder::default()
            .event_arns(vec!["arn:aws:health:us-east-1::event/EC2/AWS_EC2_EXAMPLE".to_string()])
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "health_describe_event_details");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({
            "event_arns": ["arn:aws:health:us-east-1::event/EC2/AWS_EC2_EXAMPLE"]
        });
        let _: HealthDescribeEventDetailsInput = serde_json::from_value(json).unwrap();
    }
}
