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

const API_INFO: ApiInfo<AwsApi, ComputeOptimizerGetEc2InstanceRecommendationsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ComputeOptimizerGetEc2InstanceRecommendations,
    "computeoptimizer_get_ec2_instance_recommendations",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ComputeOptimizerGetEc2InstanceRecommendations,
    API_INFO,
    struct {
        max_results: Option<i64>,
        next_token: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({});
        let result = client.execute("compute-optimizer", "POST", "/GetEC2InstanceRecommendations", None, Some(&body_val), None).await?;

        span.add_event(
            "received result from aws compute-optimizer",
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
        let input = ComputeOptimizerGetEc2InstanceRecommendationsInputBuilder::default()
            .max_results(None::<i64>)
            .next_token(None::<String>)
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "computeoptimizer_get_ec2_instance_recommendations");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: ComputeOptimizerGetEc2InstanceRecommendationsInput = serde_json::from_value(json).unwrap();
    }
}
