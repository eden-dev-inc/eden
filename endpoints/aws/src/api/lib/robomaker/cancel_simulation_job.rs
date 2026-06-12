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

const API_INFO: ApiInfo<AwsApi, RoboMakerCancelSimulationJobInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::RoboMakerCancelSimulationJob,
    "robomaker_cancel_simulation_job",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    RoboMakerCancelSimulationJob,
    API_INFO,
    struct {
        job: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body = serde_json::json!({"job": self.job});
        let result = client.execute("robomaker", "POST", "/cancelSimulationJob", None, Some(&body), None).await?;

        span.add_event("received result from aws robomaker", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = RoboMakerCancelSimulationJobInputBuilder::default()
            .job("arn:aws:robomaker:us-east-1:123456789012:simulation-job/sim-abc123")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "robomaker_cancel_simulation_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"job": "arn:aws:robomaker:us-east-1:123456789012:simulation-job/sim-abc123"});
        let _: RoboMakerCancelSimulationJobInput = serde_json::from_value(json).unwrap();
    }
}
