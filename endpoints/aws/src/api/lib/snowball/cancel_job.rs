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

const API_INFO: ApiInfo<AwsApi, SnowballCancelJobInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SnowballCancelJob, "snowball_cancel_job", ReqType::Write, true);

crate::aws_endpoint! {
    SnowballCancelJob,
    API_INFO,
    struct {
        job_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"JobId": self.job_id});
        let result = client.execute_json_target("snowball", "AmazonSnowball.CancelJob", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws snowball", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = SnowballCancelJobInputBuilder::default().job_id("JID123e4567-e89b-12d3-a456-426614174000").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "snowball_cancel_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"job_id": "JID123e4567-e89b-12d3-a456-426614174000"});
        let _: SnowballCancelJobInput = serde_json::from_value(json).unwrap();
    }
}
