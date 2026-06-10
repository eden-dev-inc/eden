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

const API_INFO: ApiInfo<AwsApi, SnowballCreateJobInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::SnowballCreateJob, "snowball_create_job", ReqType::Write, true);

crate::aws_endpoint! {
    SnowballCreateJob,
    API_INFO,
    struct {
        job_type: String,
        resources: serde_json::Value,
        address_id: String
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let body_val = serde_json::json!({"JobType": self.job_type, "Resources": self.resources, "AddressId": self.address_id});
        let result = client.execute_json_target("snowball", "AmazonSnowball.CreateJob", Some(&body_val), "1.1").await?;

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
        let input = SnowballCreateJobInputBuilder::default()
            .job_type("IMPORT")
            .resources(serde_json::json!({}))
            .address_id("ADID1234ab12-3eec-4eb3-9be6-9374c10eb51b")
            .build()
            .unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "snowball_create_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"job_type": "IMPORT", "resources": {}, "address_id": "ADID1234ab12-3eec-4eb3-9be6-9374c10eb51b"});
        let _: SnowballCreateJobInput = serde_json::from_value(json).unwrap();
    }
}
