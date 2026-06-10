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

const API_INFO: ApiInfo<AwsApi, BatchSubmitJobInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::BatchSubmitJob, "Submits an AWS Batch job", ReqType::Write, true);

crate::aws_endpoint! {
    BatchSubmitJob,
    API_INFO,
    struct {
        job_name: String,
        job_queue: String,
        job_definition: String,
        parameters: Option<serde_json::Value>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        body_map.insert("jobName".to_string(), serde_json::json!(self.job_name));
        body_map.insert("jobQueue".to_string(), serde_json::json!(self.job_queue));
        body_map.insert("jobDefinition".to_string(), serde_json::json!(self.job_definition));
        if let Some(p) = &self.parameters {
            body_map.insert("parameters".to_string(), p.clone());
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("batch", "POST", "/v1/submitjob", None, Some(&body), None).await?;

        span.add_event("received result from aws batch", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = BatchSubmitJobInputBuilder::default().job_name("n").job_queue("q").job_definition("d").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "batch_submit_job");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"job_name": "n", "job_queue": "q", "job_definition": "d"});
        let _: BatchSubmitJobInput = serde_json::from_value(json).unwrap();
    }
}
