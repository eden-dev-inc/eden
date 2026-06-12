use crate::api::lib::AwsApi;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, GlueStartJobRunInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::GlueStartJobRun, "glue_start_job_run", ReqType::Write, true);

crate::aws_endpoint! {
    GlueStartJobRun,
    API_INFO,
    struct {
        job_name: String,
        arguments: Option<serde_json::Value>,
        worker_type: Option<String>,
        number_of_workers: Option<i64>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("JobName".to_string(), Value::String(self.job_name.clone()));
        if let Some(args) = &self.arguments {
            body.insert("Arguments".to_string(), args.clone());
        }
        if let Some(wt) = &self.worker_type {
            body.insert("WorkerType".to_string(), Value::String(wt.clone()));
        }
        if let Some(n) = self.number_of_workers {
            body.insert("NumberOfWorkers".to_string(), serde_json::json!(n));
        }
        let body_val = Value::Object(body);
        let result = client.execute_json_target("glue", "AmazonWebServiceGlue.StartJobRun", Some(&body_val), "1.1").await?;

        span.add_event("received result from aws glue", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = GlueStartJobRunInputBuilder::default().job_name("job").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "glue_start_job_run");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"job_name": "job"});
        let _: GlueStartJobRunInput = serde_json::from_value(json).unwrap();
    }
}
