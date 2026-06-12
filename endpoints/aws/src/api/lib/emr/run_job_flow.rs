use crate::api::lib::AwsApi;
use crate::api::lib::params::build_query_body;
use crate::api::wrapper::output::AwsJsonOutput;
use crate::request::AwsRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use aws_core::{AwsAsync, AwsTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use std::collections::HashMap;
use telemetry::{FastSpanAttribute, TelemetryWrapper};

const API_INFO: ApiInfo<AwsApi, EmrRunJobFlowInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::EmrRunJobFlow, "emr_run_job_flow", ReqType::Write, true);

crate::aws_endpoint! {
    EmrRunJobFlow,
    API_INFO,
    struct {
        name: String,
        instances: Option<String>,
        log_uri: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("Name".to_string(), self.name.clone());
        if let Some(instances) = &self.instances {
            params.insert("Instances".to_string(), instances.clone());
        }
        if let Some(log_uri) = &self.log_uri {
            params.insert("LogUri".to_string(), log_uri.clone());
        }
        let form_body = build_query_body("RunJobFlow", "2009-03-31", &params);
        let result = client.execute_form("emr", &form_body).await?;

        span.add_event("received result from aws emr", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
        Ok(Box::new(AwsJsonOutput(Value::String(result)).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut AwsTx, _telemetry_wrapper: &mut TelemetryWrapper) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn builder_serde() {
        let input = EmrRunJobFlowInputBuilder::default().name("my-cluster").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "emr_run_job_flow");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "my-cluster"});
        let _: EmrRunJobFlowInput = serde_json::from_value(json).unwrap();
    }
}
