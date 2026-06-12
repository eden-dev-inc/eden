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

const API_INFO: ApiInfo<AwsApi, ElasticBeanstalkDeleteApplicationInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ElasticBeanstalkDeleteApplication,
    "elasticbeanstalk_delete_application",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    ElasticBeanstalkDeleteApplication,
    API_INFO,
    struct {
        application_name: String,
        terminate_env_by_force: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut params = HashMap::new();
        params.insert("ApplicationName".to_string(), self.application_name.clone());
        if let Some(v) = self.terminate_env_by_force {
            params.insert("TerminateEnvByForce".to_string(), v.to_string());
        }
        let form_body = build_query_body("DeleteApplication", "2010-12-01", &params);
        let result = client.execute_form("elasticbeanstalk", &form_body).await?;

        span.add_event(
            "received result from aws elasticbeanstalk",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = ElasticBeanstalkDeleteApplicationInputBuilder::default().application_name("my-app").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "elasticbeanstalk_delete_application");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"application_name": "my-app"});
        let _: ElasticBeanstalkDeleteApplicationInput = serde_json::from_value(json).unwrap();
    }
}
