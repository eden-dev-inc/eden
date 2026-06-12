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

const API_INFO: ApiInfo<AwsApi, LambdaUpdateFunctionConfigurationInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LambdaUpdateFunctionConfiguration,
    "Modifies the configuration of a Lambda function",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    LambdaUpdateFunctionConfiguration,
    API_INFO,
    struct {
        function_name: String,
        description: Option<String>,
        timeout: Option<i64>,
        memory_size: Option<i64>,
        runtime: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        if let Some(desc) = &self.description {
            body.insert("Description".to_string(), Value::String(desc.clone()));
        }
        if let Some(t) = self.timeout {
            body.insert("Timeout".to_string(), Value::Number(t.into()));
        }
        if let Some(m) = self.memory_size {
            body.insert("MemorySize".to_string(), Value::Number(m.into()));
        }
        if let Some(rt) = &self.runtime {
            body.insert("Runtime".to_string(), Value::String(rt.clone()));
        }
        let body_val = Value::Object(body);
        let path = format!("/2015-03-31/functions/{}/configuration", self.function_name);
        let result = client.execute("lambda", "PUT", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = LambdaUpdateFunctionConfigurationInputBuilder::default().function_name("f").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_update_function_configuration");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"function_name": "f"});
        let _: LambdaUpdateFunctionConfigurationInput = serde_json::from_value(json).unwrap();
    }
}
