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

const API_INFO: ApiInfo<AwsApi, LambdaCreateFunctionUrlConfigInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LambdaCreateFunctionUrlConfig,
    "lambda_create_function_url_config",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    LambdaCreateFunctionUrlConfig,
    API_INFO,
    struct {
        function_name: String,
        auth_type: String,
        cors: Option<serde_json::Value>,
        invoke_mode: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::Map::new();
        body.insert("AuthType".to_string(), Value::String(self.auth_type.clone()));
        if let Some(cors) = &self.cors {
            body.insert("Cors".to_string(), cors.clone());
        }
        if let Some(mode) = &self.invoke_mode {
            body.insert("InvokeMode".to_string(), Value::String(mode.clone()));
        }
        let body_val = Value::Object(body);
        let path = format!("/2021-10-31/functions/{}/url", self.function_name);
        let result = client.execute("lambda", "POST", &path, None, Some(&body_val), None).await?;

        span.add_event("received result from aws lambda", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);
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
        let input = LambdaCreateFunctionUrlConfigInputBuilder::default().function_name("f").auth_type("NONE").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_create_function_url_config");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"function_name": "f", "auth_type": "NONE"});
        let _: LambdaCreateFunctionUrlConfigInput = serde_json::from_value(json).unwrap();
    }
}
