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

const API_INFO: ApiInfo<AwsApi, LambdaCreateAliasInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LambdaCreateAlias,
    "Creates an alias for a Lambda function version",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    LambdaCreateAlias,
    API_INFO,
    struct {
        function_name: String,
        name: String,
        function_version: String,
        description: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::json!({
            "Name": self.name,
            "FunctionVersion": self.function_version
        });
        if let Some(desc) = &self.description {
            body.as_object_mut().map(|m| m.insert("Description".to_string(), Value::String(desc.clone())));
        }
        let path = format!("/2015-03-31/functions/{}/aliases", self.function_name);
        let result = client.execute("lambda", "POST", &path, None, Some(&body), None).await?;

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
        let input = LambdaCreateAliasInputBuilder::default().function_name("f").name("prod").function_version("1").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_create_alias");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"function_name": "f", "name": "prod", "function_version": "1"});
        let _: LambdaCreateAliasInput = serde_json::from_value(json).unwrap();
    }
}
