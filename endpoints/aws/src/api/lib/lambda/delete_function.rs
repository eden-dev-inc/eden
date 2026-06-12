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

const API_INFO: ApiInfo<AwsApi, LambdaDeleteFunctionInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::LambdaDeleteFunction, "Deletes a Lambda function", ReqType::Write, true);

crate::aws_endpoint! {
    LambdaDeleteFunction,
    API_INFO,
    struct {
        function_name: String,
        qualifier: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/2015-03-31/functions/{}", self.function_name);
        let query = self.qualifier.as_ref().map(|q| format!("Qualifier={}", q));
        let result = client.execute("lambda", "DELETE", &path, query.as_deref(), None, None).await?;

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
        let input = LambdaDeleteFunctionInputBuilder::default().function_name("f").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_delete_function");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"function_name": "f"});
        let _: LambdaDeleteFunctionInput = serde_json::from_value(json).unwrap();
    }
}
