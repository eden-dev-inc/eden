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

const API_INFO: ApiInfo<AwsApi, LambdaUpdateEventSourceMappingInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LambdaUpdateEventSourceMapping,
    "Updates a Lambda event source mapping",
    ReqType::Write,
    true,
);

crate::aws_endpoint! {
    LambdaUpdateEventSourceMapping,
    API_INFO,
    struct {
        uuid: String,
        function_name: Option<String>,
        batch_size: Option<i64>,
        enabled: Option<bool>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body = serde_json::json!({});
        if let Some(fn_name) = &self.function_name {
            body["FunctionName"] = serde_json::json!(fn_name);
        }
        if let Some(bs) = &self.batch_size {
            body["BatchSize"] = serde_json::json!(bs);
        }
        if let Some(en) = &self.enabled {
            body["Enabled"] = serde_json::json!(en);
        }
        let path = format!("/2015-03-31/event-source-mappings/{}", self.uuid);
        let result = client.execute("lambda", "PUT", &path, None, Some(&body), None).await?;

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
        let input = LambdaUpdateEventSourceMappingInputBuilder::default().uuid("abc-123").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_update_event_source_mapping");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"uuid": "abc-123"});
        let _: LambdaUpdateEventSourceMappingInput = serde_json::from_value(json).unwrap();
    }
}
