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

const API_INFO: ApiInfo<AwsApi, LambdaListEventSourceMappingsInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LambdaListEventSourceMappings,
    "lambda_list_event_source_mappings",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    LambdaListEventSourceMappings,
    API_INFO,
    struct {
        function_name: Option<String>,
        max_items: Option<i64>,
        marker: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut query_parts = Vec::new();
        if let Some(f) = &self.function_name {
            query_parts.push(format!("FunctionName={}", f));
        }
        if let Some(m) = self.max_items {
            query_parts.push(format!("MaxItems={}", m));
        }
        if let Some(mk) = &self.marker {
            query_parts.push(format!("Marker={}", mk));
        }
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let result = client.execute("lambda", "GET", "/2015-03-31/event-source-mappings/", query.as_deref(), None, None).await?;

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
        let input = LambdaListEventSourceMappingsInputBuilder::default().build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_list_event_source_mappings");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({});
        let _: LambdaListEventSourceMappingsInput = serde_json::from_value(json).unwrap();
    }
}
