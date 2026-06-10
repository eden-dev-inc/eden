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

const API_INFO: ApiInfo<AwsApi, LambdaListAliasesInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::LambdaListAliases,
    "Returns a list of aliases for a Lambda function",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    LambdaListAliases,
    API_INFO,
    struct {
        function_name: String,
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
        if let Some(max) = self.max_items {
            query_parts.push(format!("MaxItems={}", max));
        }
        if let Some(m) = &self.marker {
            query_parts.push(format!("Marker={}", m));
        }
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let path = format!("/2015-03-31/functions/{}/aliases", self.function_name);
        let result = client.execute("lambda", "GET", &path, query.as_deref(), None, None).await?;

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
        let input = LambdaListAliasesInputBuilder::default().function_name("f").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "lambda_list_aliases");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"function_name": "f"});
        let _: LambdaListAliasesInput = serde_json::from_value(json).unwrap();
    }
}
