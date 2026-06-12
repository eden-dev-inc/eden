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

const API_INFO: ApiInfo<AwsApi, ApiGatewayGetResourcesInput> = ApiInfo::new(
    EpKind::Aws,
    AwsApi::ApiGatewayGetResources,
    "Lists resources for a REST API in API Gateway",
    ReqType::Read,
    true,
);

crate::aws_endpoint! {
    ApiGatewayGetResources,
    API_INFO,
    struct {
        rest_api_id: String,
        limit: Option<i64>,
        position: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let path = format!("/restapis/{}/resources", self.rest_api_id);
        let mut query_parts = Vec::new();
        if let Some(l) = self.limit {
            query_parts.push(format!("limit={}", l));
        }
        if let Some(p) = &self.position {
            query_parts.push(format!("position={}", p));
        }
        let query = if query_parts.is_empty() {
            None
        } else {
            Some(query_parts.join("&"))
        };
        let result = client.execute("apigateway", "GET", &path, query.as_deref(), None, None).await?;

        span.add_event(
            "received result from aws apigateway",
            vec![FastSpanAttribute::new("type", API_INFO.api.to_string())],
        );
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
        let input = ApiGatewayGetResourcesInputBuilder::default().rest_api_id("r").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "apigateway_get_resources");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"rest_api_id": "r"});
        let _: ApiGatewayGetResourcesInput = serde_json::from_value(json).unwrap();
    }
}
