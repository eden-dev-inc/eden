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

const API_INFO: ApiInfo<AwsApi, ApiGatewayV2CreateApiInput> =
    ApiInfo::new(EpKind::Aws, AwsApi::ApiGatewayV2CreateApi, "apigatewayv2_create_api", ReqType::Write, true);

crate::aws_endpoint! {
    ApiGatewayV2CreateApi,
    API_INFO,
    struct {
        name: String,
        protocol_type: String,
        description: Option<String>
    }
}

impl_simple_operation!(SimpleInput, AwsAsync, AwsTx, AwsApi, AwsRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: AwsAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("aws.{}.{}", API_INFO.api, function_name!()));
        let client = context.get().await.map_err(EpError::request)?;

        let mut body_map = serde_json::Map::new();
        body_map.insert("name".to_string(), serde_json::json!(self.name));
        body_map.insert("protocolType".to_string(), serde_json::json!(self.protocol_type));
        if let Some(d) = &self.description {
            body_map.insert("description".to_string(), serde_json::json!(d));
        }
        let body = serde_json::Value::Object(body_map);
        let result = client.execute("apigatewayv2", "POST", "/v2/apis", None, Some(&body), None).await?;

        span.add_event(
            "received result from aws apigatewayv2",
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
        let input = ApiGatewayV2CreateApiInputBuilder::default().name("my-api").protocol_type("HTTP").build().unwrap();
        let json = serde_json::to_value(&input).unwrap();
        assert_eq!(json["type"], "apigatewayv2_create_api");
    }

    #[test]
    fn deserialize_minimal() {
        let json = serde_json::json!({"name": "my-api", "protocol_type": "HTTP"});
        let _: ApiGatewayV2CreateApiInput = serde_json::from_value(json).unwrap();
    }
}
