use crate::api::lib::PosthogApi;
use crate::output::PosthogJsonOutput;
use crate::request::PosthogRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use posthog_core::{PosthogAsync, PosthogTx};
use serde_json::Value;
use std::collections::HashMap;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<PosthogApi, CustomInput> =
    ApiInfo::new(EpKind::Posthog, PosthogApi::Custom, "Execute a custom PostHog API request", ReqType::Write);

crate::posthog_endpoint! {
    Custom,
    API_INFO,
    struct {
        method: String,
        path: String,
        body: Option<Value>,
        headers: Option<HashMap<String, String>>
    }
}

impl_simple_operation!(SimpleInput, PosthogAsync, PosthogTx, PosthogApi, PosthogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: PosthogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("posthog.{}.{}", API_INFO.api(), function_name!()));

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.request(&self.method, &self.path, self.body.clone(), self.headers.as_ref()).await?;

        span.add_event("received result from posthog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(PosthogJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut PosthogTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("PostHog transaction support not implemented")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_builder_serde_with_headers() {
        let input = CustomInputBuilder::default()
            .method("POST")
            .path("/api/v1/some/endpoint")
            .body(Some(serde_json::json!({"key": "value"})))
            .headers(Some(HashMap::from([("x-test-header".to_string(), "enabled".to_string())])))
            .build()
            .expect("Failed to build CustomInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "custom");
        assert_eq!(json["method"], "POST");
        assert_eq!(json["path"], "/api/v1/some/endpoint");
        assert_eq!(json["body"]["key"], "value");
        assert_eq!(json["headers"]["x-test-header"], "enabled");
    }

    #[test]
    fn custom_deserialize_headers() {
        let json = serde_json::json!({
            "method": "GET",
            "path": "/events",
            "headers": {
                "x-test-header": "enabled"
            }
        });

        let input: CustomInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.method, "GET");
        assert_eq!(input.path, "/events");
        assert_eq!(input.headers.unwrap()["x-test-header"], "enabled");
    }
}
