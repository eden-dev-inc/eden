use crate::api::lib::DatadogApi;
use crate::api::wrapper::output::DatadogJsonOutput;
use crate::request::DatadogRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use datadog_core::{DatadogAsync, DatadogTx};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<DatadogApi, CustomInput> = ApiInfo::new(
    EpKind::Datadog,
    DatadogApi::Custom,
    "Executes a custom Datadog API request with user-specified method and path",
    ReqType::Write,
    true,
);

crate::datadog_endpoint! {
    Custom,
    API_INFO,
    struct {
        method: String,
        path: String,
        body: Option<Value>
    }
}

impl_simple_operation!(SimpleInput, DatadogAsync, DatadogTx, DatadogApi, DatadogRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: DatadogAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("datadog.{}.{}", API_INFO.api, function_name!()));

        let client = context.get().await.map_err(EpError::request)?;

        let result = match self.method.to_uppercase().as_str() {
            "GET" => client.get(&self.path).await?,
            "POST" => client.post(&self.path, self.body.clone()).await?,
            "PUT" => client.put(&self.path, self.body.clone()).await?,
            "DELETE" => client.delete(&self.path).await?,
            other => return Err(EpError::request(format!("unsupported HTTP method: {other}"))),
        };

        span.add_event("received result from datadog", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(DatadogJsonOutput(result).to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, _context: &mut DatadogTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn custom_builder_serde() {
        let input = CustomInputBuilder::default()
            .method("POST")
            .path("/api/v1/some/endpoint")
            .body(Some(serde_json::json!({"key": "value"})))
            .build()
            .expect("Failed to build CustomInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "custom");
        assert_eq!(json["method"], "POST");
        assert_eq!(json["path"], "/api/v1/some/endpoint");
        assert_eq!(json["body"]["key"], "value");
    }

    #[test]
    fn custom_builder_no_body() {
        let input = CustomInputBuilder::default()
            .method("GET")
            .path("/api/v1/check")
            .body(None::<Value>)
            .build()
            .expect("Failed to build CustomInput");

        let json = serde_json::to_value(&input).expect("Failed to serialize");
        assert_eq!(json["type"], "custom");
        assert_eq!(json["method"], "GET");
        assert!(json["body"].is_null());
    }

    #[test]
    fn custom_deserialize() {
        let json = serde_json::json!({
            "method": "DELETE",
            "path": "/api/v1/resource/123",
            "body": null
        });
        let input: CustomInput = serde_json::from_value(json).expect("Failed to deserialize");
        assert_eq!(input.method, "DELETE");
        assert_eq!(input.path, "/api/v1/resource/123");
        assert!(input.body.is_none());
    }
}
