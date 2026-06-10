use crate::api::lib::FunctionApi;
use crate::output::{FunctionInvokeOutput, normalize_payload};
use crate::request::FunctionRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_core::{FunctionAsync, FunctionInvocationType, FunctionInvokeRequest, FunctionLogType, FunctionTx};
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<FunctionApi, InvokeInput> =
    ApiInfo::new(EpKind::Function, FunctionApi::Invoke, "Invoke a serverless function", ReqType::Write, true);

crate::function_endpoint! {
    Invoke,
    API_INFO,
    struct {
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        function_name: Option<String>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "serde_json::Value::is_null")]
        payload: Value,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        qualifier: Option<String>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        client_context_base64: Option<String>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        invocation_type: Option<FunctionInvocationType>,
        #[builder(default)]
        #[serde(default, skip_serializing_if = "Option::is_none")]
        log_type: Option<FunctionLogType>,
    }
}

impl_simple_operation!(SimpleInput, FunctionAsync, FunctionTx, FunctionApi, FunctionRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: FunctionAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("function.{}.{}", API_INFO.api(), function_name!()));
        let start = std::time::SystemTime::now();

        let client = context.get().await.map_err(EpError::connect)?;
        let response = client
            .invoke(&FunctionInvokeRequest {
                function_name: self.function_name().clone(),
                payload: (!self.payload().is_null()).then(|| self.payload().clone()),
                qualifier: self.qualifier().clone(),
                client_context_base64: self.client_context_base64().clone(),
                invocation_type: *self.invocation_type(),
                log_type: *self.log_type(),
            })
            .await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from function provider",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
                FastSpanAttribute::new("provider", format!("{:?}", response.provider)),
            ],
        );

        let output = FunctionInvokeOutput {
            provider: response.provider,
            function_name: response.function_name,
            status_code: response.status_code,
            executed_version: response.executed_version,
            function_error: response.function_error,
            log_result_base64: response.log_result_base64,
            request_id: response.request_id,
            payload: normalize_payload(response.payload.as_deref()),
        };

        Ok(Box::new(output.to_output()) as Box<dyn EpOutput>)
    }

    #[named]
    fn run_transaction_generic(&self, context: &mut FunctionTx, telemetry_wrapper: &mut TelemetryWrapper) {
        let _ = context;
        let _ = telemetry_wrapper;

        let log_ctx = ctx_with_trace!().with_feature("function.endpoint.invoke");
        log_warn!(
            log_ctx,
            "Function transactions are not supported; operation ignored",
            audience = LogAudience::Internal
        );
    }
}
