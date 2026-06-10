use crate::api::lib::LlmApi;
use crate::output::LlmOutput;
use crate::request::LlmRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_warn};
use ep_core::{EpOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use llm_core::{LlmAsync, LlmParam, LlmTx};
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<LlmApi, LlmOutput> =
    ApiInfo::new(EpKind::Llm, LlmApi::SetParam, "Set LLM connection parameter", ReqType::Read, true);

crate::llm_endpoint! {
    SetParam,
    API_INFO,
    struct {
        param: LlmParam,
        value: Value,
    }
}

type OutputWrapper = LlmOutput;

impl_simple_operation!(SimpleInput, LlmAsync, LlmTx, LlmApi, LlmRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: LlmAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("llm.{}.{}", API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let start = std::time::SystemTime::now();

        context.set_param(*self.param(), self.value()).map_err(EpError::request)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "set LLM connection parameter",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(LlmOutput::message("parameter updated").to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut LlmTx, telemetry_wrapper: &mut TelemetryWrapper) {
        let _ = context;
        let _ = telemetry_wrapper;
        let log_ctx = ctx_with_trace!().with_feature("llm.endpoint.set_param");
        log_warn!(log_ctx, "LLM transactions are not supported; operation ignored", audience = LogAudience::Internal);
    }
}
