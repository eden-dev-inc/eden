use crate::api::lib::TavilyApi;
use crate::output::TavilyJsonOutput;
use crate::request::TavilyRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use tavily_core::{TavilyAsync, TavilyTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<TavilyApi, ExtractInput> = ApiInfo::new(
    EpKind::Tavily,
    TavilyApi::Extract,
    "Extract clean content from web pages using Tavily",
    ReqType::Read,
);

crate::tavily_endpoint! {
    Extract,
    API_INFO,
    struct {
        body: Value
    }
}

impl_simple_operation!(SimpleInput, TavilyAsync, TavilyTx, TavilyApi, TavilyRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: TavilyAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("tavily.{}.{}", API_INFO.api(), function_name!()));

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.post("/extract", self.body.clone()).await?;

        span.add_event("received result from tavily", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(TavilyJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut TavilyTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Tavily transaction support not implemented")
    }
}
