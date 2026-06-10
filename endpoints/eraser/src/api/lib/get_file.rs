use crate::api::lib::EraserApi;
use crate::output::EraserJsonOutput;
use crate::request::EraserRequest;
use ep_core::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput, impl_simple_operation};
use eraser_core::{EraserAsync, EraserTx};
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;

const API_INFO: ApiInfo<EraserApi, GetFileInput> =
    ApiInfo::new(EpKind::Eraser, EraserApi::GetFile, "Get an Eraser file by ID", ReqType::Read);

crate::eraser_endpoint! {
    GetFile,
    API_INFO,
    struct {
        body: Value
    }
}

impl_simple_operation!(SimpleInput, EraserAsync, EraserTx, EraserApi, EraserRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: EraserAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("eraser.{}.{}", API_INFO.api(), function_name!()));

        let file_id = self.body.get("fileId").and_then(|v| v.as_str()).ok_or_else(|| EpError::parse("missing fileId"))?;
        let path = format!("/api/v1/files/{file_id}");

        let client = context.get().await.map_err(EpError::connect)?;
        let value = client.get(&path).await?;

        span.add_event("received result from eraser", vec![FastSpanAttribute::new("type", API_INFO.api.to_string())]);

        Ok(Box::new(EraserJsonOutput(value).to_output()) as Box<dyn EpOutput>)
    }

    fn run_transaction_generic(&self, _context: &mut EraserTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("Eraser transaction support not implemented")
    }
}
