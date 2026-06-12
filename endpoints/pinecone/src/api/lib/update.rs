use crate::api::lib::PineconeApi;
use crate::output::PineconeValueOutput;
use crate::request::PineconeRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::EpOutput;
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use pinecone_core::comm::PineconeRequests;
use pinecone_core::{PineconeAsync, PineconeTx};
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<PineconeApi, UpdateInput> =
    ApiInfo::new(EpKind::Mongo, PineconeApi::Update, "Pinecone Update", ReqType::Write, true);

crate::pinecone_endpoint! {
    Update,
    API_INFO,
    struct {
        body: String,
    }
}

type OutputWrapper = PineconeValueOutput;

impl_simple_operation!(SimpleInput, PineconeAsync, PineconeTx, PineconeApi, PineconeRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: PineconeAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("pinecone.{}.{}", API_INFO.api, function_name!()));
        let context = context.get().await.map_err(EpError::connect)?;

        let start = std::time::SystemTime::now();

        let value = context.update(self.body().to_owned()).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from pinecone",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(PineconeValueOutput(value).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut PineconeTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
