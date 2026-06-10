use crate::api::lib::MongoApi;
use crate::api::wrapper::SessionOptionsWrapper;
use crate::output::ClientSessionOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::EpOutput;
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use std::borrow::Cow;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, StartSessionInput> =
    ApiInfo::new(EpKind::Mongo, MongoApi::StartSession, "Starts a new ClientSession", ReqType::Write, true);

crate::mongo_endpoint! {
    API_INFO,
    struct StartSessionInput {
        options: SessionOptionsWrapper,
    }
}

type OutputWrapper = ClientSessionOutput;

impl_simple_operation!(StartSessionInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl StartSessionInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let session = context.start_session(self.options.as_session_options()).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e.to_string())
        })?;

        Ok(Box::new(ClientSessionOutput(session).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        context.client().selection_criteria();
    }
}
