use crate::api::lib::MongoApi;
use crate::api::wrapper::{ChangeStreamOptionsWrapper, DocumentFunction, DocumentWrapperType};
use crate::output::ChangeStreamOutput;
use crate::request::MongoRequest;
use crate::{ApiInfo, EpOutput, ReqType, RunOutput, ToOutput};
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::{MongoAsync, MongoTx};
use std::borrow::Cow;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use tokio::sync::Mutex;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<MongoApi, WatchInput> = ApiInfo::new(
    EpKind::Mongo,
    MongoApi::Watch,
    "Starts a new ChangeStream that receives events for all changes in the cluster",
    ReqType::Read,
    true,
);

crate::mongo_endpoint! {
    API_INFO,
    struct WatchInput {
        pipeline: Vec<DocumentWrapperType>,
        options: Option<ChangeStreamOptionsWrapper>,
    }
}

type OutputWrapper = ChangeStreamOutput;

impl_simple_operation!(WatchInput, MongoAsync, MongoTx, MongoApi, MongoRequest);

impl WatchInput {
    #[named]
    async fn run_async_generic(&self, context: MongoAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}.{}", API_INFO.endpoint, API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let result = context
            .watch(
                self.pipeline.clone().into_iter().map(DocumentFunction::into_document),
                self.options.to_owned().map(Into::into),
            )
            .await
            .map_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                EpError::database(e)
            })?;
        Ok(Box::new(ChangeStreamOutput(Mutex::new(result)).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, context: &mut MongoTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        // TODO: Await the future or use tokio::spawn to avoid silently dropping it
        #[allow(clippy::let_underscore_future)]
        let _ = Box::pin(async {
            context
                .client()
                .watch(
                    self.pipeline.clone().into_iter().map(DocumentFunction::into_document),
                    self.options.to_owned().map(Into::into),
                )
                .await
        });
    }
}
