use crate::api::lib::WeaviateApi;
use crate::output::WeaviateValueOutput;
use crate::request::WeaviateRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::EpOutput;
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;
use weaviate_core::comm::WeaviateRequests;
use weaviate_core::{WeaviateAsync, WeaviateTx};

const API_INFO: ApiInfo<WeaviateApi, GraphQLInput> =
    ApiInfo::new(EpKind::Weaviate, WeaviateApi::GraphQL, "Weaviate GraphQL", ReqType::Read, true);

crate::weaviate_endpoint! {
    GraphQL,
    API_INFO,
    struct {
        body: String,
    }
}

type OutputWrapper = WeaviateValueOutput;

impl_simple_operation!(SimpleInput, WeaviateAsync, WeaviateTx, WeaviateApi, WeaviateRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: WeaviateAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("weaviate.{}.{}", self.kind(), function_name!()));
        let context = context.get().await.map_err(EpError::connect)?;

        let start = std::time::Instant::now();

        let value = context.graphql(self.body().to_owned()).await?;

        let duration = start.elapsed().as_millis();

        span.add_event(
            "received result from weaviate",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(WeaviateValueOutput(value).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut WeaviateTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
