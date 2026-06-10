use crate::api::lib::CassandraApi;
use crate::output::CassandraQueryPagedOutput;
use crate::request::CassandraRequest;
use cassandra_core::{CassandraAsync, CassandraTx};
use endpoint_types::{ApiInfo, ReqType, RunOutput, ToOutput};
use ep_core::EpOutput;
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use scylla::response::PagingState;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<CassandraApi, QuerySinglePageReadOnlyInput> = ApiInfo::new(
    EpKind::Cassandra,
    CassandraApi::QuerySinglePageReadOnly,
    "Cassandra read-only Query Single Page",
    ReqType::Read,
    true,
);

crate::cassandra_endpoint! {
    QuerySinglePageReadOnly,
    API_INFO,
    struct {
        query: String,
    }
}

type OutputWrapper = CassandraQueryPagedOutput;

impl_simple_operation!(SimpleInput, CassandraAsync, CassandraTx, CassandraApi, CassandraRequest);

impl SimpleInput {
    #[named]
    async fn run_async_generic(&self, context: CassandraAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("cassandra.{}.{}", API_INFO.api, function_name!()));

        let context = context.get().await.map_err(EpError::connect)?;

        let start = std::time::SystemTime::now();

        let paging_state = PagingState::start();
        let value = context.query_single_page(self.query().as_str(), &[], paging_state).await.map_err(EpError::request)?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from cassandra",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(CassandraQueryPagedOutput(value).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut CassandraTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
