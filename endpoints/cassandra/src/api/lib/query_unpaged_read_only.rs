use crate::api::lib::CassandraApi;
use crate::output::CassandraQueryOutput;
use crate::request::CassandraRequest;
use crate::{ApiInfo, ReqType, RunOutput, ToOutput};
use cassandra_core::{CassandraAsync, CassandraTx};
use ep_core::EpOutput;
use ep_core::impl_simple_operation;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use scylla::response::query_result::QueryResult;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use utoipa::PartialSchema;

const API_INFO: ApiInfo<CassandraApi, QueryUnpagedReadOnlyInput> = ApiInfo::new(
    EpKind::Cassandra,
    CassandraApi::QueryUnpagedReadOnly,
    "Cassandra read-only Query Unpaged",
    ReqType::Read,
    true,
);

crate::cassandra_endpoint! {
    QueryUnpagedReadOnly,
    API_INFO,
    struct {
        query: String,
    }
}

type OutputWrapper = CassandraQueryOutput;

impl_simple_operation!(SimpleInput, CassandraAsync, CassandraTx, CassandraApi, CassandraRequest);

impl SimpleInput {
    pub(crate) fn new(query: String) -> Self {
        Self { query }
    }
    pub async fn run_query(&self, context: CassandraAsync) -> ResultEP<QueryResult> {
        let context = context.get().await.map_err(EpError::connect)?;

        context.query_unpaged(self.query().as_str(), &[]).await.map_err(EpError::request)
    }
    #[named]
    async fn run_async_generic(&self, context: CassandraAsync, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<Box<dyn EpOutput>> {
        let mut span = telemetry_wrapper.client_tracer(format!("cassandra.{}.{}", API_INFO.api, function_name!()));

        let start = std::time::SystemTime::now();

        let response = self.run_query(context).await?;

        let duration = start.elapsed().map_err(EpError::request)?.as_millis();

        span.add_event(
            "received result from cassandra",
            vec![
                FastSpanAttribute::new("type", API_INFO.api.to_string()),
                FastSpanAttribute::new("duration", duration.to_string()),
            ],
        );

        Ok(Box::new(CassandraQueryOutput(response).to_output()) as Box<dyn EpOutput>)
    }
    #[named]
    fn run_transaction_generic(&self, _context: &mut CassandraTx, _telemetry_wrapper: &mut TelemetryWrapper) {
        todo!("")
    }
}
