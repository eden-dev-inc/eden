use crate::api::lib::CassandraApi;
use cassandra_core::{CassandraAsync, CassandraSync};
use crate::output::{CassandraQueryOutput, CassandraQueryPagedOutput};
use crate::pinecone::comm::PineconeRequests;
use ep_core::impl_simple_operation;
use crate::{ EpOutput, Operation, RunOutput};
use error::EpError;
use function_name::named;
use opentelemetry::KeyValue;
use scylla::statement::PagingState;
use telemetry::TelemetryWrapper;
use telemetry::FastSpanAttribute;

const KIND: CassandraApi = CassandraApi::QuerySinglePage;

crate::cassandra_endpoint! {
    QuerySinglePage,
    API_INFO,
    struct {
        query: String,
    }
}

type OutputWrapper = CassandraQueryOutput;

impl_simple_operation!(SimpleInput, CassandraAsync, CassandraApi, KIND);

impl SimpleInput {
    #[named]
    fn run_sync_generic(
        &self,
        context: CassandraSync,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> RunOutput {
        Box::pin(async move {
            let span_context = telemetry_wrapper
                .client_tracer(format!("cassandra.{}.{}", self.kind(), function_name!()))
                .await;
            let start = std::time::SystemTime::now();

            let prepared = context
                .prepare(self.query().as_str())
                .await
                .map_err(EpError::request)?;

            let paging_state = PagingState::start();
            let result = context
                .execute_single_page(&prepared, &[], paging_state)
                .await
                .map_err(EpError::request)?;

            let duration = start.elapsed().map_err(EpError::request)?.as_millis();

            span.add_event(
                "received result from cassandra",
                vec![
                    FastSpanAttribute::new("type", KIND.to_string()),
                    FastSpanAttribute::new("duration", duration.to_string()),
                ],
            );

            Ok(Box::new(CassandraQueryPagedOutput(result).to_output()) as Box<dyn EpOutput>)
        })
    }
    #[named]
    fn run_async_generic(
        &self,
        context: CassandraAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> RunOutput {
        Box::pin(async move {
            let span_context = telemetry_wrapper
                .client_tracer(format!("cassandra.{}.{}", self.kind(), function_name!()))
                .await;
            let context = context.get().await.map_err(EpError::connect)?;

            let start = std::time::SystemTime::now();

            let prepared = context
                .prepare(self.query().as_str())
                .await
                .map_err(EpError::request)?;

            let paging_state = PagingState::start();
            let result = context
                .execute_single_page(&prepared, &[], paging_state)
                .await
                .map_err(EpError::request)?;

            let duration = start.elapsed().map_err(EpError::request)?.as_millis();

            span.add_event(
                "received result from cassandra",
                vec![
                    FastSpanAttribute::new("type", KIND.to_string()),
                    FastSpanAttribute::new("duration", duration.to_string()),
                ],
            );

            Ok(Box::new(CassandraQueryPagedOutput(result).to_output()) as Box<dyn EpOutput>)
        })
    }
}
