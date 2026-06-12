// use crate::api::lib::CassandraApi;
// use cassandra_core::{CassandraAsync, CassandraSync};
// use crate::output::CassandraQueryOutput;
// use crate::pinecone::comm::PineconeRequests;
// use ep_core::impl_simple_operation;
// use ep_core::ep::EpOutput;
// use error::EpError;
// use function_name::named;
// use opentelemetry::trace::Span;
// use opentelemetry::KeyValue;
// use tonic::metadata::MetadataMap;
// use trace::client_tracer_config;
//
// const KIND: CassandraApi = CassandraApi::QuerySinglePage;
//
// crate::cassandra_endpoint! {
//     QuerySinglePage,
//     struct {
//         query: String,
//     }
// }
//
// type OutputWrapper = CassandraQueryOutput;
//
// impl_simple_operation!(
//     SimpleInput,
//     CassandraSync,
//     CassandraAsync,
//     CassandraApi,
//     KIND
// );
//
// impl SimpleInput {
//     #[named]
//     fn run_sync_generic(&self, context: CassandraSync, telemetry_context: TelemetryWrapper) -> RunOutput {
//         let mut span = client_tracer_config(
//             format!("cassandra.{}.{}", self.kind(), function_name!()),
//             &metadata_map,
//         );
//
//         Box::pin(async move {
//             let start = std::time::SystemTime::now();
//
//             let value = context
//                 .query_iter(self.query(), &[])
//                 .await
//                 .map_err(EpError::request)?;
//
//             let duration = start.elapsed().map_err(EpError::request)?.as_millis();
//
//             span.add_event(
//                 "received result from cassandra",
//                 vec![
//                     FastSpanAttribute::new("type", KIND.to_string()),
//                     FastSpanAttribute::new("duration", duration.to_string()),
//                 ],
//             );
//
//             Ok(Box::new(CassandraQueryOutput(value).to_output()) as Box<dyn EpOutput>)
//         })
//     }
//     #[named]
//     fn run_async_generic(&self, context: CassandraAsync, telemetry_context: TelemetryWrapper) -> RunOutput {
//         let mut span = client_tracer_config(
//             format!("cassandra.{}.{}", self.kind(), function_name!()),
//             &metadata_map,
//         );
//
//         Box::pin(async move {
//             let context = context.get().await.map_err(EpError::connect)?;
//
//             let start = std::time::SystemTime::now();
//
//             let value = context
//                 .query_iter(self.query(), &[])
//                 .await
//                 .map_err(EpError::request)?;
//
//             let duration = start.elapsed().map_err(EpError::request)?.as_millis();
//
//             span.add_event(
//                 "received result from cassandra",
//                 vec![
//                     FastSpanAttribute::new("type", KIND.to_string()),
//                     FastSpanAttribute::new("duration", duration.to_string()),
//                 ],
//             );
//
//             Ok(Box::new(CassandraQueryOutput(value).to_output()) as Box<dyn EpOutput>)
//         })
//     }
// }
