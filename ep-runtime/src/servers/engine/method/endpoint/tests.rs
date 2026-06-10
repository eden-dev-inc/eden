mod cassandra;
mod clickhouse;
mod mongo;
mod pinecone;
mod postgres;

use eden_core::error::EpError;

use opentelemetry::{global::BoxedSpan, trace::Span};

use crate::comp::MyEngineService;

impl MyEngineService {
    pub async fn test(&self, span: &mut BoxedSpan) -> Result<String, EpError> {
        span.add_event("Testing...", vec![]);
        Ok("test-passed".to_string())
    }
}
