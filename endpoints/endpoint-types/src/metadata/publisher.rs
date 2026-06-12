use std::sync::Arc;

use database::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_logger_internal::{LogAudience, log_info, trace_context};
use error::ResultEP;
use format::cache_uuid::EndpointCacheUuid;

use super::SyncFrequency;

cfg_if::cfg_if! {
    if #[cfg(embedded_db)] {
        #[path = "publisher_backend_embedded_db.rs"]
        mod backend;
    } else {
        #[path = "publisher_backend.rs"]
        mod backend;
    }
}

/// Concrete metadata publisher that writes poll batches to the internal cache.
///
/// Replaces the former `MetadataPublisher` trait + `RedisMetadataPublisher`
/// impl.  There was only ever one implementation, so a concrete struct
/// removes the unnecessary indirection.
pub struct MetadataOutputs {
    db_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    prefix: String,
}

impl MetadataOutputs {
    pub fn new(db_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>) -> Self {
        Self { db_manager, prefix: "metadata:".to_string() }
    }

    pub fn new_with_prefix(db_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>, prefix: impl Into<String>) -> Self {
        Self { db_manager, prefix: prefix.into() }
    }

    pub fn db_manager(&self) -> &Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>> {
        &self.db_manager
    }

    pub async fn publish(&self, endpoint: &EndpointCacheUuid, frequency: SyncFrequency, batch_json: String) -> ResultEP<()> {
        let key = format!("{}{}", self.prefix, endpoint);
        let field = frequency.as_str();

        backend::publish(&self.db_manager, &key, field, &batch_json).await?;

        let ctx = trace_context().with_function("MetadataOutputs::publish");
        log_info!(
            ctx,
            "metadata batch stored in internal cache",
            audience = LogAudience::Internal,
            endpoint = endpoint.to_string(),
            key = key,
            field = field.to_string()
        );
        Ok(())
    }

    pub async fn read(&self, endpoint: &EndpointCacheUuid, frequency: SyncFrequency) -> ResultEP<Option<String>> {
        let key = format!("{}{}", self.prefix, endpoint);
        backend::read(&self.db_manager, &key, frequency.as_str()).await
    }
}

pub fn default_publisher(db_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>) -> Arc<MetadataOutputs> {
    Arc::new(MetadataOutputs::new(db_manager))
}

pub fn default_publisher_with_prefix(
    db_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
    prefix: impl Into<String>,
) -> Arc<MetadataOutputs> {
    Arc::new(MetadataOutputs::new_with_prefix(db_manager, prefix))
}
