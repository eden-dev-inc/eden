use crate::client::PostgresClient;
use crate::url::PostgresConnectionParsed;
use deadpool::managed::{Manager, Metrics, RecycleResult};
use error::EpError;
use std::sync::Arc;
use telemetry::labels::SYSTEM_ORG_UUID;

/// Connection manager for deadpool-managed PostgreSQL raw wire connections.
pub struct PgConnectionManager {
    config: Arc<PostgresConnectionParsed>,
    org_uuid: String,
    endpoint_uuid: Option<String>,
    recycle_check: bool,
}

impl PgConnectionManager {
    pub fn new(config: PostgresConnectionParsed) -> Self {
        Self {
            config: Arc::new(config),
            org_uuid: SYSTEM_ORG_UUID.to_string(),
            endpoint_uuid: None,
            recycle_check: false,
        }
    }

    /// Tag this manager with the owning organization UUID so connection gauges
    /// are tenant-scoped.
    pub fn with_org_uuid(mut self, org_uuid: impl Into<String>) -> Self {
        self.org_uuid = org_uuid.into();
        self
    }

    /// Tag this manager with the owning endpoint UUID so connections report
    /// per-endpoint labels on the `eden.connections` gauge.
    pub fn with_endpoint_uuid(mut self, endpoint_uuid: impl Into<String>) -> Self {
        self.endpoint_uuid = Some(endpoint_uuid.into());
        self
    }

    /// Enable or disable active socket checks before an idle connection is
    /// handed out again. The gateway hot path defaults this off because failed
    /// sockets are already discarded when a request write/read fails.
    pub fn with_recycle_check(mut self, recycle_check: bool) -> Self {
        self.recycle_check = recycle_check;
        self
    }
}

/// Maximum number of connection attempts before giving up.
const MAX_CONNECT_RETRIES: u32 = 5;
/// Base delay between retry attempts (doubles each retry).
const RETRY_BASE_DELAY_MS: u64 = 200;

impl Manager for PgConnectionManager {
    type Type = PostgresClient;
    type Error = EpError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        let mut last_err = None;
        for attempt in 0..MAX_CONNECT_RETRIES {
            match PostgresClient::connect_with_org_endpoint(&self.config, self.org_uuid.clone(), self.endpoint_uuid.clone()).await {
                Ok(client) => return Ok(client),
                Err(e) => {
                    let delay = RETRY_BASE_DELAY_MS * (1 << attempt);
                    last_err = Some(e);
                    tokio::time::sleep(std::time::Duration::from_millis(delay)).await;
                }
            }
        }
        Err(last_err.unwrap_or_else(|| EpError::connect("connection failed after retries")))
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> RecycleResult<Self::Error> {
        if !self.recycle_check || conn.is_connected().await {
            Ok(())
        } else {
            Err(deadpool::managed::RecycleError::message("client disconnected"))
        }
    }
}
