use crate::client::RedisClient;
use crate::config::MultiKeyExecution;
use crate::connection::RedisConnection;
use deadpool::managed::{Manager, Metrics, RecycleResult};
use error::EpError;
use std::sync::Arc;
use telemetry::labels::SYSTEM_ORG_UUID;

/// Connection manager for deadpool
pub struct RedisConnectionManager {
    config: Arc<RedisConnection>,
    org_uuid: String,
    endpoint_uuid: Option<String>,
    multi_key_execution: MultiKeyExecution,
}

impl RedisConnectionManager {
    pub fn new(config: RedisConnection) -> Self {
        Self {
            config: Arc::new(config),
            org_uuid: SYSTEM_ORG_UUID.to_string(),
            endpoint_uuid: None,
            multi_key_execution: MultiKeyExecution::default(),
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

    pub fn with_multi_key_execution(mut self, mode: MultiKeyExecution) -> Self {
        self.multi_key_execution = mode;
        self
    }

    pub fn max_retries(&self) -> u32 {
        self.config.max_retries()
    }

    pub fn multi_key_execution(&self) -> MultiKeyExecution {
        self.multi_key_execution
    }
}

impl Manager for RedisConnectionManager {
    type Type = RedisClient;
    type Error = EpError;

    async fn create(&self) -> Result<Self::Type, Self::Error> {
        RedisClient::connect_with_org_endpoint_and_policy(
            &self.config,
            self.org_uuid.clone(),
            self.endpoint_uuid.clone(),
            self.multi_key_execution,
        )
        .await
    }

    async fn recycle(&self, conn: &mut Self::Type, _metrics: &Metrics) -> RecycleResult<Self::Error> {
        if conn.is_connected().await {
            // We intentionally reconnect dirty sessions instead of trying to
            // unwind AUTH/SELECT/pubsub state in place. That path is more
            // expensive, but it keeps pooled connections isolated and should be
            // rare because the proxy blocks those stateful commands.
            conn.reset_session_state().await.map_err(deadpool::managed::RecycleError::Backend)?;
            Ok(())
        } else {
            Err(deadpool::managed::RecycleError::message("client disconnected"))
        }
        // // Send PING to verify connection is still alive
        // let ping_cmd = b"*1\r\n$4\r\nPING\r\n";

        // match conn.send_command_raw(ping_cmd).await {
        //     Ok(_) => Ok(()),
        //     Err(e) => Err(deadpool::managed::RecycleError::Backend(e)),
        // }
    }
}
