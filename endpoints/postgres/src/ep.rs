use dashmap::DashMap;
use ep_core::database::schema::interlay::InterlayState;
use function_name::named;
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

use crate::api::lib::PostgresApi;
use crate::api::lib::batch_execute::BatchExecuteInput;
use crate::api::lib::copy_in::CopyInInput;
use crate::api::lib::copy_out::CopyOutInput;
use crate::api::lib::execute::ExecuteInput;
use crate::api::lib::query::QueryInput;
use crate::api::lib::query_one::QueryOneInput;
use crate::api::lib::query_one_read_only::QueryOneReadOnlyInput;
use crate::api::lib::query_opt_read_only::QueryOptReadOnlyInput;
use crate::api::lib::query_read_only::QueryReadOnlyInput;
use crate::api::lib::simple_query::SimpleQueryInput;
use crate::api::lib::simple_query_read_only::SimpleQueryReadOnlyInput;
use crate::api::wrapper::output::{
    CancelTokenAsyncOutput, CopyInWriterOutput, CopyOutReaderOutput, EmptyOutput, PostgresOptionRowOutput, PostgresRowOutput,
    PostgresRowsOutput, PostgresSimpleQueryOutput, U64Output,
};
use crate::metadata::PostgresMetadata;
use crate::request::PostgresRequest;
use crate::{EP, EpRequest, EpTransaction, ToOutput, Transaction};
use eden_logger_internal::LogContext;
use ep_core::ep::{EpConfig, EpConnection, EpPool, EpRouter, PoolType, RWPool};
use ep_core::settings::EdenSettings;
use ep_core::{EpOutput, GetPool};
use error::{ConnectError, EpError, ResultEP, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use format::{CacheUuid, OrganizationUuid};
use postgres_core::PgRawPool;
use postgres_core::connection::PostgresConnection;
use postgres_core::extract_command_complete_count;
use postgres_core::pool::PgConnectionManager;
pub(crate) use postgres_core::{PostgresAsync, PostgresConfig, PostgresTx};
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;

// ──────────────────────────────────────────────────────────────────────────────
// PostgresEp: manually defined to add raw wire protocol pool storage.
// Replaces the macro: impl_endpoint!(EpKind::Postgres => Postgres, PostgresAsync);
// ──────────────────────────────────────────────────────────────────────────────

/// Raw wire protocol pool set for an endpoint.
#[derive(Clone)]
#[allow(dead_code)]
struct RawPgPools {
    read: Option<PgRawPool>,
    write: Option<PgRawPool>,
    admin: Option<PgRawPool>,
    system: Option<PgRawPool>,
    _pool_status_pollers: Vec<telemetry::PoolStatusPollerHandle>,
}

#[derive(Clone)]
pub struct PostgresEp {
    pool: EpPool<PostgresAsync>,
    /// Raw wire protocol pools per endpoint — used by the proxy for extended
    /// query protocol and pinned connections (which require wire-level access).
    raw_pools: Arc<DashMap<EndpointCacheUuid, RawPgPools>>,
}

impl Default for PostgresEp {
    fn default() -> Self {
        Self { pool: EpPool::default(), raw_pools: Arc::new(DashMap::new()) }
    }
}

impl Debug for PostgresEp {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PostgresEp").field("pool", &self.pool).finish()
    }
}

impl GetPool<PostgresAsync> for PostgresEp {
    fn pool(&self) -> &EpPool<PostgresAsync> {
        &self.pool
    }
    fn mut_pool(&mut self) -> &mut EpPool<PostgresAsync> {
        &mut self.pool
    }
}

impl EpRouter for PostgresEp {
    fn as_router(self: Box<Self>) -> Box<dyn EpRouter> {
        self
    }
    fn as_any(&self) -> &dyn std::any::Any {
        self
    }
    fn any_mut(&mut self) -> &mut dyn std::any::Any {
        self
    }
}

/// Pinned connection for transaction affinity (BEGIN..COMMIT on same connection).
///
/// Uses a raw wire protocol connection from the deadpool pool. When dropped,
/// the connection is returned to the pool (unlike the old bb8 dedicated_connection
/// which was discarded).
pub type PgPinnedConnection = deadpool::managed::Object<PgConnectionManager>;

impl PostgresEp {
    fn build_raw_pools(endpoint_cache_uuid: &EndpointCacheUuid, config: &PostgresConfig) -> RawPgPools {
        let downcast_pg = |conn: Option<Box<dyn EpConnection>>| -> Option<PostgresConnection> {
            conn.and_then(|c| c.as_any().downcast_ref::<PostgresConnection>().cloned())
        };
        let ep_uuid = endpoint_cache_uuid.uuid().to_string();
        let org_uuid = endpoint_cache_uuid
            .org()
            .map(|org| org.eden_uuid::<OrganizationUuid>().to_string())
            .unwrap_or_else(|| telemetry::labels::SYSTEM_ORG_UUID.to_string());
        let read = downcast_pg(config.read_conn())
            .and_then(|c| PostgresConfig::build_raw_pool_for_endpoint(&c, org_uuid.clone(), Some(ep_uuid.clone())).ok());
        let write = downcast_pg(config.write_conn())
            .and_then(|c| PostgresConfig::build_raw_pool_for_endpoint(&c, org_uuid.clone(), Some(ep_uuid.clone())).ok());
        let admin = downcast_pg(config.admin_conn())
            .and_then(|c| PostgresConfig::build_raw_pool_for_endpoint(&c, org_uuid.clone(), Some(ep_uuid.clone())).ok());
        let system = downcast_pg(config.system_conn())
            .and_then(|c| PostgresConfig::build_raw_pool_for_endpoint(&c, org_uuid.clone(), Some(ep_uuid.clone())).ok());

        // Spawn a lazy poller that samples pool.status() every 5s to emit
        // in-use (checked out) vs open counts without touching the hot path.
        let poll_interval = std::time::Duration::from_secs(5);
        let mut pool_status_pollers = Vec::new();
        for pool in [&read, &write, &admin, &system].into_iter().flatten() {
            let pool = pool.clone();
            let poller =
                telemetry::spawn_pool_status_poller("postgres", org_uuid.clone(), Some(ep_uuid.clone()), poll_interval, move || {
                    let s = pool.status();
                    Some((s.size, s.available.max(0)))
                });
            pool_status_pollers.push(poller);
        }

        RawPgPools {
            read,
            write,
            admin,
            system,
            _pool_status_pollers: pool_status_pollers,
        }
    }

    /// Register raw wire protocol pools for an endpoint.
    ///
    /// Called only after the main endpoint pool has been initialized.
    /// The raw pools are used by the proxy for extended query protocol and
    /// pinned transaction connections.
    pub fn register_raw_pools(&self, endpoint_cache_uuid: &EndpointCacheUuid, config: &PostgresConfig) {
        self.raw_pools.insert(endpoint_cache_uuid.clone(), Self::build_raw_pools(endpoint_cache_uuid, config));
    }

    /// Get a pinned write connection for transaction affinity (raw wire protocol).
    ///
    /// Returns a deadpool Object that is held for the transaction duration.
    /// When dropped, the connection is returned to the pool.
    pub async fn pinned_write_connection(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<PgPinnedConnection> {
        let pools = self.raw_pools.get(endpoint_cache_uuid).ok_or_else(|| EpError::connect("No raw pool registered for endpoint"))?;
        let pool = pools
            .write
            .as_ref()
            .or(pools.read.as_ref())
            .ok_or_else(|| EpError::connect("No write pool available for pinned connection"))?;
        pool.get().await.map_err(|e| EpError::connect(format!("Failed to get pinned connection: {e}")))
    }

    /// Get a raw wire protocol connection for extended query protocol.
    ///
    /// Uses the read or write pool based on request type.
    pub async fn raw_connection(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        req_type: ep_core::ReqType,
    ) -> ResultEP<PgPinnedConnection> {
        let pools = self.raw_pools.get(endpoint_cache_uuid).ok_or_else(|| EpError::connect("No raw pool registered for endpoint"))?;
        let pool = match req_type {
            ep_core::ReqType::Read => pools.read.as_ref().or(pools.write.as_ref()),
            ep_core::ReqType::Write => pools.write.as_ref().or(pools.read.as_ref()),
        }
        .ok_or_else(|| EpError::connect("No pool available for raw connection"))?;
        pool.get().await.map_err(|e| EpError::connect(format!("Failed to get raw connection: {e}")))
    }
}

ep_core::impl_endpoint_lifecycle_spec!(
    PostgresEp,
    PostgresAsync,
    PostgresConfig,
    PostgresRequest,
    PostgresMetadata,
    PostgresApi,
    PostgresTx
);

impl EP<PostgresAsync, PostgresConfig, PostgresRequest, PostgresMetadata, PostgresApi, PostgresTx> for PostgresEp {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::default()
    }

    /// Override connect_async to also build raw wire protocol pools.
    ///
    /// The raw pools are used by the proxy for extended query protocol and
    /// pinned transaction connections, which require wire-level access.
    #[named]
    async fn connect_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<PoolType<PostgresAsync>>> {
        use std::borrow::Cow;

        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pg_config = match config.as_any().downcast_ref::<PostgresConfig>() {
            Some(c) => c.clone(),
            None => {
                let error = "failed to downcast config";
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
                return Err(EpError::connect(error));
            }
        };

        // Pin the endpoint UUID onto telemetry labels so the API-path pool
        // (`PostgresConfig::conn_async`) picks it up for per-endpoint tracking.
        let endpoint_uuid: format::EndpointUuid = endpoint_cache_uuid.eden_uuid();
        telemetry_wrapper.mut_labels(|labels| labels.set_endpoint_uuid(endpoint_uuid.clone()));

        let conn_set = pg_config.init_conn_async(telemetry_wrapper).await?;
        let raw_pools = Self::build_raw_pools(endpoint_cache_uuid, &pg_config);
        let previous = self.mut_pool().connect_async(endpoint_cache_uuid, conn_set).await;
        self.raw_pools.insert(endpoint_cache_uuid.clone(), raw_pools);

        Ok(previous)
    }

    #[named]
    async fn disconnect_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Option<PoolType<PostgresAsync>>> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));
        let previous = self.mut_pool().disconnect_async(endpoint_cache_uuid).await;
        self.raw_pools.remove(endpoint_cache_uuid);
        Ok(previous)
    }

    #[named]
    async fn reconnect_async(
        &mut self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        config: Box<dyn EpConfig>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        use std::borrow::Cow;

        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        span.add_event(format!("attempting to reconnect async connection to {}", Self::kind()), vec![]);

        let pg_config = match config.as_any().downcast_ref::<PostgresConfig>() {
            Some(c) => c.clone(),
            None => {
                let error = "failed to downcast config";
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
                return Err(EpError::connect(error));
            }
        };

        span.add_event(format!("downcast connection to {}-config", Self::kind()), vec![]);

        let mut candidate = Self::new();
        candidate.connect_async(endpoint_cache_uuid, pg_config.as_config(), telemetry_wrapper).await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let health_result = candidate.health_check(endpoint_cache_uuid, telemetry_wrapper).await;
        let disconnect_result = candidate.disconnect_async(endpoint_cache_uuid, telemetry_wrapper).await;

        if let Err(disconnect_error) = disconnect_result {
            span.add_event(
                "failed to disconnect temporary reconnect validation connection",
                vec![telemetry::FastSpanAttribute::new("error", disconnect_error.to_string())],
            );

            if health_result.is_ok() {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(disconnect_error.to_string()) });
                return Err(disconnect_error);
            }
        }

        if let Err(e) = health_result {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            return Err(e);
        }

        self.connect_async(endpoint_cache_uuid, pg_config.as_config(), telemetry_wrapper).await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        span.add_simple_event("reconnected successfully");

        Ok(())
    }

    #[named]
    async fn transaction(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        outbound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let tx = match transaction.as_any().downcast_ref::<Transaction<PostgresRequest>>() {
            Some(tx) => tx,
            None => return Err(EpError::Transaction(TransactionError::FailedToDowncast)),
        };

        span.add_simple_event("processing transaction");

        // Get a raw wire protocol connection for the transaction
        let mut client = self.pool().write_conn_async(endpoint_cache_uuid).await?.get().await.map_err(EpError::request)?;

        // BEGIN the transaction using raw wire protocol
        client.batch_execute("BEGIN").await.map_err(EpError::transaction)?;

        span.add_simple_event("preparing transaction");

        let mut output = vec![];
        for req in &tx.0 {
            output.push(match req.as_any().downcast_ref::<PostgresRequest>() {
                Some(request) => {
                    let operation = &*request.0;

                    match operation.kind() {
                        PostgresApi::BatchExecute => match operation.as_any().downcast_ref::<BatchExecuteInput>() {
                            Some(op) => {
                                client.batch_execute(op.query()).await.map_err(EpError::transaction)?;
                                Box::new(EmptyOutput(()).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::CancelToken => Box::new(CancelTokenAsyncOutput.to_output()) as Box<dyn EpOutput>,
                        PostgresApi::ClearTypeCache => {
                            return Err(EpError::transaction("clear_type_cache cannot run in a transaction"));
                        }
                        PostgresApi::CopyIn => match operation.as_any().downcast_ref::<CopyInInput>() {
                            Some(op) => {
                                client.copy_in(op.query(), op.value().as_bytes()).await.map_err(EpError::transaction)?;
                                Box::new(CopyInWriterOutput::from(0u64).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::CopyOut => match operation.as_any().downcast_ref::<CopyOutInput>() {
                            Some(op) => {
                                let buf = client.copy_out(op.query()).await.map_err(EpError::transaction)?;
                                Box::new(CopyOutReaderOutput::new(buf).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::Execute => match operation.as_any().downcast_ref::<ExecuteInput>() {
                            Some(op) => {
                                let text_params: Vec<Option<String>> = op.params().iter().map(|p| p.to_pg_text()).collect();
                                let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
                                let raw = client.query_params_raw(op.query(), &param_refs).await.map_err(EpError::transaction)?;
                                let result = extract_command_complete_count(&raw);
                                Box::new(U64Output(result).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::IsClosed => {
                            return Err(EpError::transaction("is_closed cannot run in a transaction"));
                        }
                        PostgresApi::IsValid => {
                            return Err(EpError::transaction("is_valid cannot run in a transaction"));
                        }
                        PostgresApi::Notifications => {
                            return Err(EpError::transaction("notifications cannot run in a transaction"));
                        }
                        PostgresApi::Prepare => {
                            return Err(EpError::Transaction(TransactionError::PrepareCannotRunInTransaction));
                        }
                        PostgresApi::PrepareTyped => {
                            return Err(EpError::transaction("prepare_typed cannot run in a transaction"));
                        }
                        PostgresApi::Query => match operation.as_any().downcast_ref::<QueryInput>() {
                            Some(op) => {
                                let text_params: Vec<Option<String>> = op.params().iter().map(|p| p.to_pg_text()).collect();
                                let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
                                let raw = client.query_params_raw(op.query(), &param_refs).await.map_err(EpError::transaction)?;
                                Box::new(PostgresRowsOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::QueryReadOnly => match operation.as_any().downcast_ref::<QueryReadOnlyInput>() {
                            Some(op) => {
                                let text_params: Vec<Option<String>> = op.params().iter().map(|p| p.to_pg_text()).collect();
                                let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
                                let raw = client.query_params_raw(op.query(), &param_refs).await.map_err(EpError::transaction)?;
                                Box::new(PostgresRowsOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::QueryOne => match operation.as_any().downcast_ref::<QueryOneInput>() {
                            Some(op) => {
                                let text_params: Vec<Option<String>> = op.params().iter().map(|p| p.to_pg_text()).collect();
                                let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
                                let raw = client.query_params_raw(op.query(), &param_refs).await.map_err(EpError::transaction)?;
                                Box::new(PostgresRowOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::QueryOneReadOnly => match operation.as_any().downcast_ref::<QueryOneReadOnlyInput>() {
                            Some(op) => {
                                let text_params: Vec<Option<String>> = op.params().iter().map(|p| p.to_pg_text()).collect();
                                let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
                                let raw = client.query_params_raw(op.query(), &param_refs).await.map_err(EpError::transaction)?;
                                Box::new(PostgresRowOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::QueryOpt => match operation.as_any().downcast_ref::<QueryOneInput>() {
                            Some(op) => {
                                let text_params: Vec<Option<String>> = op.params().iter().map(|p| p.to_pg_text()).collect();
                                let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
                                let raw = client.query_params_raw(op.query(), &param_refs).await.map_err(EpError::transaction)?;
                                Box::new(PostgresOptionRowOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::QueryOptReadOnly => match operation.as_any().downcast_ref::<QueryOptReadOnlyInput>() {
                            Some(op) => {
                                let text_params: Vec<Option<String>> = op.params().iter().map(|p| p.to_pg_text()).collect();
                                let param_refs: Vec<Option<&str>> = text_params.iter().map(|o| o.as_deref()).collect();
                                let raw = client.query_params_raw(op.query(), &param_refs).await.map_err(EpError::transaction)?;
                                Box::new(PostgresOptionRowOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::QueryRaw => {
                            return Err(EpError::transaction("query_raw cannot run in a transaction"));
                        }
                        PostgresApi::QueryRawReadOnly => {
                            return Err(EpError::transaction("query_raw_read_only cannot run in a transaction"));
                        }
                        PostgresApi::QueryTyped => {
                            return Err(EpError::transaction("query_typed cannot run in a transaction"));
                        }
                        PostgresApi::QueryTypedReadOnly => {
                            return Err(EpError::transaction("query_typed_read_only cannot run in a transaction"));
                        }
                        PostgresApi::QueryTypedRaw => {
                            return Err(EpError::transaction("query_typed_raw cannot run in a transaction"));
                        }
                        PostgresApi::SimpleQuery => match operation.as_any().downcast_ref::<SimpleQueryInput>() {
                            Some(op) => {
                                let raw = client.simple_query_raw(op.query()).await.map_err(EpError::transaction)?;
                                Box::new(PostgresSimpleQueryOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                        PostgresApi::SimpleQueryReadOnly => match operation.as_any().downcast_ref::<SimpleQueryReadOnlyInput>() {
                            Some(op) => {
                                let raw = client.simple_query_raw(op.query()).await.map_err(EpError::transaction)?;
                                Box::new(PostgresSimpleQueryOutput(raw).to_output()) as Box<dyn EpOutput>
                            }
                            None => {
                                return Err(EpError::transaction("failed to downcast operation"));
                            }
                        },
                    }
                    .try_serde_serialize()
                }
                None => Err(EpError::Connect(ConnectError::FailedToDowncastRequest)),
            })
        }

        span.add_simple_event("sending outbound");
        // if results were collect Ok, send output

        let mut results: Vec<Value> = vec![];
        for res in output {
            match res {
                Err(e) => outbound.send(Err(e)).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?,
                Ok(str) => results.push(str),
            };
        }

        outbound.send(Ok(())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;

        span.add_simple_event("waiting for inbound");

        match inbound.await.map_err(EpError::transaction)? {
            true => {
                span.add_simple_event("commiting transaction");
                client.batch_execute("COMMIT").await.map_err(EpError::transaction)?;
                Ok(serde_json::to_value(&results).map_err(EpError::transaction)?)
            }
            false => {
                span.add_simple_event("rolling back transaction");
                client.batch_execute("ROLLBACK").await.map_err(EpError::transaction)?;
                Err(EpError::Transaction(TransactionError::Rollback))
            }
        }
    }
    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let mut conn = pool.get().await.map_err(EpError::connect)?;

        conn.batch_execute("SELECT 1").await
    }
    fn kind() -> EpKind {
        EpKind::Postgres
    }

    async fn process_wire_protocol(
        &self,
        _endpoint_cache_uuid: &EndpointCacheUuid,
        _receiver: &mut UnboundedReceiver<Vec<u8>>,
        _sender: UnboundedSender<Vec<u8>>,
        _settings: EdenSettings,
        _interlay_cache_uuid: InterlayCacheUuid,
        _interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        _telemetry_wrapper: &mut TelemetryWrapper,
        _ctx: LogContext,
    ) {
        //DO_NOTHING
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use endpoint_test_utils::telemetry_test_utils::test_telemetry;
    use format::{EndpointUuid, OrganizationCacheUuid, OrganizationUuid};
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use tokio::time::{Duration, Instant};

    fn endpoint_cache_uuid() -> EndpointCacheUuid {
        EndpointCacheUuid::new(Some(OrganizationCacheUuid::new(None, OrganizationUuid::new_uuid())), EndpointUuid::new_uuid())
    }

    fn postgres_config(read: bool, write: bool, admin: bool, system: bool) -> Box<PostgresConfig> {
        let connection = PostgresConnection {
            url: "postgres://postgres:postgres@localhost:5432/postgres".to_string(),
            sslmode: None,
        };
        let (target, creds) = connection.split().expect("split postgres connection");

        Box::new(PostgresConfig {
            target,
            read_credentials: read.then(|| creds.clone()),
            write_credentials: write.then(|| creds.clone()),
            admin_credentials: admin.then(|| creds.clone()),
            system_credentials: system.then_some(creds),
        })
    }

    fn raw_pool_presence(ep: &PostgresEp, endpoint_cache_uuid: &EndpointCacheUuid) -> Option<(bool, bool, bool, bool)> {
        ep.raw_pools
            .get(endpoint_cache_uuid)
            .map(|pools| (pools.read.is_some(), pools.write.is_some(), pools.admin.is_some(), pools.system.is_some()))
    }

    fn postgres_endpoint_in_use_count(endpoint_cache_uuid: &EndpointCacheUuid) -> Option<i64> {
        let endpoint_uuid = endpoint_cache_uuid.uuid().to_string();
        telemetry::connection_tracker::connection_state()
            .snapshot_endpoint_in_use()
            .into_iter()
            .find(|(db_type, uuid, _)| *db_type == "postgres" && uuid == &endpoint_uuid)
            .map(|(_, _, count)| count)
    }

    async fn wait_for_postgres_endpoint_in_use(endpoint_cache_uuid: &EndpointCacheUuid, expected: Option<i64>) {
        let deadline = Instant::now() + Duration::from_secs(1);
        loop {
            if postgres_endpoint_in_use_count(endpoint_cache_uuid) == expected {
                return;
            }

            assert!(Instant::now() < deadline, "timed out waiting for Postgres endpoint in-use count {expected:?}");
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    }

    fn synthetic_raw_pools_with_poller(endpoint_cache_uuid: &EndpointCacheUuid, poll_count: Arc<AtomicUsize>) -> RawPgPools {
        let endpoint_uuid = endpoint_cache_uuid.uuid().to_string();
        let org_uuid = endpoint_cache_uuid
            .org()
            .map(|org| org.eden_uuid::<OrganizationUuid>().to_string())
            .unwrap_or_else(|| telemetry::labels::SYSTEM_ORG_UUID.to_string());
        let poller = telemetry::spawn_pool_status_poller("postgres", org_uuid, Some(endpoint_uuid), Duration::from_millis(10), move || {
            poll_count.fetch_add(1, Ordering::Relaxed);
            Some((3, 0))
        });

        RawPgPools {
            read: None,
            write: None,
            admin: None,
            system: None,
            _pool_status_pollers: vec![poller],
        }
    }

    #[tokio::test]
    async fn failed_connect_async_leaves_no_raw_pool_entry() {
        let mut ep = PostgresEp::new();
        let endpoint_cache_uuid = endpoint_cache_uuid();
        let mut telemetry = test_telemetry();

        let result = ep.connect_async(&endpoint_cache_uuid, Box::new(PostgresConfig::default()), &mut telemetry).await;

        assert!(result.is_err());
        assert!(raw_pool_presence(&ep, &endpoint_cache_uuid).is_none());
        assert!(ep.pool().read_conn_async(&endpoint_cache_uuid).await.is_err());
    }

    #[tokio::test]
    async fn disconnect_async_removes_raw_pool_pollers() {
        let mut ep = PostgresEp::new();
        let endpoint_cache_uuid = endpoint_cache_uuid();
        let mut telemetry = test_telemetry();
        let poll_count = Arc::new(AtomicUsize::new(0));

        ep.raw_pools.insert(
            endpoint_cache_uuid.clone(),
            synthetic_raw_pools_with_poller(&endpoint_cache_uuid, poll_count.clone()),
        );
        wait_for_postgres_endpoint_in_use(&endpoint_cache_uuid, Some(3)).await;

        ep.disconnect_async(&endpoint_cache_uuid, &mut telemetry).await.expect("disconnect postgres pools");
        wait_for_postgres_endpoint_in_use(&endpoint_cache_uuid, None).await;

        tokio::time::sleep(Duration::from_millis(30)).await;
        let count_after_drop = poll_count.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(poll_count.load(Ordering::Relaxed), count_after_drop);
    }

    #[tokio::test]
    async fn disconnect_async_removes_main_and_raw_pool_entries() {
        let mut ep = PostgresEp::new();
        let endpoint_cache_uuid = endpoint_cache_uuid();
        let mut telemetry = test_telemetry();

        ep.connect_async(&endpoint_cache_uuid, postgres_config(true, true, false, false), &mut telemetry)
            .await
            .expect("connect postgres pools");

        assert_eq!(raw_pool_presence(&ep, &endpoint_cache_uuid), Some((true, true, false, false)));
        assert!(ep.pool().read_conn_async(&endpoint_cache_uuid).await.is_ok());

        ep.disconnect_async(&endpoint_cache_uuid, &mut telemetry).await.expect("disconnect postgres pools");

        assert!(raw_pool_presence(&ep, &endpoint_cache_uuid).is_none());
        assert!(ep.pool().read_conn_async(&endpoint_cache_uuid).await.is_err());
    }

    #[tokio::test]
    async fn repeated_connect_async_replaces_raw_pool_entry() {
        let mut ep = PostgresEp::new();
        let endpoint_cache_uuid = endpoint_cache_uuid();
        let mut telemetry = test_telemetry();

        ep.connect_async(&endpoint_cache_uuid, postgres_config(true, false, false, false), &mut telemetry)
            .await
            .expect("connect read pool");
        assert_eq!(raw_pool_presence(&ep, &endpoint_cache_uuid), Some((true, false, false, false)));

        ep.connect_async(&endpoint_cache_uuid, postgres_config(false, true, false, false), &mut telemetry)
            .await
            .expect("replace with write pool");

        assert_eq!(raw_pool_presence(&ep, &endpoint_cache_uuid), Some((false, true, false, false)));
    }

    #[tokio::test]
    async fn repeated_connect_async_replaces_raw_pool_pollers() {
        let mut ep = PostgresEp::new();
        let endpoint_cache_uuid = endpoint_cache_uuid();
        let mut telemetry = test_telemetry();
        let poll_count = Arc::new(AtomicUsize::new(0));

        ep.connect_async(&endpoint_cache_uuid, postgres_config(true, false, false, false), &mut telemetry)
            .await
            .expect("connect read pool");
        ep.raw_pools.insert(
            endpoint_cache_uuid.clone(),
            synthetic_raw_pools_with_poller(&endpoint_cache_uuid, poll_count.clone()),
        );
        wait_for_postgres_endpoint_in_use(&endpoint_cache_uuid, Some(3)).await;

        ep.connect_async(&endpoint_cache_uuid, postgres_config(false, true, true, false), &mut telemetry)
            .await
            .expect("replace with write/admin pools");
        wait_for_postgres_endpoint_in_use(&endpoint_cache_uuid, None).await;
        assert_eq!(raw_pool_presence(&ep, &endpoint_cache_uuid), Some((false, true, true, false)));

        tokio::time::sleep(Duration::from_millis(30)).await;
        let count_after_drop = poll_count.load(Ordering::Relaxed);
        tokio::time::sleep(Duration::from_millis(30)).await;
        assert_eq!(poll_count.load(Ordering::Relaxed), count_after_drop);
    }

    #[tokio::test]
    async fn reconnect_async_preserves_raw_pools_on_candidate_connect_failure() {
        let mut ep = PostgresEp::new();
        let endpoint_cache_uuid = endpoint_cache_uuid();
        let mut telemetry = test_telemetry();

        ep.connect_async(&endpoint_cache_uuid, postgres_config(true, false, false, false), &mut telemetry)
            .await
            .expect("connect read pool");
        assert_eq!(raw_pool_presence(&ep, &endpoint_cache_uuid), Some((true, false, false, false)));

        ep.connect_async(&endpoint_cache_uuid, postgres_config(false, true, true, false), &mut telemetry)
            .await
            .expect("replace with write/admin pools");
        assert_eq!(raw_pool_presence(&ep, &endpoint_cache_uuid), Some((false, true, true, false)));

        let result = ep.reconnect_async(&endpoint_cache_uuid, Box::new(PostgresConfig::default()), &mut telemetry).await;

        assert!(result.is_err());
        assert_eq!(raw_pool_presence(&ep, &endpoint_cache_uuid), Some((false, true, true, false)));
    }
}
