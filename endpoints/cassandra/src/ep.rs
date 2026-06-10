use cassandra_core::config::CassandraConfig;
use cassandra_core::{CassandraAsync, CassandraTx};
use dashmap::DashMap;
use eden_logger_internal::LogContext;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use function_name::named;
use scylla::response::PagingState;
use serde_json::Value;
use std::fmt::Debug;
use std::sync::Arc;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
/*
~~~~~ NEEDED FOR STREAMING ~~~~~
use futures_util::{pin_mut, TryStreamExt};
use std::str::FromStr;
*/

use crate::api::lib::CassandraApi;
use crate::metadata::CassandraMetadata;
use crate::request::CassandraRequest;
use endpoint_types::{Transaction, ep::EP, transaction::EpTransaction};
use ep_core::settings::EdenSettings;
use ep_core::{GetPool, impl_endpoint};
use error::{EpError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use telemetry::TelemetryWrapper;

impl_endpoint!(EpKind::Cassandra => Cassandra, CassandraAsync);

ep_core::impl_endpoint_lifecycle_spec!(
    CassandraEp,
    CassandraAsync,
    CassandraConfig,
    CassandraRequest,
    CassandraMetadata,
    CassandraApi,
    CassandraTx
);

impl EP<CassandraAsync, CassandraConfig, CassandraRequest, CassandraMetadata, CassandraApi, CassandraTx> for CassandraEp {
    fn new() -> Self
    where
        Self: Sized,
    {
        Self::default()
    }
    #[named]
    async fn transaction(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        _outbound: UnboundedSender<Result<(), EpError>>,
        _inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let _tx = match transaction.as_any().downcast_ref::<Transaction<CassandraRequest>>() {
            Some(tx) => tx,
            None => return Err(EpError::Transaction(TransactionError::FailedToDowncast)),
        };

        span.add_simple_event("processing transaction");

        let pool = self.pool().write_conn_async(endpoint_cache_uuid).await?;
        let _conn = &mut pool.get().await.map_err(EpError::connect)?;

        Err(EpError::Transaction(TransactionError::TransactionsNotImplemented))
        // todo!("session to start transaction");
        // let session = conn.start_transaction();
        //
        // span.add_simple_event("preparing transaction");
        //
        // let mut output = vec![];
        // for req in &tx.0 {
        //     output.push(match req.as_any().downcast_ref::<CassandraRequest>() {
        //         Some(request) => {
        //             let client = self.pool().write_conn(endpoint_uuid)?;
        //             request
        //                 .run_sync(client.to_owned(), settings, telemetry_wrapper)
        //                 .await?
        //                 .try_serde_serialize()
        //         }
        //         None => Err(EpError::Connect(ConnectError::FailedToDowncastRequest)),
        //     })
        // }
        //
        // span.add_simple_event("sending outbound");
        // // if results were collect Ok, send output
        //
        // let mut results = vec![];
        // for res in output {
        //     match res {
        //         Err(e) => outbound
        //             .send(Err(e))
        //             .map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?,
        //         Ok(str) => results.push(str),
        //     };
        // }
        //
        // outbound
        //     .send(Ok(()))
        //     .map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;
        //
        // // let res = outpound.send(Ok(())).unwrap_or_default();
        //
        // span.add_simple_event("waiting for inbound");
        // // assert!(inbound.await.unwrap_or_default());
        // match inbound.await.map_err(|e| EpError::transaction(e))? {
        //     true => {
        //         span.add_simple_event("commiting transaction");
        //         session
        //             .commit_transaction()
        //             .await
        //             .map_err(|e| EpError::transaction(e.to_string()))?;
        //         Ok(serde_json::to_value(&results).map_err(|e| EpError::transaction(e))?)
        //     }
        //     false => {
        //         span.add_simple_event("rolling back transaction");
        //         session
        //             .abort_transaction()
        //             .await
        //             .map_err(|e| EpError::transaction(e.to_string()))?;
        //         Err(EpError::Transaction(TransactionError::Rollback))
        //     }
        // }
    }
    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let conn = &mut pool.get().await.map_err(EpError::connect)?;

        match conn.query_single_page("SELECT now() FROM system.local;", &[], PagingState::default()).await.map_err(EpError::request) {
            Ok(_) => Ok(()),
            Err(err) => Err(EpError::request(err)),
        }
    }
    fn kind() -> EpKind {
        EpKind::Cassandra
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
