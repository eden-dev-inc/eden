use std::collections::HashSet;
use std::sync::Arc;

use database::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::endpoint_schema::endpoint::EndpointSchema;
use eden_core::auth::ParsedJwt;
use eden_core::error::{ConnectError, EpError, ResultEP, TransactionError};
use eden_core::format::endpoint::EpKind;
use eden_core::format::{CacheObjectType, CacheUuid, EndpointId, EndpointUuid, OrganizationCacheUuid};
use eden_core::format::{cache_id::EndpointCacheId, cache_uuid::EndpointCacheUuid};
use eden_core::telemetry::{FastSpanAttribute, TelemetryWrapper};
use endpoint::EpTransaction;
use ep_core::ep::EpConfig;
use ep_core::settings::EdenSettings;
use function_name::named;
use futures::stream::{FuturesUnordered, StreamExt};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use tokio::sync::mpsc;
use tokio::sync::{mpsc::UnboundedSender, oneshot, oneshot::Receiver};

use crate::comp::MyEngineService;

#[derive(Debug, borsh::BorshDeserialize, borsh::BorshSerialize, Serialize, Deserialize)]
pub struct MultiTransactionInfo {
    pub txs: Vec<TransactionInfo>,
}

#[allow(dead_code)]
impl MultiTransactionInfo {
    fn new(txs: Vec<TransactionInfo>) -> Self {
        Self { txs }
    }

    fn endpoints(&self) -> Vec<&EndpointUuid> {
        self.txs.iter().map(|t| t.endpoint_uuid()).collect()
    }

    fn transactions(&self) -> Vec<&dyn EpTransaction> {
        self.txs.iter().map(|t| t.tx()).collect()
    }

    fn kinds(&self) -> Vec<EpKind> {
        self.txs.iter().map(|t| t.kind()).collect::<HashSet<_>>().into_iter().collect()
    }

    fn len(&self) -> usize {
        self.txs.len()
    }
}

#[derive(Debug, borsh::BorshDeserialize, borsh::BorshSerialize, Serialize, Deserialize)]
pub struct TransactionInfo {
    endpoint_uuid: EndpointUuid,
    tx: Box<dyn EpTransaction>,
}

impl TransactionInfo {
    pub fn endpoint_uuid(&self) -> &EndpointUuid {
        &self.endpoint_uuid
    }

    pub fn kind(&self) -> EpKind {
        self.tx.kind()
    }

    pub fn tx(&self) -> &dyn EpTransaction {
        &*self.tx
    }

    pub fn tx_as_type<T: EpTransaction + 'static>(&self) -> ResultEP<&T> {
        match self.tx.as_any().downcast_ref::<T>() {
            Some(t) => Ok(t),
            None => Err(EpError::Transaction(TransactionError::FailedToDowncast)),
        }
    }
}

impl MyEngineService {
    #[named]
    pub async fn transaction_with_reconnect(
        &self,
        database_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_cache_uuid: &EndpointCacheUuid,
        config_override: Option<Box<dyn EpConfig>>,
        transaction: &dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());

        if let Some(config) = config_override.as_ref() {
            span.add_simple_event("connecting temporary ELS transaction pool");
            let mut lock = self.router.write().await;
            let ep = lock.get_mut(&transaction.kind()).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;
            ep.connect_boxed(endpoint_cache_uuid, config.clone(), telemetry_wrapper).await?;
        }

        let result = async {
            span.add_simple_event("getting router");

            let lock = self.router.read().await;
            span.add_simple_event("getting endpoint");
            let ep = lock.get(&transaction.kind()).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

            span.add_simple_event("building channels");
            let (phase1_result_tx, mut phase1_result_rx) = mpsc::unbounded_channel::<Result<(), EpError>>();
            let (phase2_commit_tx, phase2_commit_rx) = oneshot::channel();

            span.add_simple_event("running channels");
            tokio::spawn(async move {
                match phase1_result_rx.recv().await {
                    Some(res) => match res {
                        Ok(_) => phase2_commit_tx.send(true).map_err(EpError::transaction),
                        Err(_) => phase2_commit_tx.send(false).map_err(EpError::transaction),
                    },
                    None => Err(EpError::Transaction(TransactionError::NothingReceived)),
                }
            });

            span.add_simple_event("running transaction");
            if ep.test_write_conn_boxed(endpoint_cache_uuid, settings, telemetry_wrapper).await.is_err() {
                span.add_simple_event("failed to connect to endpoint");
                drop(lock);

                let mut lock = self.router.write().await;
                let ep = lock.get_mut(&transaction.kind()).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

                let reconnect_config = match config_override.as_ref() {
                    Some(config) => config.clone(),
                    None => <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                        EndpointSchema,
                        EndpointCacheUuid,
                        EndpointUuid,
                        EndpointCacheId,
                        EndpointId,
                    >>::get_from_cache(
                        database_manager, &CacheObjectType::new(Some(endpoint_cache_uuid.clone()), None), telemetry_wrapper
                    )
                    .await?
                    .config(),
                };
                ep.reconnect_boxed(endpoint_cache_uuid, reconnect_config, telemetry_wrapper).await?;

                span.add_simple_event("reconnected! sending transaction");
                drop(lock);

                let lock = self.router.read().await;
                let ep = lock.get(&transaction.kind()).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;
                ep.transaction_boxed(endpoint_cache_uuid, phase1_result_tx, phase2_commit_rx, transaction, settings, telemetry_wrapper)
                    .await
            } else {
                ep.transaction_boxed(endpoint_cache_uuid, phase1_result_tx, phase2_commit_rx, transaction, settings, telemetry_wrapper)
                    .await
            }
        }
        .await;

        if config_override.is_some() {
            span.add_simple_event("disconnecting temporary ELS transaction pool");
            let mut lock = self.router.write().await;
            let ep = lock.get_mut(&transaction.kind()).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;
            let _ = ep.disconnect_boxed(endpoint_cache_uuid, telemetry_wrapper).await;
        }

        result
    }

    #[allow(clippy::too_many_arguments)]
    #[named]
    pub async fn two_phase_transaction_with_reconnect(
        &self,
        org_cache_key: OrganizationCacheUuid,
        database_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        transaction: TransactionInfo,
        phase1_result_tx: UnboundedSender<Result<(), EpError>>,
        phase2_commit_rx: Receiver<bool>,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        let endpoint_cache_uuid = EndpointCacheUuid::new(Some(org_cache_key), transaction.endpoint_uuid().clone());
        let kind = transaction.kind();

        let lock = self.router.read().await;
        let ep = lock.get(&kind).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

        span.add_simple_event("running transaction");
        if ep.test_write_conn_boxed(&endpoint_cache_uuid, settings, telemetry_wrapper).await.is_err() {
            span.add_simple_event("failed to connect to endpoint");
            drop(lock);

            let mut lock = self.router.write().await;
            let ep = lock.get_mut(&kind).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;

            let reconnect_config = <DatabaseManager<RedisConn, PgConn, ClickhouseConn> as CacheFunctions<
                EndpointSchema,
                EndpointCacheUuid,
                EndpointUuid,
                EndpointCacheId,
                EndpointId,
            >>::get_from_cache(
                database_manager, &CacheObjectType::new(Some(endpoint_cache_uuid.clone()), None), telemetry_wrapper
            )
            .await?
            .config();
            ep.reconnect_boxed(&endpoint_cache_uuid, reconnect_config, telemetry_wrapper).await?;

            span.add_simple_event("reconnected! sending transaction");
            drop(lock);

            let lock = self.router.read().await;
            let ep = lock.get(&kind).ok_or(EpError::Connect(ConnectError::CouldNotGetEndpoint))?;
            ep.transaction_boxed(
                &endpoint_cache_uuid,
                phase1_result_tx,
                phase2_commit_rx,
                transaction.tx(),
                settings,
                telemetry_wrapper,
            )
            .await
        } else {
            ep.transaction_boxed(
                &endpoint_cache_uuid,
                phase1_result_tx,
                phase2_commit_rx,
                transaction.tx(),
                settings,
                telemetry_wrapper,
            )
            .await
        }
    }
}

impl MyEngineService {
    pub async fn transaction(
        &self,
        _org_cache_key: OrganizationCacheUuid,
        database_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_cache_uuid: EndpointCacheUuid,
        transaction: &mut dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        self.transaction_els(database_manager, endpoint_cache_uuid, None, transaction, settings, telemetry_wrapper).await
    }

    #[named]
    pub async fn transaction_els(
        &self,
        database_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        endpoint_cache_uuid: EndpointCacheUuid,
        config_override: Option<Box<dyn EpConfig>>,
        transaction: &mut dyn EpTransaction,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        span.add_simple_event("processing transaction...");

        self.transaction_with_reconnect(database_manager, &endpoint_cache_uuid, config_override, transaction, settings, telemetry_wrapper)
            .await
    }

    #[named]
    pub async fn two_phase_transactions(
        &self,
        org_cache_key: OrganizationCacheUuid,
        auth: ParsedJwt,
        database_manager: Arc<DatabaseManager<RedisConn, PgConn, ClickhouseConn>>,
        multi_transaction_info: MultiTransactionInfo,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<String, EpError> {
        let mut span = telemetry_wrapper.server_tracer(function_name!().to_string());
        span.add_simple_event("processing transaction...");

        let (phase1_result_tx, mut phase1_result_rx) = mpsc::unbounded_channel::<Result<(), EpError>>();
        let len = multi_transaction_info.len();

        span.add_event("tx info", vec![FastSpanAttribute::new("len", len.to_string())]);

        let mut sender = vec![];
        let mut receiver = vec![];
        for _ in 0..len {
            let (s, r) = oneshot::channel();
            sender.push(s);
            receiver.push(r);
        }

        span.add_simple_event("created channels");

        tokio::spawn(async move {
            let mut responses = vec![];
            while responses.len() < len {
                responses.push(match phase1_result_rx.recv().await {
                    Some(res) => res,
                    None => continue,
                })
            }

            if responses.iter().filter(|r| r.is_ok()).count() == len {
                for s in sender {
                    s.send(true).map_err(EpError::transaction)?;
                }
                Ok(())
            } else {
                for s in sender {
                    s.send(false).map_err(EpError::transaction)?;
                }
                Err(EpError::Transaction(TransactionError::FailedToCollectApprovals))
            }
        });

        span.add_simple_event("spawn transaction");

        let tasks = multi_transaction_info
            .txs
            .into_iter()
            .map(|tx| {
                let org_cache_key = org_cache_key.clone();
                let endpoint_uuid = tx.endpoint_uuid.clone();
                let mut telemetry_context = telemetry_wrapper.clone();
                let database_manager = database_manager.clone();
                let auth = auth.clone();
                let phase1_result_tx = phase1_result_tx.clone();
                let phase2_commit_rx = match receiver.pop() {
                    Some(r) => r,
                    None => {
                        let (_, r) = tokio::sync::oneshot::channel();
                        r
                    }
                };
                let service = self.clone();
                async move {
                    (
                        endpoint_uuid,
                        service
                            .ep_transaction_handle(
                                org_cache_key,
                                auth,
                                &database_manager,
                                tx,
                                phase1_result_tx,
                                phase2_commit_rx,
                                settings,
                                &mut telemetry_context,
                            )
                            .await,
                    )
                }
            })
            .collect::<FuturesUnordered<_>>();

        let result: Vec<_> = tasks.collect().await;
        span.add_simple_event("processing transaction result");

        let mut output = vec![];
        for res in result {
            let (endpoint_uuid, result) = res;
            output.push((endpoint_uuid, result.map_err(|e| e.to_string())))
        }

        serde_json::to_string(&output).map_err(EpError::transaction)
    }

    #[allow(clippy::too_many_arguments)]
    #[named]
    async fn ep_transaction_handle(
        &self,
        org_cache_key: OrganizationCacheUuid,
        _auth: ParsedJwt,
        database_manager: &DatabaseManager<RedisConn, PgConn, ClickhouseConn>,
        transaction_info: TransactionInfo,
        phase1_result_tx: UnboundedSender<Result<(), EpError>>,
        phase2_commit_rx: Receiver<bool>,
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let _span = telemetry_wrapper.server_tracer(function_name!().to_string());

        self.two_phase_transaction_with_reconnect(
            org_cache_key,
            database_manager,
            transaction_info,
            phase1_result_tx,
            phase2_commit_rx,
            settings,
            telemetry_wrapper,
        )
        .await
    }
}

#[cfg(all(test, feature = "infra-tests", external_db))]
mod tests {
    use tokio::sync::oneshot::{self, Receiver, Sender};

    async fn send(phase1_result_tx: Sender<i32>, phase2_commit_rx: Receiver<i32>) {
        phase1_result_tx.send(3).unwrap_or_default();
        println!("sending 3");
        let result = phase2_commit_rx.await.unwrap_or_default();
        assert_eq!(result, 6);
    }

    #[tokio::test]
    async fn channel() {
        let (phase1_result_tx, phase1_result_rx) = oneshot::channel();
        let (phase2_commit_tx, phase2_commit_rx) = oneshot::channel();

        tokio::spawn(async move { send(phase1_result_tx, phase2_commit_rx).await });

        tokio::spawn(async move {
            let result = phase1_result_rx.await.unwrap_or_default();
            assert_eq!(result, 3);
            phase2_commit_tx.send(6).unwrap_or_default();
            println!("sending 6");
        });
    }
}
