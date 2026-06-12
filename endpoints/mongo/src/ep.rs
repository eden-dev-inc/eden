use std::sync::Arc;

use super::request::MongoRequest;
use crate::api::lib::MongoApi;
use crate::metadata::MongoMetadata;
use crate::output::EmptyOutput;
use crate::{EP, EpTransaction, RunRequest, Transaction};
use dashmap::DashMap;
use eden_logger_internal::LogContext;
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use ep_core::settings::EdenSettings;
use ep_core::{GetPool, impl_endpoint};
use error::{EpError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use function_name::named;
use mongo_core::config::MongoConfig;
use mongo_core::{MongoAsync, MongoTx};
use mongodb::bson::doc;
use mongodb::options::{Acknowledgment, ReadConcern, TransactionOptions, WriteConcern};
use serde_json::Value;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;
use utoipa::openapi::{Object, ObjectBuilder, RefOr, Schema};
use utoipa::{PartialSchema, ToSchema};

impl_endpoint!(EpKind::Mongo => Mongo, MongoAsync);

ep_core::impl_endpoint_lifecycle_spec!(MongoEp, MongoAsync, MongoConfig, MongoRequest, MongoMetadata, MongoApi, MongoTx);

impl EP<MongoAsync, MongoConfig, MongoRequest, MongoMetadata, MongoApi, MongoTx> for MongoEp {
    fn new() -> Self {
        Self::default()
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

        let tx = match transaction.as_any().downcast_ref::<Transaction<MongoRequest>>() {
            Some(tx) => tx,
            None => return Err(EpError::Transaction(TransactionError::FailedToDowncast)),
        };

        span.add_simple_event("processing transaction");

        let conn = self.pool().write_conn_async(endpoint_cache_uuid).await?;

        let options = TransactionOptions::builder()
            .read_concern(ReadConcern::majority())
            .write_concern(WriteConcern::builder().w(Acknowledgment::Majority).build())
            .build();

        let session = &mut conn.get().await.map_err(EpError::transaction)?.start_session(None).await.map_err(EpError::transaction)?;

        session.start_transaction(options).await.map_err(|e| EpError::transaction(e.to_string()))?;

        span.add_simple_event("preparing transaction");

        let mut output = vec![];
        for req in &tx.0 {
            output.push(req.run_transaction(session, telemetry_wrapper)?);
            // output.push(match req.as_any().downcast_ref::<MongoRequest>() {
            //     Some(request) => {
            //         let client = self
            //             .pool()
            //             .write_conn_async(endpoint_cache_uuid, telemetry_wrapper.clone())
            //             .await?;
            //         request
            //             .run_async(client.to_owned(), settings, telemetry_wrapper.clone())
            //             .await?
            //             .try_serde_serialize()
            //     }
            //     None => Err(EpError::Connect(ConnectError::FailedToDowncastRequest)),
            // })
        }

        span.add_simple_event("sending outbound");
        // if results were collect Ok, send output

        // let mut results = vec![];
        // for res in output {
        //     match res {
        //         Err(e) => outbound
        //             .send(Err(e))
        //             .map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?,
        //         Ok(str) => results.push(str),
        //     };
        // }

        outbound.send(Ok(())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;

        // let res = outpound.send(Ok(())).unwrap_or_default();

        span.add_simple_event("waiting for inbound");
        // assert!(inbound.await.unwrap_or_default());
        match inbound.await.map_err(EpError::transaction)? {
            true => {
                span.add_simple_event("commiting transaction");
                serde_json::to_value(EmptyOutput(session.commit_transaction().await.map_err(EpError::transaction)?)).map_err(EpError::serde)
            }
            false => {
                span.add_simple_event("rolling back transaction");
                session.abort_transaction().await.map_err(|e| EpError::transaction(e.to_string()))?;
                Err(EpError::Transaction(TransactionError::Rollback))
            }
        }
    }

    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let conn = &mut pool.get().await.map_err(EpError::connect)?;

        match conn.database("admin").run_command(doc! { "ping": 1 }, None).await {
            Ok(_) => Ok(()),
            Err(err) => Err(EpError::request(err)),
        }
    }

    fn kind() -> EpKind {
        EpKind::Mongo
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

impl ToSchema for MongoEp {}
impl PartialSchema for MongoEp {
    fn schema() -> RefOr<Schema> {
        RefOr::T(Schema::Object(
            ObjectBuilder::new()
                .property("endpoint", String::schema())
                .property("kind", EpKind::schema())
                .property(
                    "config",
                    Schema::Object(
                        ObjectBuilder::new()
                            .property("auth", String::schema())
                            .property("read_conn", Object::default())
                            .property("write_conn", Object::default())
                            .property("content", String::schema())
                            .property("accept", String::schema())
                            .property("api_key", String::schema())
                            .build(),
                    ),
                )
                .required("endpoint_uuid")
                .required("request")
                .build(),
        ))
    }
}
