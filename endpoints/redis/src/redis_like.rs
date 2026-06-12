// Suppress async_fn_in_trait warning because RedisLikeEp is used internally.
#![allow(async_fn_in_trait)]

use std::borrow::Cow;
use std::sync::Arc;

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use eden_logger_internal::{LogAudience, LogContext, log_error, log_trace};
use endpoint_types::protocol::{EpProtocol, Method, MethodResponse};
use endpoint_types::request::EpWireRequest;
use endpoint_types::transaction::Transaction;
use endpoint_types::{EP, EpRequest, EpTransaction, RunRequest};
use ep_core::database::schema::interlay::InterlayState;
use ep_core::settings::EdenSettings;
use error::{EpError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use redis_core::config::RedisConfig;
use redis_core::{RedisAsync, RedisTx};
use serde::Serialize;
use serde_json::Value;
use telemetry::FastSpanStatus;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

use crate::api::RedisApi;
use crate::metadata::RedisMetadata;
use crate::protocol::RedisProtocol;

// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn process_wire_protocol_with_bytes<E, Req, K, B>(
    ep: &E,
    endpoint_cache_uuid: &EndpointCacheUuid,
    receiver: &mut UnboundedReceiver<Vec<u8>>,
    sender: UnboundedSender<Vec<u8>>,
    settings: EdenSettings,
    interlay_cache_uuid: InterlayCacheUuid,
    interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
    telemetry_wrapper: &mut TelemetryWrapper,
    ctx: LogContext,
    wire_label: &'static str,
) where
    E: EP<RedisAsync, RedisConfig, Req, RedisMetadata, K, RedisTx> + ?Sized,
    Req: EpRequest + ep_core::EndpointType + RunRequest<RedisAsync, K, RedisTx> + Serialize + 'static,
    K: 'static,
    B: EpWireRequest<RedisAsync> + From<Bytes>,
{
    let mut endpoint_cache_uuid = endpoint_cache_uuid.clone();

    let mut buffer = BytesMut::with_capacity(16 * 1024);
    let mut response_buffer = BytesMut::with_capacity(16 * 1024);

    while let Some(data) = receiver.recv().await {
        buffer.extend_from_slice(&data);

        loop {
            match RedisProtocol::validate_buffer(Method::default(), &buffer) {
                Ok(Some(response)) => {
                    let consumed = match &response {
                        MethodResponse::Simple { consumed, .. } => *consumed,
                        MethodResponse::Decode { consumed, .. } => *consumed,
                        MethodResponse::Parse { consumed, .. } => *consumed,
                    };
                    let command_bytes = buffer.split_to(consumed).freeze();

                    log_trace!(
                        ctx.clone(),
                        format!("Processing complete {} command", wire_label),
                        audience = LogAudience::Internal,
                        cache_key = endpoint_cache_uuid.to_string()
                    );

                    if let Some(interlay_state) = interlay_endpoints.get(&interlay_cache_uuid)
                        && interlay_state.endpoint_uuid() != &endpoint_cache_uuid
                    {
                        endpoint_cache_uuid = interlay_state.endpoint_uuid().to_owned();
                    }

                    match ep.raw_bytes(&endpoint_cache_uuid, B::from(command_bytes), settings, telemetry_wrapper).await {
                        Ok(resp) => {
                            response_buffer.extend_from_slice(&resp);
                        }
                        Err(e) => {
                            let err = format!("-ERR {}\r\n", e);
                            response_buffer.extend_from_slice(err.as_bytes());
                        }
                    }
                }
                Ok(None) => {
                    break;
                }
                Err(e) => {
                    log_error!(
                        ctx.clone(),
                        format!("{} protocol parse error", wire_label),
                        audience = LogAudience::Internal,
                        error = e.to_string()
                    );

                    let err = format!("-ERR {}\r\n", e);
                    response_buffer.extend_from_slice(err.as_bytes());
                    buffer.clear();

                    break;
                }
            }
        }

        if !response_buffer.is_empty() {
            if sender.send(response_buffer.to_vec()).is_err() {
                log_trace!(ctx, "Client disconnected, closing wire protocol processor", audience = LogAudience::Internal);
                return;
            }
            response_buffer.clear();
        }
    }

    log_trace!(ctx, "Wire protocol receiver closed, ending processor", audience = LogAudience::Internal);
}

/// Shared Redis-like endpoint logic for Redis and Elasticache.
pub trait RedisLikeEp: EP<RedisAsync, RedisConfig, Self::Request, RedisMetadata, RedisApi, RedisTx> {
    type Request: EpRequest + ep_core::EndpointType + RunRequest<RedisAsync, RedisApi, RedisTx> + Serialize + 'static;
    type WireBytes: EpWireRequest<RedisAsync> + From<Bytes>;

    const WIRE_LABEL: &'static str;

    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    #[allow(clippy::unit_arg)]
    async fn transaction_impl(
        &self,
        _endpoint_cache_uuid: &EndpointCacheUuid,
        outbound: UnboundedSender<Result<(), EpError>>,
        inbound: Receiver<bool>,
        transaction: &dyn EpTransaction,
        _settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
        span: &mut telemetry::FastSpan,
    ) -> Result<Value, EpError> {
        let tx = match transaction.as_any().downcast_ref::<Transaction<Self::Request>>() {
            Some(tx) => tx,
            None => {
                let error = "failed to downcast transaction";
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });

                return Err(EpError::transaction(error));
            }
        };

        span.add_simple_event("processing transaction");

        for req in &tx.0 {
            if let Err(err) = self.validate_operation(req.operation()) {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(err.to_string()) });

                outbound.send(Err(err.clone())).map_err(|e| {
                    span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
                    EpError::Transaction(TransactionError::ChannelFailure)
                })?;

                return Err(err);
            }
        }

        outbound.send(Ok(())).map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::Transaction(TransactionError::ChannelFailure)
        })?;

        span.add_simple_event("waiting for inbound");

        let confirmation = tokio::spawn(async { inbound.await.map_err(EpError::transaction) }).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::transaction(e)
        })??;

        if !confirmation {
            span.add_simple_event("rolling back transaction");
            return Err(EpError::Transaction(TransactionError::Rollback));
        }

        span.add_simple_event("commiting transaction");

        let mut pipeline = RedisTx::new();
        pipeline.atomic();

        for req in &tx.0 {
            req.run_transaction(&mut pipeline, telemetry_wrapper).inspect_err(|e| {
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            })?;
        }

        let pool = self.pool().write_conn_async(_endpoint_cache_uuid).await?;
        let mut conn = pool.get().await.map_err(EpError::parse_redis_error)?;
        let (response, _latency) = conn.send_command_raw(&pipeline.get_packed_pipeline()).await.map_err(|e| {
            let ep_error = EpError::parse_redis_error(e);
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(ep_error.to_string()) });
            ep_error
        })?;

        let bytes = response.to_bytes();
        let mut offset = 0;
        let mut exec_result = None;
        while offset < bytes.len() {
            let (frame, consumed) = RedisProtocol::decode_buffer(&bytes[offset..]).ok_or_else(|| {
                let error = EpError::parse("incomplete Redis transaction response");
                span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
                error
            })?;
            exec_result = Some(frame);
            offset += consumed;
        }

        let exec_result = exec_result.ok_or_else(|| {
            let error = EpError::transaction("empty Redis transaction response");
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(error.to_string()) });
            error
        })?;

        let value = crate::api::RedisJsonValue::try_from(exec_result).inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        Ok(value.into())
    }

    async fn health_check_impl(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        _telemetry_wrapper: &mut TelemetryWrapper,
        _span: &mut telemetry::FastSpan,
    ) -> Result<(), EpError> {
        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let conn = &mut pool.get().await.map_err(EpError::parse_redis_error)?;

        match conn.send_command_raw(b"*1\r\n$4\r\nPING\r\n").await {
            Ok((resp_bytes, _latency)) => {
                let bytes = resp_bytes.to_bytes();
                if bytes == Bytes::from_static(b"+PONG\r\n") {
                    Ok(())
                } else {
                    Err(EpError::request(format!("Unexpected PING response: {:?}", String::from_utf8_lossy(&bytes))))
                }
            }
            Err(err) => Err(EpError::parse_redis_error(err)),
        }
    }

    // TODO: Refactor parameters into a request/context struct to reduce argument count.
    #[allow(clippy::too_many_arguments)]
    async fn process_wire_protocol_impl(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        receiver: &mut UnboundedReceiver<Vec<u8>>,
        sender: UnboundedSender<Vec<u8>>,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: &mut TelemetryWrapper,
        ctx: LogContext,
    ) {
        process_wire_protocol_with_bytes::<Self, Self::Request, RedisApi, Self::WireBytes>(
            self,
            endpoint_cache_uuid,
            receiver,
            sender,
            settings,
            interlay_cache_uuid,
            interlay_endpoints,
            telemetry_wrapper,
            ctx,
            Self::WIRE_LABEL,
        )
        .await;
    }
}
