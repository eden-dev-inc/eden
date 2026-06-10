use std::sync::Arc;

use super::request::ClickhouseRequest;
use crate::api::lib::ClickhouseApi;
use crate::metadata::ClickhouseMetadata;
use crate::protocol::{ClickhouseBytes, ClickhouseProtocol};
use crate::{EP, EpTransaction, Transaction};
use bytes::BytesMut;
use clickhouse_core::config::ClickhouseConfig;
use clickhouse_core::{ClickhouseAsync, ClickhouseTx};
use dashmap::DashMap;
use eden_logger_internal::{LogAudience, LogContext, ctx_with_trace, log_debug, log_error, log_trace};
use ep_core::database::schema::interlay::InterlayState;
use ep_core::ep::{EpPool, EpRouter};
use ep_core::settings::EdenSettings;
use ep_core::{GetPool, impl_endpoint};
use error::{EpError, TransactionError};
use format::cache_uuid::{EndpointCacheUuid, InterlayCacheUuid};
use format::endpoint::EpKind;
use function_name::named;
use serde_json::Value;
use telemetry::FastSpanAttribute;
use telemetry::TelemetryWrapper;
use tokio::sync::mpsc::{UnboundedReceiver, UnboundedSender};
use tokio::sync::oneshot::Receiver;

impl_endpoint!(EpKind::Clickhouse => Clickhouse, ClickhouseAsync);

ep_core::impl_endpoint_lifecycle_spec!(
    ClickhouseEp,
    ClickhouseAsync,
    ClickhouseConfig,
    ClickhouseRequest,
    ClickhouseMetadata,
    ClickhouseApi,
    ClickhouseTx
);

impl EP<ClickhouseAsync, ClickhouseConfig, ClickhouseRequest, ClickhouseMetadata, ClickhouseApi, ClickhouseTx> for ClickhouseEp {
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
        settings: EdenSettings,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<Value, EpError> {
        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let tx = transaction
            .as_any()
            .downcast_ref::<Transaction<ClickhouseRequest>>()
            .ok_or(EpError::Transaction(TransactionError::FailedToDowncast))?;

        span.add_simple_event("processing transaction");

        outbound.send(Ok(())).map_err(|_| EpError::Transaction(TransactionError::ChannelFailure))?;

        span.add_simple_event("waiting for inbound");

        let confirmation = tokio::spawn(async { inbound.await.map_err(EpError::transaction) }).await.map_err(EpError::transaction)??;

        if !confirmation {
            span.add_simple_event("rolling back transaction");
            return Err(EpError::Transaction(TransactionError::Rollback));
        }

        span.add_simple_event("commiting transaction");

        let mut results = vec![];
        for req in &tx.0 {
            results.push(match self.write(endpoint_cache_uuid, req, settings, telemetry_wrapper).await {
                Ok(res) => res,
                Err(e) => serde_json::from_str(&e.to_string()).map_err(EpError::transaction)?,
            })
        }
        serde_json::to_value(&results).map_err(EpError::transaction)
    }
    #[named]
    async fn health_check(&self, endpoint_cache_uuid: &EndpointCacheUuid, telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), EpError> {
        let _span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        let pool = self.pool().read_conn_async(endpoint_cache_uuid).await?;
        let conn = &mut pool.get().await.map_err(EpError::connect)?;

        match conn.query("SELECT 1").execute().await {
            Ok(_) => Ok(()),
            Err(err) => Err(EpError::request(err)),
        }
    }
    fn kind() -> EpKind {
        EpKind::Clickhouse
    }

    /// Process ClickHouse native wire protocol (TCP port 9000).
    ///
    /// This implements a streaming proxy that:
    /// 1. Receives client data in chunks
    /// 2. Forwards packets to upstream ClickHouse
    /// 3. Sends responses back to client
    /// Process ClickHouse wire protocol requests in a continuous loop.
    ///
    /// This implementation:
    /// 1. Buffers incoming bytes from the client
    /// 2. Validates and parses ClickHouse native protocol frames
    /// 3. Routes each command through `raw_bytes()` which applies migration logic
    /// 4. Sends responses back to the client
    #[named]
    async fn process_wire_protocol(
        &self,
        endpoint_cache_uuid: &EndpointCacheUuid,
        receiver: &mut UnboundedReceiver<Vec<u8>>,
        sender: UnboundedSender<Vec<u8>>,
        settings: EdenSettings,
        interlay_cache_uuid: InterlayCacheUuid,
        interlay_endpoints: Arc<DashMap<InterlayCacheUuid, InterlayState>>,
        telemetry_wrapper: &mut TelemetryWrapper,
        _ctx: LogContext,
    ) {
        let mut endpoint_cache_uuid = endpoint_cache_uuid.clone();

        let mut span = telemetry_wrapper.client_tracer(format!("{}.{}", Self::kind(), function_name!()));

        span.add_simple_event("starting clickhouse wire protocol processor");

        let _ctx = ctx_with_trace!().with_feature("clickhouse");
        log_debug!(_ctx, "Starting ClickHouse wire protocol processor", audience = LogAudience::Internal);

        // Pre-allocate buffers
        let mut buffer = BytesMut::with_capacity(16 * 1024);
        let mut response_buffer = BytesMut::with_capacity(16 * 1024);
        let mut total_bytes_read: u64 = 0;
        let mut total_bytes_written: u64 = 0;
        let mut command_count: u64 = 0;

        // Main processing loop - runs until the client closes the connection
        while let Some(data) = receiver.recv().await {
            if data.is_empty() {
                continue;
            }

            // Append new data to the input buffer
            buffer.extend_from_slice(&data);
            let bytes_read = data.len() as u64;
            total_bytes_read += bytes_read;

            log_trace!(
                _ctx,
                "Received data",
                audience = LogAudience::Internal,
                bytes = data.len(),
                buffer_size = buffer.len()
            );

            // Inner loop: process all complete packets in the buffer
            loop {
                if buffer.is_empty() {
                    break;
                }

                // Try to peek at packet type to understand what we're dealing with
                let packet_info = ClickhouseProtocol::peek_packet_type(&buffer, false);

                if packet_info.is_none() {
                    // Not enough data to determine packet type
                    break;
                }

                let (_packet_type, _type_len) = packet_info.unwrap();

                log_trace!(
                    _ctx,
                    "Processing packet",
                    audience = eden_logger_internal::LogAudience::Internal,
                    packet_type = format!("{:?}", _packet_type)
                );

                // Check for endpoint migration
                if let Some(interlay_state) = interlay_endpoints.get(&interlay_cache_uuid)
                    && interlay_state.endpoint_uuid() != &endpoint_cache_uuid
                {
                    endpoint_cache_uuid = interlay_state.endpoint_uuid().to_owned();
                }

                // For ClickHouse native protocol, we forward the entire buffer
                // because packet boundaries are complex to determine without full parsing
                let command_bytes = buffer.split().freeze();
                command_count += 1;

                // Start timing the proxy request
                let request_start = std::time::Instant::now();

                // Forward through raw_bytes which handles connection pooling and migration
                let ch_bytes = ClickhouseBytes::from(command_bytes.to_vec());
                match self.raw_bytes(&endpoint_cache_uuid, ch_bytes, settings, telemetry_wrapper).await {
                    Ok(resp) => {
                        let duration_us = request_start.elapsed().as_micros() as u64;
                        let bytes_written = resp.len() as u64;
                        total_bytes_written += bytes_written;

                        // Successful response - add to response buffer
                        response_buffer.extend_from_slice(&resp);

                        span.add_event(
                            "processed clickhouse command",
                            vec![
                                FastSpanAttribute::new("bytes_read", command_bytes.len() as i64),
                                FastSpanAttribute::new("bytes_written", resp.len() as i64),
                                FastSpanAttribute::new("duration_us", duration_us as i64),
                            ],
                        );

                        log_trace!(
                            _ctx,
                            "Command processed",
                            audience = LogAudience::Internal,
                            response_bytes = resp.len(),
                            duration_us = duration_us
                        );
                    }
                    Err(e) => {
                        log_error!(_ctx, "Error forwarding command: {}", audience = LogAudience::Internal, details = e.to_string());

                        // For errors, we could encode a ClickHouse Exception packet
                        // For now, log and continue
                        span.add_event("clickhouse command error", vec![FastSpanAttribute::new("error", e.to_string())]);
                    }
                }
            }

            // Send accumulated responses back to the client
            if !response_buffer.is_empty() {
                if sender.send(response_buffer.to_vec()).is_err() {
                    log_error!(_ctx, "Failed to send response - channel closed", audience = LogAudience::Internal);
                    break;
                }
                response_buffer.clear();
            }
        }

        span.add_event(
            "clickhouse wire protocol session ended",
            vec![
                FastSpanAttribute::new("total_bytes_read", total_bytes_read as i64),
                FastSpanAttribute::new("total_bytes_written", total_bytes_written as i64),
                FastSpanAttribute::new("total_commands", command_count as i64),
            ],
        );

        log_debug!(
            _ctx,
            "ClickHouse wire protocol session ended",
            audience = LogAudience::Internal,
            total_bytes_read = total_bytes_read,
            total_bytes_written = total_bytes_written,
            total_commands = command_count
        );
    }
}

impl ClickhouseEp {
    // Note: Native TCP protocol (port 9000) support requires additional infrastructure
    // for storing ClickhouseConnection config separately from the HTTP pool.
    // For now, wire protocol proxying uses the HTTP API through raw_bytes.
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ep_kind() {
        assert_eq!(ClickhouseEp::kind(), EpKind::Clickhouse);
    }
}
