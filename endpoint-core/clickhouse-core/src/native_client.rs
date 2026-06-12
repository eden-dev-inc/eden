//! Native TCP client for ClickHouse protocol (port 9000).
//!
//! This client speaks the ClickHouse native binary protocol,
//! enabling low-latency communication and protocol proxying.

use crate::codec::{ClickhouseBuffer, ClickhouseStream};
use crate::connection::ClickhouseConnection;
use clickhouse_wire::native::ServerPacketType;
use clickhouse_wire::native::client::{ClientHello, Query};
use clickhouse_wire::native::server::{ServerException, ServerHello};
use clickhouse_wire::{ClickhouseReadSyncExt, ClickhouseWireError, DBMS_TCP_PROTOCOL_VERSION};
use eden_logger_internal::{ctx_with_trace, log_debug, log_trace};
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, IoError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;
use wire_stream::SliceStream;

/// Response from ClickHouse native protocol.
#[derive(Debug, Clone, PartialEq)]
pub struct ClickhouseResponse {
    /// Raw response bytes.
    pub bytes: bytes::Bytes,
    /// Number of packets received.
    pub packet_count: usize,
}

// Custom serialization for ClickhouseResponse since bytes::Bytes doesn't implement Serialize
impl Serialize for ClickhouseResponse {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut s = serializer.serialize_struct("ClickhouseResponse", 2)?;
        s.serialize_field("bytes", self.bytes.as_ref())?;
        s.serialize_field("packet_count", &self.packet_count)?;
        s.end()
    }
}

impl<'de> Deserialize<'de> for ClickhouseResponse {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        struct ClickhouseResponseHelper {
            bytes: Vec<u8>,
            packet_count: usize,
        }

        let helper = ClickhouseResponseHelper::deserialize(deserializer)?;
        Ok(Self {
            bytes: bytes::Bytes::from(helper.bytes),
            packet_count: helper.packet_count,
        })
    }
}

impl ClickhouseResponse {
    /// Create a new response.
    pub fn new(bytes: bytes::Bytes, packet_count: usize) -> Self {
        Self { bytes, packet_count }
    }

    /// Get the response bytes.
    pub fn to_vec(self) -> Vec<u8> {
        self.bytes.into()
    }

    /// Check if the response is empty.
    pub fn is_empty(&self) -> bool {
        self.bytes.is_empty()
    }
}

impl ToOutput for ClickhouseResponse {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Clickhouse, EndpointResponse::Response(self))
    }

    fn try_to_bytes(self) -> ResultEP<bytes::Bytes> {
        Ok(self.bytes)
    }

    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self).map_err(EpError::serde)
    }

    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented"))
    }
}

/// ClickHouse native protocol client.
pub struct ClickhouseNativeClient {
    config: ClickhouseConnection,
    stream: ClickhouseStream,
    buffer: ClickhouseBuffer,
    /// Server information from handshake.
    server_info: Option<ServerHello>,
    /// Protocol version negotiated.
    protocol_version: u64,
}

impl ClickhouseNativeClient {
    /// Create a new native client connection.
    #[named]
    pub async fn connect(config: &ClickhouseConnection) -> ResultEP<Self> {
        let stream = ClickhouseStream::new(config).await.map_err(|e| EpError::connect(format!("Failed to connect: {}", e)))?;

        let mut client = Self {
            config: config.clone(),
            stream,
            buffer: ClickhouseBuffer::new(),
            server_info: None,
            protocol_version: DBMS_TCP_PROTOCOL_VERSION,
        };

        // Perform handshake
        client.handshake().await?;

        Ok(client)
    }

    /// Check if the client is connected.
    pub async fn is_connected(&self) -> bool {
        self.stream.is_connected().await
    }

    /// Get server information from handshake.
    pub fn server_info(&self) -> Option<&ServerHello> {
        self.server_info.as_ref()
    }

    /// Get negotiated protocol version.
    pub fn protocol_version(&self) -> u64 {
        self.protocol_version
    }

    /// Perform the initial handshake with the server.
    #[named]
    async fn handshake(&mut self) -> ResultEP<()> {
        let _ctx = ctx_with_trace!().with_feature("clickhouse_core");
        log_debug!(_ctx, "Starting ClickHouse native handshake", audience = eden_logger_internal::LogAudience::Internal);

        // Build ClientHello
        let client_hello = ClientHello::with_client_info(
            "Eden",
            24,
            0,
            DBMS_TCP_PROTOCOL_VERSION,
            self.config.database.as_deref().unwrap_or("default"),
            self.config.user.as_deref().unwrap_or("default"),
            self.config.password.as_deref().unwrap_or(""),
        );

        // Encode and send
        let mut hello_buf = Vec::new();
        client_hello.encode(&mut hello_buf).map_err(|e| EpError::connect(format!("Failed to encode ClientHello: {}", e)))?;

        self.stream.write_all(&hello_buf).await.map_err(|e| EpError::connect(format!("Failed to send ClientHello: {}", e)))?;

        self.stream.flush().await.map_err(|e| EpError::connect(format!("Failed to flush: {}", e)))?;

        // Read response
        let mut temp_buf = [0u8; 65536];
        let n = self.stream.read(&mut temp_buf).await.map_err(|e| EpError::connect(format!("Failed to read response: {}", e)))?;

        if n == 0 {
            return Err(EpError::connect("Connection closed during handshake"));
        }

        self.buffer.append(&temp_buf[..n]);

        // Parse response packet type
        let stream = SliceStream::new(self.buffer.unprocessed());
        let packet_type = stream.read_varuint_sync().map_err(|e| EpError::connect(format!("Failed to read packet type: {}", e)))?;

        match ServerPacketType::from_u64(packet_type) {
            Some(ServerPacketType::Hello) => {
                let server_hello = ServerHello::parse_sync(&stream, self.protocol_version)
                    .map_err(|e| EpError::connect(format!("Failed to parse ServerHello: {}", e)))?;

                log_debug!(
                    _ctx,
                    "Connected to ClickHouse server",
                    audience = eden_logger_internal::LogAudience::Internal,
                    server_name = &server_hello.server_name,
                    version = server_hello.version_string()
                );

                // Use the server's protocol version if lower
                self.protocol_version = self.protocol_version.min(server_hello.protocol_version);
                self.server_info = Some(server_hello);
                self.buffer.clear();
                Ok(())
            }
            Some(ServerPacketType::Exception) => {
                let exception =
                    ServerException::parse_sync(&stream).map_err(|e| EpError::connect(format!("Failed to parse exception: {}", e)))?;

                Err(EpError::connect(format!("Server rejected connection: {}", exception.display_message())))
            }
            Some(other) => Err(EpError::connect(format!("Unexpected packet type during handshake: {:?}", other))),
            None => Err(EpError::connect(format!("Unknown packet type: {}", packet_type))),
        }
    }

    /// Send raw bytes and receive raw response.
    ///
    /// This is the core method for proxy scenarios where we want
    /// to forward packets without full parsing.
    #[named]
    pub async fn send_raw(&mut self, data: &[u8]) -> ResultEP<ClickhouseResponse> {
        if data.is_empty() {
            return Ok(ClickhouseResponse::new(bytes::Bytes::new(), 0));
        }

        let _t0 = Instant::now();
        let _ctx = ctx_with_trace!().with_feature("clickhouse_core");
        log_trace!(_ctx, "Sending raw data", audience = eden_logger_internal::LogAudience::Internal, bytes = data.len());

        // Send data
        if let Err(err) = self.stream.write_all(data).await {
            log::info!("Reconnecting after write failure: {err}");
            self.buffer.clear();
            self.stream = ClickhouseStream::new(&self.config).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;
            self.handshake().await?;
            self.stream.write_all(data).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;
        }

        self.stream.flush().await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;

        // Read response
        self.read_response().await
    }

    /// Execute a query and return raw response bytes.
    #[named]
    pub async fn query_raw(&mut self, sql: &str) -> ResultEP<ClickhouseResponse> {
        let _t0 = Instant::now();
        let _ctx = ctx_with_trace!().with_feature("clickhouse_core");
        log_trace!(_ctx, "Executing query", audience = eden_logger_internal::LogAudience::Internal, sql = sql);

        // Build Query packet
        let query = Query::with_id(uuid::Uuid::new_v4().to_string(), sql);

        let mut query_buf = Vec::new();
        query
            .encode(&mut query_buf, self.protocol_version)
            .map_err(|e| EpError::request(format!("Failed to encode query: {}", e)))?;

        // Send query
        self.stream.write_all(&query_buf).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;

        self.stream.flush().await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;

        // Read response
        let response = self.read_response().await?;

        log_trace!(
            _ctx,
            "Query completed",
            audience = eden_logger_internal::LogAudience::Internal,
            timing_micros = _t0.elapsed().as_micros(),
            response_bytes = response.bytes.len()
        );

        Ok(response)
    }

    /// Read response packets until EndOfStream or Exception.
    #[named]
    async fn read_response(&mut self) -> ResultEP<ClickhouseResponse> {
        const READ_BUF_LEN: usize = 65536;
        const MAX_EMPTY_READS: u32 = 30;

        let mut packet_count = 0;
        let mut empty_read_count = 0;
        let mut received_data = false;
        let t0 = Instant::now();

        let _ctx = ctx_with_trace!().with_feature("clickhouse_core");

        loop {
            // Read directly into BytesMut buffer - zero-copy
            let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::Io(IoError::Read(e.to_string())))?;

            if n > 0 {
                empty_read_count = 0;
                received_data = true;
            } else {
                empty_read_count += 1;
                if empty_read_count >= MAX_EMPTY_READS && self.buffer.is_empty() {
                    return Err(EpError::Io(IoError::Read(format!(
                        "Read timeout after {} attempts ({} ms)",
                        empty_read_count,
                        t0.elapsed().as_millis()
                    ))));
                }
            }

            // Process buffered data
            if !self.buffer.is_empty() {
                let stream = SliceStream::new(self.buffer.unprocessed());

                // Try to read packet type
                let packet_type_result = stream.read_varuint_sync();
                let packet_type = match packet_type_result {
                    Ok(pt) => pt,
                    Err(ClickhouseWireError::Stream(_)) => {
                        // Need more data
                        break;
                    }
                    Err(e) => {
                        return Err(EpError::request(format!("Failed to read packet type: {}", e)));
                    }
                };

                match ServerPacketType::from_u64(packet_type) {
                    Some(ServerPacketType::EndOfStream) => {
                        packet_count += 1;

                        log_trace!(
                            _ctx,
                            "EndOfStream received",
                            audience = eden_logger_internal::LogAudience::Internal,
                            packets = packet_count
                        );

                        // Zero-copy: drain buffer directly to Bytes
                        return Ok(ClickhouseResponse::new(self.buffer.drain_to_bytes(), packet_count));
                    }
                    Some(ServerPacketType::Exception) => {
                        // Try to parse exception for error message
                        let exception = ServerException::parse_sync(&stream).ok();
                        let error_msg = exception.map(|e| e.display_message()).unwrap_or_else(|| "Unknown server exception".to_string());

                        // Clear buffer on exception
                        self.buffer.clear();

                        return Err(EpError::request(error_msg));
                    }
                    Some(_) => {
                        // For proxy scenarios, we accumulate all packets
                        // Continue reading until EndOfStream
                        if n == 0 && self.buffer.len() < READ_BUF_LEN {
                            // No new data and buffer isn't full - might be complete
                            packet_count += 1;
                            // Zero-copy: drain buffer directly to Bytes
                            return Ok(ClickhouseResponse::new(self.buffer.drain_to_bytes(), packet_count));
                        }
                        break;
                    }
                    None => {
                        return Err(EpError::request(format!("Unknown packet type: {}", packet_type)));
                    }
                }
            }

            // If we got no new data and buffer is stable, return what we have
            if n == 0 && !self.buffer.is_empty() {
                // Zero-copy: drain buffer directly to Bytes
                return Ok(ClickhouseResponse::new(self.buffer.drain_to_bytes(), packet_count));
            }

            // If buffer is empty and no new data, we might be done
            if n == 0 && self.buffer.is_empty() && received_data {
                break;
            }
        }

        Ok(ClickhouseResponse::new(bytes::Bytes::new(), packet_count))
    }

    /// Reconnect to the server.
    #[named]
    pub async fn reconnect(&mut self) -> ResultEP<()> {
        self.buffer.clear();
        self.stream = ClickhouseStream::new(&self.config).await?;
        self.handshake().await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Note: Integration tests require a running ClickHouse server
    // These are placeholder tests for the data structures

    #[test]
    fn test_response_new() {
        let response = ClickhouseResponse::new(vec![1, 2, 3].into(), 1);
        assert_eq!(response.bytes, vec![1, 2, 3]);
        assert_eq!(response.packet_count, 1);
        assert!(!response.is_empty());
    }

    #[test]
    fn test_response_empty() {
        let response = ClickhouseResponse::new(vec![].into(), 0);
        assert!(response.is_empty());
    }

    #[test]
    fn test_response_to_bytes() {
        let response = ClickhouseResponse::new(vec![1, 2, 3].into(), 1);
        assert_eq!(response.to_vec(), vec![1, 2, 3]);
    }
}
