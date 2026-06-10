use crate::codec::{RedisBuffer, RedisStream, RedisStreamReader, RedisStreamWriter};
use crate::config::MultiKeyExecution;
use crate::connection::RedisConnection;
use bytes::{Bytes, BytesMut};
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_trace};
use ep_core::{EndpointOutput, EndpointResponse, ToOutput};
use error::{EpError, IoError, ProtocolError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use redis_protocol::resp2::decode::decode as decode_resp2;
use redis_protocol::resp3::decode::complete::decode as decode_resp3;
use redis_protocol::resp3::types::Resp3Frame;
use resp_wire::{PipelineError, PipelineExt, RespParseError, SliceReadError, SliceStream};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use std::time::Instant;

/// Maximum consecutive empty reads before declaring a read timeout.
/// Each empty read uses a 1-second poll interval (see `RedisStream::read_buf`),
/// so the effective total read timeout is MAX_EMPTY_READS seconds.
/// Set high enough to tolerate transient delays (pool checkout latency,
/// Docker networking jitter, tokio scheduler contention) without killing
/// healthy connections.
const MAX_EMPTY_READS: u32 = 5;

pub(crate) fn empty_read_budget() -> u32 {
    MAX_EMPTY_READS
}

/// Redis client that maintains a single TCP connection
pub struct RedisClient {
    config: RedisConnection,
    stream: RedisStream,
    buffer: RedisBuffer,
    protocol_version: u8,
    session_dirty: bool,
    multi_key_execution: MultiKeyExecution,
    /// Increments `eden.connections{db_type=redis}` on creation, decrements on drop.
    _conn_guard: telemetry::ConnectionGuard,
}

#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RespResponse(Vec<RespBytes>);

impl RespResponse {
    /// Convert to Bytes - zero-copy for single responses, concatenates for pipelines
    pub fn to_bytes(self) -> Bytes {
        if self.0.len() == 1 {
            // Single response - zero-copy
            self.0.into_iter().next().unwrap().into_bytes()
        } else {
            // Pipeline - need to concatenate
            let total_len: usize = self.0.iter().map(|r| r.len()).sum();
            let mut buf = Vec::with_capacity(total_len);
            for resp in self.0 {
                buf.extend_from_slice(resp.as_bytes());
            }
            Bytes::from(buf)
        }
    }

    pub fn commands(&self) -> usize {
        self.0.len()
    }

    pub fn is_resp3(&self) -> bool {
        matches!(self.0.last(), Some(RespBytes::Resp3(_)))
    }

    pub fn is_resp2(&self) -> bool {
        matches!(self.0.last(), Some(RespBytes::Resp2(_)))
    }
}

impl From<RespBytes> for RespResponse {
    fn from(value: RespBytes) -> Self {
        Self(vec![value])
    }
}

impl From<Vec<RespBytes>> for RespResponse {
    fn from(value: Vec<RespBytes>) -> Self {
        Self(value)
    }
}

impl ToOutput for RespResponse {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Redis, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<Bytes> {
        Ok(self.to_bytes())
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

#[derive(Debug, Clone, PartialEq)]
pub enum RespBytes {
    Resp2(Bytes),
    Resp3(Bytes),
}

#[allow(dead_code)]
impl RespBytes {
    /// Convert to owned Bytes - zero-copy (just Arc increment)
    pub fn into_bytes(self) -> Bytes {
        match self {
            RespBytes::Resp2(bytes) | RespBytes::Resp3(bytes) => bytes,
        }
    }

    /// Get a reference to the underlying bytes
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            RespBytes::Resp2(bytes) | RespBytes::Resp3(bytes) => bytes,
        }
    }

    /// Convert to Vec<u8> - for compatibility (allocates)
    pub fn to_vec(&self) -> Vec<u8> {
        self.as_bytes().to_vec()
    }

    fn resp2(bytes: impl Into<Bytes>) -> Self {
        Self::Resp2(bytes.into())
    }

    fn resp3(bytes: impl Into<Bytes>) -> Self {
        Self::Resp3(bytes.into())
    }

    pub fn len(&self) -> usize {
        match self {
            Self::Resp2(buffer) | Self::Resp3(buffer) => buffer.len(),
        }
    }

    pub fn is_empty(&self) -> bool {
        match self {
            Self::Resp2(buffer) | Self::Resp3(buffer) => buffer.is_empty(),
        }
    }
}

// Custom serialization for RespBytes since Bytes doesn't implement Serialize
impl Serialize for RespBytes {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStructVariant;
        match self {
            RespBytes::Resp2(bytes) => {
                let mut sv = serializer.serialize_struct_variant("RespBytes", 0, "Resp2", 1)?;
                sv.serialize_field("0", bytes.as_ref())?;
                sv.end()
            }
            RespBytes::Resp3(bytes) => {
                let mut sv = serializer.serialize_struct_variant("RespBytes", 1, "Resp3", 1)?;
                sv.serialize_field("0", bytes.as_ref())?;
                sv.end()
            }
        }
    }
}

impl<'de> Deserialize<'de> for RespBytes {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(Deserialize)]
        enum RespBytesHelper {
            Resp2(Vec<u8>),
            Resp3(Vec<u8>),
        }

        let helper = RespBytesHelper::deserialize(deserializer)?;
        Ok(match helper {
            RespBytesHelper::Resp2(v) => RespBytes::Resp2(Bytes::from(v)),
            RespBytesHelper::Resp3(v) => RespBytes::Resp3(Bytes::from(v)),
        })
    }
}

impl ToOutput for RespBytes {
    fn to_output(self) -> EndpointOutput<Self> {
        EndpointOutput::new(EpKind::Redis, EndpointResponse::Response(self))
    }
    fn try_to_bytes(self) -> ResultEP<Bytes> {
        Ok(self.into_bytes())
    }
    fn try_serde_serialize(&self) -> ResultEP<Value> {
        serde_json::to_value(self).map_err(EpError::serde)
    }
    fn try_borsh_serialize(&self) -> ResultEP<Vec<u8>> {
        Err(EpError::serde("borsh serialize not implemented for Database"))
    }
}

#[allow(unused_macros)] // #[named] generates function_name!() for future tracing use
impl RedisClient {
    pub fn count_pipeline_commands(buffer: &[u8]) -> ResultEP<usize> {
        Self::parse_pipeline_count(buffer)
    }

    /// Walk the leading complete RESP commands in `buffer` and return
    /// `(count, consumed_bytes)` for that prefix. The trailing bytes
    /// (after `consumed_bytes`) are an incomplete command-in-progress
    /// and the caller should read more from the wire before parsing
    /// again. Errors only on malformed framing — truncation is treated
    /// as "stop counting here" rather than a hard failure.
    ///
    /// Used by the direct-proxy lane-pool path to frame client batches
    /// without waiting for the full pipeline if a TCP read landed mid-
    /// command.
    pub fn parse_pipeline_prefix(buffer: &[u8]) -> ResultEP<(usize, usize)> {
        let mut count = 0usize;
        let mut pos = 0usize;

        'outer: while pos < buffer.len() {
            if buffer[pos] != b'*' {
                return Err(EpError::parse(format!("expected '*' at position {pos}, got '{}'", buffer[pos] as char)));
            }
            let cmd_start = pos;

            let nl = match memchr::memchr(b'\n', &buffer[pos..]) {
                Some(p) => pos + p + 1,
                None => break 'outer,
            };
            if nl < pos + 4 {
                return Err(EpError::parse(format!("malformed RESP array header at position {pos}")));
            }
            let argc: usize = std::str::from_utf8(&buffer[pos + 1..nl - 2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| EpError::parse(format!("invalid RESP array argc at position {pos}")))?;
            pos = nl;

            for _ in 0..argc {
                if pos >= buffer.len() {
                    pos = cmd_start;
                    break 'outer;
                }
                if buffer[pos] != b'$' {
                    return Err(EpError::parse(format!("expected '$' at position {pos}, got '{}'", buffer[pos] as char)));
                }
                let nl = match memchr::memchr(b'\n', &buffer[pos..]) {
                    Some(p) => pos + p + 1,
                    None => {
                        pos = cmd_start;
                        break 'outer;
                    }
                };
                if nl < pos + 4 {
                    return Err(EpError::parse(format!("malformed RESP bulk string header at position {pos}")));
                }
                let len: usize = std::str::from_utf8(&buffer[pos + 1..nl - 2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| EpError::parse(format!("invalid RESP bulk string length at position {pos}")))?;
                let end = nl + len + 2;
                if end > buffer.len() {
                    pos = cmd_start;
                    break 'outer;
                }
                pos = end;
            }
            count += 1;
        }

        Ok((count, pos))
    }

    /// Create a new Redis client connection
    #[named]
    pub async fn connect(config: &RedisConnection) -> ResultEP<Self> {
        Self::connect_with_org_endpoint_and_policy(config, telemetry::labels::SYSTEM_ORG_UUID, None, MultiKeyExecution::default()).await
    }

    /// Connect with an optional endpoint UUID for connection metrics labeling.
    #[named]
    pub async fn connect_with_endpoint(config: &RedisConnection, endpoint_uuid: Option<String>) -> ResultEP<Self> {
        Self::connect_with_org_endpoint_and_policy(config, telemetry::labels::SYSTEM_ORG_UUID, endpoint_uuid, MultiKeyExecution::default())
            .await
    }

    #[named]
    pub async fn connect_with_org_endpoint(
        config: &RedisConnection,
        org_uuid: impl Into<String>,
        endpoint_uuid: Option<String>,
    ) -> ResultEP<Self> {
        Self::connect_with_org_endpoint_and_policy(config, org_uuid, endpoint_uuid, MultiKeyExecution::default()).await
    }

    #[named]
    pub(crate) async fn connect_with_org_endpoint_and_policy(
        config: &RedisConnection,
        org_uuid: impl Into<String>,
        endpoint_uuid: Option<String>,
        multi_key_execution: MultiKeyExecution,
    ) -> ResultEP<Self> {
        let stream = RedisStream::new(config).await.map_err(|e| EpError::Io(IoError::Connect(e.to_string())))?;

        let mut client = Self {
            config: config.to_owned(),
            stream,
            buffer: RedisBuffer::new(),
            protocol_version: config.protocol_version(),
            session_dirty: false,
            multi_key_execution,
            _conn_guard: telemetry::ConnectionGuard::new_with_endpoint("redis", org_uuid, endpoint_uuid),
        };

        // Authenticate and select database
        client.initialize(config).await?;

        Ok(client)
    }

    pub fn multi_key_execution(&self) -> MultiKeyExecution {
        self.multi_key_execution
    }

    pub async fn is_connected(&self) -> bool {
        self.stream.is_connected().await
    }

    /// Consume this client and split its underlying stream into
    /// independent reader/writer halves. The writer task can pump
    /// commands to the wire while the reader task drains responses from
    /// the FIFO the writer feeds, so the two directions run
    /// concurrently rather than serialized behind a single `&mut self`.
    ///
    /// The connection-metrics guard moves with the writer half so that
    /// `eden.connections{db_type=redis}` decrements once when the worker
    /// drops both halves on disconnect, matching the un-split lifecycle.
    pub fn into_split(self) -> (RedisClientWriter, RedisClientReader) {
        let RedisClient {
            config,
            stream,
            buffer,
            protocol_version,
            session_dirty,
            _conn_guard,
            multi_key_execution: _,
        } = self;
        let (reader, writer) = stream.into_split();
        let writer_half = RedisClientWriter { config, writer, protocol_version, session_dirty, _conn_guard };
        let reader_half = RedisClientReader { reader, buffer, protocol_version };
        (writer_half, reader_half)
    }

    pub fn session_dirty(&self) -> bool {
        self.session_dirty
    }

    async fn reconnect_clean(&mut self) -> ResultEP<()> {
        self.buffer.clear();
        self.stream = RedisStream::new(&self.config).await.map_err(|e| EpError::Io(IoError::Connect(e.to_string())))?;
        let config_clone = self.config.clone();
        self.initialize(&config_clone).await?;
        self.session_dirty = false;
        Ok(())
    }

    pub async fn reset_session_state(&mut self) -> ResultEP<()> {
        if self.session_dirty {
            self.reconnect_clean().await?;
        }
        Ok(())
    }

    fn mark_session_dirty_if_needed(&mut self, command_bytes: &[u8]) {
        if let Some(command) = first_command_name(command_bytes)
            && matches!(
                command.as_str(),
                "AUTH"
                    | "HELLO"
                    | "SELECT"
                    | "SUBSCRIBE"
                    | "PSUBSCRIBE"
                    | "SSUBSCRIBE"
                    | "UNSUBSCRIBE"
                    | "PUNSUBSCRIBE"
                    | "SUNSUBSCRIBE"
                    | "WATCH"
                    | "UNWATCH"
                    | "MULTI"
                    | "EXEC"
                    | "DISCARD"
                    | "RESET"
            )
        {
            self.session_dirty = true;
        }
    }

    /// Initialize connection with AUTH and SELECT if needed
    async fn initialize(&mut self, config: &RedisConnection) -> ResultEP<()> {
        // Send HELLO command to set protocol version
        if self.protocol_version == 3 {
            let hello_cmd = if let Some(ref username) = config.username {
                if let Some(ref password) = config.password {
                    format!(
                        "*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        username.len(),
                        username,
                        password.len(),
                        password
                    )
                } else {
                    "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".to_string()
                }
            } else {
                // if only password was sent without username, use default
                if let Some(ref password) = config.password {
                    format!(
                        "*5\r\n$5\r\nHELLO\r\n$1\r\n3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n",
                        "default".len(),
                        "default",
                        password.len(),
                        password
                    )
                } else {
                    "*2\r\n$5\r\nHELLO\r\n$1\r\n3\r\n".to_string()
                }
            };

            self.stream
                .write_all(hello_cmd.as_bytes())
                .await
                .map_err(|e| EpError::connect(format!("Failed to send HELLO: {}", e)))?;
            self.stream.flush().await.map_err(|e| EpError::connect(format!("Failed to flush HELLO: {}", e)))?;

            // Read response
            let _ = self.read_response_raw(1).await?;
        } else {
            // RESP2: Use AUTH command if credentials provided
            if let Some(ref password) = config.password {
                let auth_cmd = if let Some(ref username) = config.username {
                    format!("*3\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n${}\r\n{}\r\n", username.len(), username, password.len(), password)
                } else {
                    format!("*2\r\n$4\r\nAUTH\r\n${}\r\n{}\r\n", password.len(), password)
                };

                self.send_command_raw_no_reconnect(auth_cmd.as_bytes()).await?;
            }
        }

        // Select database if specified
        if let Some(db) = config.db
            && db != 0
        {
            // Use itoa to avoid allocation from format!()
            let mut select_cmd = BytesMut::with_capacity(32);
            select_cmd.extend_from_slice(b"*2\r\n$6\r\nSELECT\r\n$");
            // Format db number and get its length
            let mut itoa_buf = itoa::Buffer::new();
            let db_str = itoa_buf.format(db);
            let db_len = db_str.len();
            // Need a second buffer for the length since db_str borrows itoa_buf
            let mut len_buf = itoa::Buffer::new();
            select_cmd.extend_from_slice(len_buf.format(db_len).as_bytes());
            select_cmd.extend_from_slice(b"\r\n");
            select_cmd.extend_from_slice(db_str.as_bytes());
            select_cmd.extend_from_slice(b"\r\n");
            self.send_command_raw_no_reconnect(&select_cmd).await?;
        }

        self.session_dirty = false;
        Ok(())
    }

    /// Send raw command bytes and read the response bytes
    /// Returns (response, network_latency_us) where network_latency_us measures only raw TCP I/O
    #[named]
    pub async fn send_command_raw(&mut self, command_bytes: &[u8]) -> ResultEP<(RespResponse, u64)> {
        if command_bytes.is_empty() {
            return Ok((RespResponse::from(RespBytes::Resp3(Bytes::new())), 0));
        }
        self.mark_session_dirty_if_needed(command_bytes);
        // Send command
        let _t0 = Instant::now();
        let _ctx = ctx_with_trace!().with_feature("redis_core");
        log_trace!(
            _ctx,
            "Send command raw",
            audience = LogAudience::Internal,
            command_hex = hex::encode(command_bytes),
            command_str = str::from_utf8(command_bytes).unwrap_or_default()
        );

        let commands = Self::parse_pipeline_count(command_bytes)?;

        // Start timing raw I/O (excludes logging, parsing overhead)
        let io_start = Instant::now();

        // Send bytes while parsing happens.
        // Explicit flush after write_all to ensure the full pipeline batch
        // reaches the upstream socket without lingering in kernel buffers.
        let _write_result = if let Err(err) = self.stream.write_all(command_bytes).await {
            log::info!("Reconnecting stream after write failure: {err}");
            // Reset local buffer so old partial frames don't corrupt the new connection
            self.reconnect_clean().await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;
            // Retry once; bubble up on failure
            self.stream.write_all(command_bytes).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;
            "reconnected"
        } else {
            "success"
        };
        self.stream.flush().await.map_err(|e| EpError::Io(IoError::Write(format!("flush: {e}"))))?;

        let _ctx = ctx_with_trace!().with_feature("redis_core");
        log_trace!(
            _ctx,
            "Send command raw - write: {} µs ({})",
            audience = LogAudience::Internal,
            details = format!("{}", _t0.elapsed().as_micros()),
            write_result = _write_result
        );

        // Read response
        let response = self.read_response_raw(commands).await;

        // Capture raw I/O latency (write + read, excludes all overhead)
        let network_latency_us = io_start.elapsed().as_micros() as u64;

        let _ctx = ctx_with_trace!().with_feature("redis_core");
        log_trace!(
            _ctx,
            "Send command raw - got response",
            audience = LogAudience::Internal,
            timing_micros = _t0.elapsed().as_micros()
        );

        response.map(|r| (r, network_latency_us))
    }

    /// Variant that avoids reconnect attempts; used inside initialization to prevent recursion.
    #[named]
    pub async fn send_command_raw_no_reconnect(&mut self, command_bytes: &[u8]) -> ResultEP<RespResponse> {
        if command_bytes.is_empty() {
            return Ok(RespResponse::from(RespBytes::Resp3(Bytes::new())));
        }
        self.mark_session_dirty_if_needed(command_bytes);

        let _ctx = ctx_with_trace!().with_feature("redis_core");

        let _t0 = Instant::now();
        let commands = Self::parse_pipeline_count(command_bytes)?;

        self.stream.write_all(command_bytes).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;

        let response = self.read_response_raw(commands).await;
        log_trace!(
            _ctx,
            "Send command raw (no reconnect) - got response",
            audience = LogAudience::Internal,
            timing_micros = _t0.elapsed().as_micros()
        );

        response
    }

    /// Count the number of RESP commands in a pipeline buffer by properly
    /// walking the RESP framing. The previous implementation scanned for
    /// `*` bytes, which miscounted when binary payload data contained
    /// byte sequences that looked like RESP array headers (e.g. `\r\n*3`
    /// inside a bulk string value).
    fn parse_pipeline_count(buffer: &[u8]) -> ResultEP<usize> {
        let mut count = 0;
        let mut pos = 0;

        while pos < buffer.len() {
            if buffer[pos] != b'*' {
                return Err(EpError::parse(format!("expected '*' at position {pos}, got '{}'", buffer[pos] as char)));
            }
            count += 1;

            // Skip past this command by walking the RESP array structure:
            // *N\r\n followed by N bulk strings ($len\r\ndata\r\n)
            let nl = match memchr::memchr(b'\n', &buffer[pos..]) {
                Some(p) => pos + p + 1,
                None => break,
            };
            // Need at least *N\r\n — nl points past \n, so the \r is at nl-2.
            // Minimum valid: pos=0, nl=4 for "*1\r\n". Guard against short lines.
            if nl < pos + 4 {
                return Err(EpError::parse(format!("malformed RESP array header at position {pos}")));
            }
            let argc: usize = std::str::from_utf8(&buffer[pos + 1..nl - 2])
                .ok()
                .and_then(|s| s.parse().ok())
                .ok_or_else(|| EpError::parse(format!("invalid RESP array argc at position {pos}")))?;
            pos = nl;

            for _ in 0..argc {
                if pos >= buffer.len() || buffer[pos] != b'$' {
                    return Err(EpError::parse(format!(
                        "expected '$' at position {pos}, got '{}'",
                        buffer.get(pos).map_or('?', |&b| b as char)
                    )));
                }
                let nl = match memchr::memchr(b'\n', &buffer[pos..]) {
                    Some(p) => pos + p + 1,
                    None => return Err(EpError::parse(format!("unterminated bulk string header at position {pos}"))),
                };
                if nl < pos + 4 {
                    return Err(EpError::parse(format!("malformed RESP bulk string header at position {pos}")));
                }
                let len: usize = std::str::from_utf8(&buffer[pos + 1..nl - 2])
                    .ok()
                    .and_then(|s| s.parse().ok())
                    .ok_or_else(|| EpError::parse(format!("invalid RESP bulk string length at position {pos}")))?;
                let end = nl + len + 2; // data + \r\n
                if end > buffer.len() {
                    return Err(EpError::parse(format!(
                        "truncated bulk string at position {pos}: declared {len} bytes but buffer ends at {}",
                        buffer.len()
                    )));
                }
                pos = end;
            }
        }

        if count == 0 {
            Err(EpError::parse("no commands found"))
        } else {
            Ok(count)
        }
    }

    /// Read a raw response from Redis as bytes
    async fn read_response_raw(&mut self, commands: usize) -> ResultEP<RespResponse> {
        if commands == 1 {
            self.read_single_response().await
        } else {
            self.read_pipeline_response(commands).await
        }
    }

    async fn read_pipeline_response(&mut self, commands: usize) -> ResultEP<RespResponse> {
        let mut responses = Vec::with_capacity(commands);
        let mut decoded_count = 0;
        let mut empty_read_count: u32 = 0;

        while decoded_count < commands {
            // Try decoding existing buffer first
            while decoded_count < commands {
                if self.protocol_version == 2 {
                    let decode_result = decode_resp2(self.buffer.unprocessed());
                    match decode_result {
                        Ok(Some((_, consumed))) => {
                            // Zero-copy: split_to_bytes returns Bytes without copying
                            responses.push(RespBytes::Resp2(self.buffer.split_to_bytes(consumed)));
                            decoded_count += 1;
                        }
                        Ok(None) => break, // Need more data
                        Err(e) => {
                            return Err(EpError::Protocol(ProtocolError::RESP2(e.to_string())));
                        }
                    }
                } else {
                    let decode_result = decode_resp3(self.buffer.unprocessed());
                    match decode_result {
                        Ok(Some((_, consumed))) => {
                            // Zero-copy: split_to_bytes returns Bytes without copying
                            responses.push(RespBytes::Resp3(self.buffer.split_to_bytes(consumed)));
                            decoded_count += 1;
                        }
                        Ok(None) => break, // Need more data
                        Err(e) => {
                            return Err(EpError::Protocol(ProtocolError::RESP3(e.to_string())));
                        }
                    }
                };
            }

            // If still need more responses, read directly into buffer (zero-copy)
            if decoded_count < commands {
                let n = self.stream.read_buf(self.buffer.buffer_mut()).await?;

                if n == 0 {
                    // Empty read (timeout) — the per-read tokio::select! in codec fired,
                    // not necessarily a closed connection. Retry up to MAX_EMPTY_READS times.
                    empty_read_count += 1;
                    if empty_read_count >= MAX_EMPTY_READS {
                        return Err(EpError::Io(IoError::Read(format!(
                            "Redis pipeline read timeout: received {}/{} responses after {} empty reads",
                            decoded_count, commands, empty_read_count
                        ))));
                    }
                } else {
                    empty_read_count = 0;
                }
            }
        }

        Ok(RespResponse::from(responses))
    }

    /// Read a raw response from Redis as bytes
    #[named]
    async fn read_single_response(&mut self) -> ResultEP<RespResponse> {
        let mut _count = 0; // Used in log_trace when log features enabled
        let mut empty_read_count: u32 = 0;
        let t0 = Instant::now();

        let _ctx = ctx_with_trace!().with_feature("redis_core");

        log_trace!(
            _ctx,
            "Read response raw",
            audience = LogAudience::Internal,
            count = _count,
            protocol_version = self.protocol_version,
            buffer_data = str::from_utf8(self.buffer.unprocessed()).unwrap_or_default()
        );

        loop {
            _count += 1;
            let _ctx = ctx_with_trace!().with_feature("redis_core");
            log_trace!(_ctx, "ReadResponseRaw: reading from stream {_count}...", audience = LogAudience::Internal);

            // Read directly into buffer (zero-copy)
            let _bytes_before = self.buffer.buffer_mut().len();
            let n = self.stream.read_buf(self.buffer.buffer_mut()).await.map_err(|e| EpError::Io(IoError::Read(e.to_string())))?;

            let _ctx = ctx_with_trace!().with_feature("redis_core");

            log_trace!(
                _ctx,
                "ReadResponseRaw: read from stream {} µs",
                audience = LogAudience::Internal,
                details = t0.elapsed().as_micros()
            );

            if n > 0 {
                // Reset empty read counter since we got data
                empty_read_count = 0;
                let _ctx = ctx_with_trace!().with_feature("redis_core");
                log_trace!(
                    _ctx,
                    "ReadResponseRaw: read bytes",
                    audience = LogAudience::Internal,
                    count = _count,
                    bytes_read = n,
                    timing_micros = t0.elapsed().as_micros(),
                    data = str::from_utf8(&self.buffer.unprocessed()[_bytes_before..]).unwrap_or_default()
                );

                let _ctx = ctx_with_trace!().with_feature("redis_core");
                log_trace!(
                    _ctx,
                    "ReadResponseRaw: buffer updated",
                    audience = LogAudience::Internal,
                    count = _count,
                    timing_micros = t0.elapsed().as_micros()
                );
            } else {
                // Empty read (timeout) - increment counter and check limit
                empty_read_count += 1;
                if empty_read_count >= MAX_EMPTY_READS {
                    let buffered = self.buffer.unprocessed().len();
                    self.buffer.clear();
                    if buffered == 0 {
                        return Err(EpError::Io(IoError::Read(format!(
                            "Redis read timeout: no response after {} attempts ({} ms)",
                            empty_read_count,
                            t0.elapsed().as_millis()
                        ))));
                    } else {
                        return Err(EpError::Io(IoError::Read(format!(
                            "Redis read timeout: incomplete frame after {} attempts ({} ms, {} bytes buffered)",
                            empty_read_count,
                            t0.elapsed().as_millis(),
                            buffered
                        ))));
                    }
                }
            }

            let _ctx = ctx_with_trace!().with_feature("redis_core");

            log_trace!(
                _ctx,
                "ReadResponseRaw: processing response buffer",
                audience = LogAudience::Internal,
                buffer_data = str::from_utf8(self.buffer.unprocessed()).unwrap_or_default(),
                timing_micros = t0.elapsed().as_micros()
            );

            // Try to parse existing buffer
            if self.protocol_version == 2 {
                match decode_resp2(self.buffer.unprocessed()) {
                    Ok(Some((_, consumed))) => {
                        // Zero-copy: split_to_bytes returns Bytes without copying
                        let frame_bytes = self.buffer.split_to_bytes(consumed);
                        return Ok(RespResponse::from(RespBytes::Resp2(frame_bytes)));
                    }
                    Ok(None) => {
                        // Need more data - continue to read
                    }
                    Err(e) => {
                        return Err(EpError::Protocol(ProtocolError::RESP2(e.to_string())));
                    }
                }
            } else {
                match decode_resp3(self.buffer.unprocessed()) {
                    Ok(Some((_, consumed))) => {
                        let _ctx = ctx_with_trace!().with_feature("redis_core");
                        log_trace!(
                            _ctx,
                            "ReadResponseRaw: consumed bytes",
                            audience = LogAudience::Internal,
                            bytes_consumed = consumed,
                            timing_micros = t0.elapsed().as_micros()
                        );

                        // Zero-copy: split_to_bytes returns Bytes without copying
                        let frame_bytes = self.buffer.split_to_bytes(consumed);
                        return Ok(RespResponse::from(RespBytes::Resp3(frame_bytes)));
                    }
                    Ok(None) => {
                        // Need more data - continue to read
                        let _ctx = ctx_with_trace!().with_feature("redis_core");
                        log_trace!(
                            _ctx,
                            "ReadResponseRaw: decode_resp3 == None, {} µs",
                            audience = LogAudience::Internal,
                            details = t0.elapsed().as_micros()
                        );
                        // If we've exhausted retries with an incomplete frame, return an error.
                        // Returning partial bytes would corrupt the upstream RESP stream.
                        if empty_read_count >= MAX_EMPTY_READS {
                            let buffered = self.buffer.unprocessed().len();
                            self.buffer.clear();
                            return Err(EpError::Io(IoError::Read(format!(
                                "Redis read timeout: incomplete RESP3 frame after {} empty reads ({} ms, {} bytes buffered)",
                                empty_read_count,
                                t0.elapsed().as_millis(),
                                buffered
                            ))));
                        }
                    }
                    Err(e) => {
                        let _ctx = ctx_with_trace!().with_feature("redis_core");
                        log_trace!(
                            _ctx,
                            "ReadResponseRaw: error decoding {e}, {} µs",
                            audience = LogAudience::Internal,
                            details = t0.elapsed().as_micros()
                        );

                        // Check if this is an incomplete frame error vs a parse error
                        let error_str = format!("{e:?}");
                        let is_invalid_hash_key = error_str.contains("Invalid hash key");

                        // Check if this is an incomplete frame error vs a parse error
                        if n > 0 {
                            let _ctx = ctx_with_trace!().with_feature("redis_core");
                            log_trace!(
                                _ctx,
                                "Incomplete RESP3 frame, trying to read more data from stream",
                                audience = LogAudience::Internal,
                                error = format!("{e}")
                            );
                            continue;
                        }

                        // For "Invalid hash key" errors (Sets containing Maps/Arrays),
                        // try fallback decoding by converting Sets to Arrays
                        if is_invalid_hash_key {
                            let _ctx = ctx_with_trace!().with_feature("redis_core");
                            log_trace!(
                                _ctx,
                                "Attempting fallback decode: converting RESP3 Sets to Arrays",
                                audience = LogAudience::Internal
                            );

                            let raw_bytes = self.buffer.unprocessed();
                            if let Some(converted) = convert_sets_to_arrays(raw_bytes) {
                                // Verify the converted bytes decode successfully
                                match decode_resp3(&converted) {
                                    Ok(Some((frame, consumed))) => {
                                        // Re-encode the frame to get clean output
                                        let len = frame.encode_len(false);
                                        let mut out = vec![0u8; len];
                                        if redis_protocol::resp3::encode::complete::encode(&mut out, &frame, false).is_ok() {
                                            self.buffer.clear();
                                            return Ok(RespResponse::from(RespBytes::Resp3(Bytes::from(out))));
                                        }
                                        // If re-encoding fails, use the converted bytes directly
                                        self.buffer.clear();
                                        return Ok(RespResponse::from(RespBytes::Resp3(Bytes::copy_from_slice(&converted[..consumed]))));
                                    }
                                    Ok(None) => {
                                        // Incomplete after conversion - need more data, continue reading
                                        continue;
                                    }
                                    Err(_fallback_err) => {
                                        // Fallback also failed, will return raw bytes below
                                    }
                                }
                            }
                        }

                        // Parse error with no new data and no successful fallback.
                        // Return an error instead of raw bytes to avoid corrupting the RESP stream.
                        let _ctx = ctx_with_trace!().with_feature("redis_core");
                        log_error!(_ctx, "Failed to decode RESP3 response: {e:?}", audience = LogAudience::Internal);
                        self.buffer.clear();
                        return Err(EpError::Protocol(ProtocolError::RESP3(format!("Failed to decode RESP3 response: {e:?}"))));
                    }
                }
            }

            // Need more data
            // let mut temp_buf = vec![0u8; 1024 * 1024];
            // let n = self
            //     .stream
            //     .read(&mut temp_buf)
            //     .await
            //     .map_err(|e| EpError::io(format!("Failed to read response: {}", e)))?;

            // if n == 0 {
            //     return Err(EpError::io("Connection closed by server"));
            // }

            // self.buffer.append(&temp_buf[..n]);
        }
    }
}

/// Writer half of a `RedisClient` after `into_split`. Owns the write side
/// of the underlying stream plus reconnect-relevant config; the multiplexer's
/// writer task drives this independently of the reader half so that a
/// command's TCP write does not block on the previous command's response.
pub struct RedisClientWriter {
    /// Retained so a future reconnect path on the writer side has the
    /// connection parameters available without round-tripping through the
    /// supervisor. Currently unused since the supervisor handles reconnect
    /// by dropping both halves and calling `RedisClient::connect_with_endpoint`.
    #[allow(dead_code)]
    config: RedisConnection,
    writer: RedisStreamWriter,
    protocol_version: u8,
    session_dirty: bool,
    /// Mirrors the `_conn_guard` on the un-split `RedisClient` — drops with
    /// the writer half on disconnect so `eden.connections` decrements once
    /// per session.
    _conn_guard: telemetry::ConnectionGuard,
}

impl RedisClientWriter {
    /// Write a command (or pipeline batch) to the wire without awaiting a
    /// response. The reader half is responsible for consuming the matching
    /// response(s) in FIFO order.
    pub async fn write_command_raw_no_response(&mut self, command_bytes: &[u8]) -> ResultEP<()> {
        if command_bytes.is_empty() {
            return Ok(());
        }

        self.mark_session_dirty_if_needed(command_bytes);
        self.write_command_raw_no_response_inner(command_bytes).await
    }

    /// Write bytes that the caller has already classified as stateless Redis
    /// traffic. The direct gateway lane-pool path performs that classification
    /// before dispatch, so re-sniffing the first command here would add work to
    /// every safe GET/SET batch without changing session-safety behavior.
    pub async fn write_command_raw_no_response_stateless(&mut self, command_bytes: &[u8]) -> ResultEP<()> {
        if command_bytes.is_empty() {
            return Ok(());
        }

        self.write_command_raw_no_response_inner(command_bytes).await
    }

    /// Write already-classified stateless Redis command chunks without first
    /// copying them into one contiguous buffer when the underlying stream
    /// supports vectored writes.
    pub async fn write_command_chunks_raw_no_response_stateless(&mut self, command_chunks: &[Bytes]) -> ResultEP<()> {
        match command_chunks {
            [] => Ok(()),
            [single] => self.write_command_raw_no_response_inner(single).await,
            chunks if self.writer.is_write_vectored() => {
                self.writer.write_all_vectored_chunks(chunks).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;
                self.writer.flush().await.map_err(|e| EpError::Io(IoError::Write(format!("flush: {e}"))))?;
                Ok(())
            }
            chunks => {
                let total_bytes: usize = chunks.iter().map(Bytes::len).sum();
                let mut combined = BytesMut::with_capacity(total_bytes);
                for chunk in chunks {
                    combined.extend_from_slice(chunk);
                }
                self.write_command_raw_no_response_inner(&combined).await
            }
        }
    }

    async fn write_command_raw_no_response_inner(&mut self, command_bytes: &[u8]) -> ResultEP<()> {
        self.writer.write_all(command_bytes).await.map_err(|e| EpError::Io(IoError::Write(e.to_string())))?;
        self.writer.flush().await.map_err(|e| EpError::Io(IoError::Write(format!("flush: {e}"))))?;
        Ok(())
    }

    pub fn session_dirty(&self) -> bool {
        self.session_dirty
    }

    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    fn mark_session_dirty_if_needed(&mut self, command_bytes: &[u8]) {
        if let Some(command) = first_command_name(command_bytes)
            && matches!(
                command.as_str(),
                "AUTH"
                    | "HELLO"
                    | "SELECT"
                    | "SUBSCRIBE"
                    | "PSUBSCRIBE"
                    | "SSUBSCRIBE"
                    | "UNSUBSCRIBE"
                    | "PUNSUBSCRIBE"
                    | "SUNSUBSCRIBE"
                    | "WATCH"
                    | "UNWATCH"
                    | "MULTI"
                    | "EXEC"
                    | "DISCARD"
                    | "RESET"
            )
        {
            self.session_dirty = true;
        }
    }
}

/// Reader half of a `RedisClient` after `into_split`. Owns the read side
/// of the stream and the framing buffer; the multiplexer's reader task
/// pops a pending FIFO entry, reads the matching number of RESP frames,
/// and fulfills the requestor's oneshot.
pub struct RedisClientReader {
    reader: RedisStreamReader,
    buffer: RedisBuffer,
    protocol_version: u8,
}

#[inline]
fn scan_response_frame_len(buffer: &[u8], protocol_version: u8) -> ResultEP<Option<usize>> {
    let stream = SliceStream::new(buffer);
    let mut pipeline = stream.pipeline();

    match pipeline.skip_len() {
        Ok(consumed) => Ok(consumed),
        Err(RespParseError::Stream(SliceReadError::NotEnoughData)) | Err(RespParseError::Parse(PipelineError::UnexpectedEnd)) => Ok(None),
        Err(err) => Err(redis_response_scan_error(protocol_version, err.to_string())),
    }
}

#[inline]
fn redis_response_scan_error(protocol_version: u8, error: String) -> EpError {
    if protocol_version == 2 {
        EpError::Protocol(ProtocolError::RESP2(error))
    } else {
        EpError::Protocol(ProtocolError::RESP3(error))
    }
}

impl RedisClientReader {
    pub fn protocol_version(&self) -> u8 {
        self.protocol_version
    }

    /// Drain whatever bytes are currently available on the wire into
    /// `buf` without parsing RESP frames. Used by the direct-proxy
    /// bridge mode that forwards raw bytes from backend to client
    /// without framing — the bridge doesn't care how many frames the
    /// kernel returned, only that the bytes get to the client TCP
    /// socket in arrival order. Returns `Err(UnexpectedEof)` on
    /// stream close (matches `read_buf_no_timeout` semantics on the
    /// underlying `RedisStreamReader`).
    pub async fn read_buf_raw(&mut self, buf: &mut bytes::BytesMut) -> std::io::Result<usize> {
        self.reader.read_buf_no_timeout(buf).await
    }

    /// Drain raw backend bytes into `buf`, appending at most `limit`
    /// bytes in this call.
    pub async fn read_buf_raw_limited(&mut self, buf: &mut bytes::BytesMut, limit: usize) -> std::io::Result<usize> {
        self.reader.read_buf_no_timeout_limited(buf, limit).await
    }

    /// Read `commands` RESP frames from the wire and return them as a
    /// single contiguous `Bytes`. For `commands <= 1` returns a
    /// zero-copy single-frame buffer; for pipeline batches uses a
    /// contiguous decode that avoids splitting the response into N owned
    /// chunks.
    pub async fn read_response_group_raw_bytes(&mut self, commands: usize) -> ResultEP<Bytes> {
        if commands <= 1 {
            return self.read_single_response_bytes().await;
        }

        self.read_response_group_contiguous_bytes(commands).await
    }

    /// Read one RESP response frame from the wire as raw bytes.
    ///
    /// This preserves the same buffering semantics as
    /// `read_response_group_raw_bytes(1)`, while giving proxy callers a
    /// smaller unit they can forward downstream before the full pipeline
    /// response group has been read.
    pub async fn read_response_frame_raw_bytes(&mut self) -> ResultEP<Bytes> {
        self.read_single_response_bytes().await
    }

    /// Read and discard `commands` RESP frames from the wire.
    ///
    /// Used by best-effort mirror drains where the caller only needs to
    /// preserve Redis response ordering and release in-flight accounting; the
    /// response payload itself is intentionally ignored.
    pub async fn discard_response_group(&mut self, commands: usize) -> ResultEP<()> {
        if commands <= 1 {
            self.discard_single_response().await
        } else {
            self.discard_response_group_contiguous(commands).await
        }
    }

    async fn read_single_response_bytes(&mut self) -> ResultEP<Bytes> {
        let mut empty_read_count: u32 = 0;
        let t0 = Instant::now();

        loop {
            // Decode anything already in the buffer before blocking on
            // a fresh socket read. Pipelined or back-to-back responses
            // are routinely coalesced into a previous read, so the
            // next frame may already be sitting in
            // `self.buffer.unprocessed()` — reading first would block
            // unnecessarily until the next packet arrives.
            if !self.buffer.unprocessed().is_empty() {
                match scan_response_frame_len(self.buffer.unprocessed(), self.protocol_version) {
                    Ok(Some(consumed)) => return Ok(self.buffer.split_to_bytes(consumed)),
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            }

            // Hot path: skip the 1-second timeout-poll select. EOF errors
            // propagate immediately (instead of returning 0), so the
            // empty_read_count branches below are effectively unreachable
            // under healthy traffic — kept as a safety net for any future
            // partial-frame edge case.
            let n =
                self.reader.read_buf_no_timeout(self.buffer.buffer_mut()).await.map_err(|e| EpError::Io(IoError::Read(e.to_string())))?;

            if n > 0 {
                empty_read_count = 0;
                continue;
            }

            empty_read_count += 1;
            if empty_read_count >= MAX_EMPTY_READS {
                let buffered = self.buffer.unprocessed().len();
                self.buffer.clear();
                return Err(EpError::Io(IoError::Read(format!(
                    "Redis read timeout: no response after {} attempts ({} ms, {} bytes buffered)",
                    empty_read_count,
                    t0.elapsed().as_millis(),
                    buffered
                ))));
            }
        }
    }

    async fn discard_single_response(&mut self) -> ResultEP<()> {
        let mut empty_read_count: u32 = 0;
        let t0 = Instant::now();

        loop {
            if !self.buffer.unprocessed().is_empty() {
                match scan_response_frame_len(self.buffer.unprocessed(), self.protocol_version) {
                    Ok(Some(consumed)) => {
                        self.buffer.consume(consumed);
                        return Ok(());
                    }
                    Ok(None) => {}
                    Err(e) => return Err(e),
                }
            }

            let n =
                self.reader.read_buf_no_timeout(self.buffer.buffer_mut()).await.map_err(|e| EpError::Io(IoError::Read(e.to_string())))?;

            if n > 0 {
                empty_read_count = 0;
                continue;
            }

            empty_read_count += 1;
            if empty_read_count >= MAX_EMPTY_READS {
                let buffered = self.buffer.unprocessed().len();
                self.buffer.clear();
                return Err(EpError::Io(IoError::Read(format!(
                    "Redis read timeout: no response after {} attempts ({} ms, {} bytes buffered)",
                    empty_read_count,
                    t0.elapsed().as_millis(),
                    buffered
                ))));
            }
        }
    }

    async fn read_response_group_contiguous_bytes(&mut self, commands: usize) -> ResultEP<Bytes> {
        let mut decoded_count = 0usize;
        let mut total_consumed = 0usize;
        let mut empty_read_count: u32 = 0;

        while decoded_count < commands {
            while decoded_count < commands {
                let buffered = &self.buffer.unprocessed()[total_consumed..];
                if buffered.is_empty() {
                    break;
                }

                match scan_response_frame_len(buffered, self.protocol_version)? {
                    Some(consumed) => {
                        total_consumed += consumed;
                        decoded_count += 1;
                    }
                    None => break,
                }
            }

            if decoded_count >= commands {
                return Ok(self.buffer.split_to_bytes(total_consumed));
            }

            // Hot path: skip the 1-second timeout-poll select.
            let n = self.reader.read_buf_no_timeout(self.buffer.buffer_mut()).await?;
            if n == 0 {
                empty_read_count += 1;
                if empty_read_count >= MAX_EMPTY_READS {
                    return Err(EpError::Io(IoError::Read(format!(
                        "Redis pipeline read timeout: received {}/{} responses after {} empty reads",
                        decoded_count, commands, empty_read_count
                    ))));
                }
            } else {
                empty_read_count = 0;
            }
        }

        Ok(self.buffer.split_to_bytes(total_consumed))
    }

    async fn discard_response_group_contiguous(&mut self, commands: usize) -> ResultEP<()> {
        let mut decoded_count = 0usize;
        let mut total_consumed = 0usize;
        let mut empty_read_count: u32 = 0;

        while decoded_count < commands {
            while decoded_count < commands {
                let buffered = &self.buffer.unprocessed()[total_consumed..];
                if buffered.is_empty() {
                    break;
                }

                match scan_response_frame_len(buffered, self.protocol_version)? {
                    Some(consumed) => {
                        total_consumed += consumed;
                        decoded_count += 1;
                    }
                    None => break,
                }
            }

            if decoded_count >= commands {
                self.buffer.consume(total_consumed);
                return Ok(());
            }

            let n = self.reader.read_buf_no_timeout(self.buffer.buffer_mut()).await?;
            if n == 0 {
                empty_read_count += 1;
                if empty_read_count >= MAX_EMPTY_READS {
                    return Err(EpError::Io(IoError::Read(format!(
                        "Redis pipeline read timeout: received {}/{} responses after {} empty reads",
                        decoded_count, commands, empty_read_count
                    ))));
                }
            } else {
                empty_read_count = 0;
            }
        }

        self.buffer.consume(total_consumed);
        Ok(())
    }
}

fn first_command_name(buffer: &[u8]) -> Option<String> {
    // This is a conservative RESP sniff for pooled-session tracking. We only
    // care about the common RESP array form; malformed or inline input returns
    // `None`, which avoids false-positive dirtying.
    if buffer.first().copied()? != b'*' {
        return None;
    }

    let array_end = memchr::memchr(b'\n', buffer)? + 1;
    if array_end < 4 || buffer.get(array_end)? != &b'$' {
        return None;
    }
    let bulk_end = array_end + memchr::memchr(b'\n', &buffer[array_end..])? + 1;
    if bulk_end < array_end + 4 {
        return None;
    }
    let len = std::str::from_utf8(&buffer[array_end + 1..bulk_end - 2]).ok()?.parse::<usize>().ok()?;
    let data_start = bulk_end;
    let data_end = data_start.checked_add(len)?;
    let command = std::str::from_utf8(buffer.get(data_start..data_end)?).ok()?;
    Some(command.to_ascii_uppercase())
}

/// Convert RESP3 Set markers (~) to Array markers (*) in raw bytes.
/// This is a workaround for redis-protocol library not supporting Sets
/// containing Maps/Arrays (which Redis returns for COMMAND INFO/DOCS).
///
/// The function scans for Set markers at the start of lines and replaces
/// them with Array markers, preserving the rest of the RESP3 structure.
fn convert_sets_to_arrays(input: &[u8]) -> Option<Vec<u8>> {
    // Quick check: if no set markers, return None to skip conversion
    if !input.contains(&b'~') {
        return None;
    }

    let mut output = Vec::with_capacity(input.len());
    let mut i = 0;

    while i < input.len() {
        // Set marker must be at start of a "line" (after \r\n or at position 0)
        let is_line_start = i == 0 || (i >= 2 && input[i - 2] == b'\r' && input[i - 1] == b'\n');

        if is_line_start && input[i] == b'~' {
            // Replace ~ with * (Set -> Array)
            output.push(b'*');
        } else {
            output.push(input[i]);
        }
        i += 1;
    }

    Some(output)
}

#[cfg(test)]
mod buffered_reader_tests {
    use super::*;
    use tokio::io::AsyncWriteExt;
    use tokio::time::{Duration, timeout};

    fn quiet_reader_with_buffer(buffered: &[u8]) -> (RedisClientReader, tokio::io::DuplexStream) {
        let (client_stream, server_stream) = tokio::io::duplex(64);
        let mut buffer = RedisBuffer::new();
        buffer.append(buffered);

        let reader = RedisClientReader {
            reader: RedisStreamReader::Duplex(client_stream),
            buffer,
            protocol_version: 2,
        };

        (reader, server_stream)
    }

    #[tokio::test]
    async fn single_response_drains_buffered_frames_before_reading_socket() {
        let (mut reader, _server_stream) = quiet_reader_with_buffer(b"+ONE\r\n+TWO\r\n");

        let first = reader.read_response_group_raw_bytes(1).await.expect("first buffered frame");
        assert_eq!(first.as_ref(), b"+ONE\r\n");

        let second = timeout(Duration::from_millis(25), reader.read_response_group_raw_bytes(1))
            .await
            .expect("second buffered frame should not wait for socket")
            .expect("second buffered frame");
        assert_eq!(second.as_ref(), b"+TWO\r\n");
    }

    #[tokio::test]
    async fn response_frame_reader_returns_one_buffered_frame_at_a_time() {
        let (mut reader, _server_stream) = quiet_reader_with_buffer(b"+ONE\r\n+TWO\r\n");

        let first = reader.read_response_frame_raw_bytes().await.expect("first buffered frame");
        assert_eq!(first.as_ref(), b"+ONE\r\n");

        let second = timeout(Duration::from_millis(25), reader.read_response_frame_raw_bytes())
            .await
            .expect("second buffered frame should not wait for socket")
            .expect("second buffered frame");
        assert_eq!(second.as_ref(), b"+TWO\r\n");
    }

    #[tokio::test]
    async fn response_frame_reader_waits_for_partial_bulk_frame() {
        let (mut reader, mut server_stream) = quiet_reader_with_buffer(b"$5\r\nhe");

        server_stream.write_all(b"llo\r\n+NEXT\r\n").await.expect("write remaining response bytes");

        let first = reader.read_response_frame_raw_bytes().await.expect("complete partial frame");
        assert_eq!(first.as_ref(), b"$5\r\nhello\r\n");

        let second = timeout(Duration::from_millis(25), reader.read_response_frame_raw_bytes())
            .await
            .expect("buffered next frame should not wait for socket")
            .expect("second buffered frame");
        assert_eq!(second.as_ref(), b"+NEXT\r\n");
    }

    #[tokio::test]
    async fn response_frame_reader_keeps_nested_array_together() {
        let (mut reader, _server_stream) = quiet_reader_with_buffer(b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n+NEXT\r\n");

        let first = reader.read_response_frame_raw_bytes().await.expect("array frame");
        assert_eq!(first.as_ref(), b"*2\r\n$3\r\none\r\n$3\r\ntwo\r\n");

        let second = timeout(Duration::from_millis(25), reader.read_response_frame_raw_bytes())
            .await
            .expect("buffered next frame should not wait for socket")
            .expect("second buffered frame");
        assert_eq!(second.as_ref(), b"+NEXT\r\n");
    }
}

#[cfg(all(test, feature = "infra-tests"))]
mod tests {
    use super::*;
    use crate::connection::RedisConnection;
    use crate::test_utils::wait_for_redis_ready;
    use testcontainers_modules::testcontainers::runners::AsyncRunner;
    use testcontainers_modules::testcontainers::{ContainerAsync, GenericImage};

    pub async fn run_redis_test<F, Fut>(f: F)
    where
        F: FnOnce(RedisClient) -> Fut,
        Fut: Future<Output = ()>,
    {
        let (_container, host, port) = initialize_redis().await;

        let config = RedisConnection { host, port: Some(port), ..Default::default() };

        let client = RedisClient::connect(&config).await.expect("Failed to connect to Redis");

        f(client).await;
    }

    pub async fn initialize_redis() -> (ContainerAsync<GenericImage>, String, u16) {
        use testcontainers_modules::testcontainers::{GenericImage, core::ContainerPort};

        let container = GenericImage::new("redis", "7.2.4")
            .with_exposed_port(ContainerPort::Tcp(6379))
            .start()
            .await
            .expect("Failed to start database");

        wait_for_redis_ready(&container).await;

        let host_ip = container.get_host().await.expect("Failed to get host address");
        let host_port = container.get_host_port_ipv4(6379).await.expect("Failed to get host port");

        (container, host_ip.to_string(), host_port)
    }

    #[tokio::test]
    async fn test_connect_basic() {
        let (_container, host, port) = initialize_redis().await;

        let config = RedisConnection { host, port: Some(port), ..Default::default() };

        let client = RedisClient::connect(&config).await;
        assert!(client.is_ok());
        assert!(client.unwrap().is_connected().await);
    }

    #[tokio::test]
    async fn test_connect_with_db_selection() {
        let (_container, host, port) = initialize_redis().await;

        let config = RedisConnection { host, port: Some(port), db: Some(1), ..Default::default() };

        let result = RedisClient::connect(&config).await;
        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_connect_invalid_host() {
        let config = RedisConnection {
            host: "invalid.host.local".to_string(),
            port: Some(6379),
            ..Default::default()
        };

        let result = RedisClient::connect(&config).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_send_command_resp3() {
        run_redis_test(|mut client| async move {
            let ping_cmd = b"*1\r\n$4\r\nPING\r\n";
            let result = client.send_command_raw(ping_cmd).await;

            assert!(result.is_ok());
            let expected = b"+PONG\r\n";
            assert_eq!(result.unwrap().0.to_bytes().as_ref(), expected);
        })
        .await;
    }

    #[tokio::test]
    async fn test_send_pipeline_resp3() {
        run_redis_test(|mut client| async move {
            let mut pipeline: Vec<u8> = vec![];
            let command_count = 10;

            for _ in 0..command_count {
                pipeline.extend(b"*1\r\n$4\r\nPING\r\n");
            }

            let result = client.send_command_raw(&pipeline).await;

            assert!(result.is_ok());
            let (responses, _latency) = result.unwrap();
            assert_eq!(responses.commands(), command_count);

            let _expected = RespBytes::resp3(Bytes::from_static(b"+PONG\r\n"));
            // for response in responses {
            //     assert_eq!(response, expected);
            // }
        })
        .await;
    }

    #[tokio::test]
    async fn test_set_and_get() {
        run_redis_test(|mut client| async move {
            // SET key value
            let set_cmd = b"*3\r\n$3\r\nSET\r\n$7\r\ntestkey\r\n$9\r\ntestvalue\r\n";
            let result = client.send_command_raw(set_cmd).await;
            assert!(result.is_ok());

            // GET key
            let get_cmd = b"*2\r\n$3\r\nGET\r\n$7\r\ntestkey\r\n";
            let result = client.send_command_raw(get_cmd).await;
            assert!(result.is_ok());

            let response = result.unwrap().0.to_bytes();
            let response_str = String::from_utf8_lossy(&response);
            assert!(response_str.contains("testvalue"));
        })
        .await;
    }

    #[tokio::test]
    async fn test_pipeline_set_commands() {
        run_redis_test(|mut client| async move {
            let mut pipeline: Vec<u8> = vec![];
            let command_count = 5;

            for i in 0..command_count {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                let set_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value);
                pipeline.extend(set_cmd.as_bytes());
            }

            let result = client.send_command_raw(&pipeline).await;
            assert!(result.is_ok());

            let (responses, _latency) = result.unwrap();
            assert_eq!(responses.commands(), command_count);

            // All SET commands should return +OK
            // for response in &responses {
            //     let bytes = response.clone().to_bytes();
            //     let resp_str = String::from_utf8_lossy(&bytes);
            //     assert!(resp_str.contains("OK"));
            // }
        })
        .await;
    }

    #[tokio::test]
    async fn test_10_pipeline() {
        run_redis_test(|mut client| async move {
            let mut pipeline: Vec<u8> = vec![];
            let command_count = 10;

            for i in 0..command_count {
                let key = format!("largekey{}", i);
                let value = format!("largevalue{}", i);
                let set_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value);
                pipeline.extend(set_cmd.as_bytes());
            }

            let result = client.send_command_raw(&pipeline).await;
            assert!(result.is_ok());

            let (responses, _latency) = result.unwrap();
            assert_eq!(responses.commands(), command_count);
        })
        .await;
    }

    #[tokio::test]
    async fn test_100_pipeline_raw_bytes() {
        run_redis_test(|mut client| async move {
            let mut pipeline: Vec<u8> = vec![];
            let command_count = 100;

            for i in 0..command_count {
                let key = format!("key{}", i);
                let value = format!("value{}", i);
                let set_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value);
                pipeline.extend(set_cmd.as_bytes());
            }

            // Print first 500 bytes of pipeline
            println!("Pipeline preview (first 500 bytes):");
            println!("{}", String::from_utf8_lossy(&pipeline[..pipeline.len().min(500)]));
            println!("\nTotal pipeline length: {} bytes", pipeline.len());

            // Count asterisks manually
            let asterisk_count = pipeline.iter().filter(|&&b| b == b'*').count();
            println!("Total '*' characters in pipeline: {}", asterisk_count);

            let t0 = std::time::Instant::now();
            let result = client.send_command_raw(&pipeline).await;
            let _duration = t0.elapsed();

            assert!(result.is_ok());
            let (responses, _latency) = result.unwrap();

            println!("Expected commands: {}", command_count);
            println!("Responses received: {}", responses.commands());

            assert_eq!(responses.commands(), command_count);
        })
        .await;
    }

    #[tokio::test]
    async fn test_del_command() {
        run_redis_test(|mut client| async move {
            // SET key
            let set_cmd = b"*3\r\n$3\r\nSET\r\n$6\r\ndelkey\r\n$8\r\ndelvalue\r\n";
            let _ = client.send_command_raw(set_cmd).await.unwrap();

            // DEL key
            let del_cmd = b"*2\r\n$3\r\nDEL\r\n$6\r\ndelkey\r\n";
            let result = client.send_command_raw(del_cmd).await;
            assert!(result.is_ok());

            // Verify deletion - GET should return nil
            let get_cmd = b"*2\r\n$3\r\nGET\r\n$6\r\ndelkey\r\n";
            let (response, _latency) = client.send_command_raw(get_cmd).await.expect("failed to send command");
            let result = response.to_bytes();

            let response = String::from_utf8_lossy(&result);
            assert!(response.contains("$-1") || response.contains("_\r\n")); // nil response
        })
        .await;
    }

    #[tokio::test]
    async fn test_exists_command() {
        run_redis_test(|mut client| async move {
            // SET key
            let set_cmd = b"*3\r\n$3\r\nSET\r\n$9\r\nexistskey\r\n$11\r\nexistsvalue\r\n";
            let _ = client.send_command_raw(set_cmd).await.unwrap();

            // EXISTS key
            let exists_cmd = b"*2\r\n$6\r\nEXISTS\r\n$9\r\nexistskey\r\n";
            let (response, _latency) = client.send_command_raw(exists_cmd).await.expect("failed to send command");
            let result = response.to_bytes();

            let response = String::from_utf8_lossy(&result);
            assert!(response.contains(":1") || response.contains("#t")); // 1 for exists
        })
        .await;
    }

    #[tokio::test]
    async fn test_lpush_and_lrange() {
        run_redis_test(|mut client| async move {
            // LPUSH list values
            let lpush_cmd = b"*4\r\n$5\r\nLPUSH\r\n$6\r\nmylist\r\n$6\r\nvalue1\r\n$6\r\nvalue2\r\n";
            let result = client.send_command_raw(lpush_cmd).await;
            assert!(result.is_ok());

            // LRANGE list 0 -1
            let lrange_cmd = b"*4\r\n$6\r\nLRANGE\r\n$6\r\nmylist\r\n$1\r\n0\r\n$2\r\n-1\r\n";
            let (response, _latency) = client.send_command_raw(lrange_cmd).await.expect("command failed");
            let result = response.to_bytes();

            let response = String::from_utf8_lossy(&result);
            println!("{}", response);
            assert!(response.contains("value"));
        })
        .await;
    }

    #[tokio::test]
    async fn test_empty_pipeline() {
        run_redis_test(|mut client| async move {
            let pipeline: Vec<u8> = vec![];
            let result = client.send_command_raw(&pipeline).await;

            assert!(result.is_ok());
            let (responses, _latency) = result.unwrap();
            assert_eq!(responses.to_bytes().len(), 0);
        })
        .await;
    }

    #[tokio::test]
    async fn test_resp_bytes_to_bytes() {
        let data = vec![1, 2, 3];
        let resp2 = RespBytes::Resp2(Bytes::from(data.clone()));
        assert_eq!(resp2.into_bytes().as_ref(), data.as_slice());

        let resp3 = RespBytes::Resp3(Bytes::from(data.clone()));
        assert_eq!(resp3.into_bytes().as_ref(), data.as_slice());
    }

    #[tokio::test]
    async fn test_resp_bytes_len() {
        let data = vec![1, 2, 3, 4, 5];
        let resp2 = RespBytes::Resp2(Bytes::from(data.clone()));
        assert_eq!(resp2.len(), 5);
        assert!(!resp2.is_empty());

        let resp3 = RespBytes::Resp3(Bytes::from(data.clone()));
        assert_eq!(resp3.len(), 5);
        assert!(!resp3.is_empty());

        let empty = RespBytes::Resp2(Bytes::new());
        assert!(empty.is_empty());
    }

    #[tokio::test]
    async fn test_multiple_gets_after_pipeline_set() {
        run_redis_test(|mut client| async move {
            // Pipeline SET commands
            let mut pipeline: Vec<u8> = vec![];
            for i in 0..10 {
                let key = format!("multikey{}", i);
                let value = format!("multivalue{}", i);
                let set_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n${}\r\n{}\r\n", key.len(), key, value.len(), value);
                pipeline.extend(set_cmd.as_bytes());
            }

            let _ = client.send_command_raw(&pipeline).await.unwrap();

            // Now GET each key individually
            for i in 0..10 {
                let key = format!("multikey{}", i);
                let get_cmd = format!("*2\r\n$3\r\nGET\r\n${}\r\n{}\r\n", key.len(), key);

                let (response, _latency) = client.send_command_raw(get_cmd.as_bytes()).await.expect("command failed");
                let result = response.to_bytes();

                let response = String::from_utf8_lossy(&result);
                let expected_value = format!("multivalue{}", i);
                assert!(response.contains(&expected_value));
            }
        })
        .await;
    }

    #[tokio::test]
    async fn test_watch_aborts_after_external_write() {
        let (_container, host, port) = initialize_redis().await;

        let config = RedisConnection { host: host.clone(), port: Some(port), ..Default::default() };

        let mut watched_client = RedisClient::connect(&config).await.expect("failed to connect watched client");
        let mut interferer = RedisClient::connect(&config).await.expect("failed to connect interferer");

        let key = "watched_key";

        // Prime the key
        let set_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\n0\r\n", key.len(), key);
        watched_client.send_command_raw(set_cmd.as_bytes()).await.expect("prime set failed");

        let watch_cmd = format!("*2\r\n$5\r\nWATCH\r\n${}\r\n{}\r\n", key.len(), key);
        watched_client.send_command_raw(watch_cmd.as_bytes()).await.expect("watch failed");

        // External change to trigger EXEC abort
        let interfering_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\n1\r\n", key.len(), key);
        interferer.send_command_raw(interfering_cmd.as_bytes()).await.expect("interfering write failed");

        let multi_cmd = b"*1\r\n$5\r\nMULTI\r\n";
        let tx_cmd = format!("*3\r\n$3\r\nSET\r\n${}\r\n{}\r\n$1\r\n2\r\n", key.len(), key);
        let exec_cmd = b"*1\r\n$4\r\nEXEC\r\n";

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(multi_cmd);
        pipeline.extend_from_slice(tx_cmd.as_bytes());
        pipeline.extend_from_slice(exec_cmd);

        let (responses, _latency) = watched_client.send_command_raw(&pipeline).await.expect("pipeline send failed");
        let bytes = responses.to_bytes();

        // Decode RESP3 frames to inspect EXEC result
        let mut offset = 0;
        let mut frames = Vec::new();
        while offset < bytes.len() {
            let decoded = redis_protocol::resp3::decode::complete::decode(&bytes[offset..]).unwrap();
            if let Some((frame, consumed)) = decoded {
                frames.push(frame);
                offset += consumed;
            } else {
                break;
            }
        }

        assert_eq!(frames.len(), 3, "expected MULTI, queued command, and EXEC responses");
        assert!(
            matches!(frames[2], redis_protocol::resp3::types::OwnedFrame::Null),
            "EXEC should abort when watched key changes"
        );
    }

    /// Dropping the guard without disarming must remove the connection from the
    /// pool so a subsequent checkout gets a fresh one.
    #[tokio::test]
    async fn test_poison_guard_discards_connection() {
        use crate::pool::RedisConnectionManager;
        use deadpool::managed::Pool;
        use ep_core::pool::PoisonGuard;

        let (_container, host, port) = initialize_redis().await;
        let config = RedisConnection { host, port: Some(port), ..Default::default() };

        let mgr = RedisConnectionManager::new(config);
        let pool: Pool<RedisConnectionManager> = Pool::builder(mgr).max_size(1).build().expect("pool");

        {
            let mut conn = pool.get().await.expect("checkout");
            let set_cmd = b"*3\r\n$3\r\nSET\r\n$10\r\npoison_key\r\n$5\r\nhello\r\n";
            conn.send_command_raw(set_cmd).await.expect("SET");
        }
        assert_eq!(pool.status().size, 1);

        {
            let conn = pool.get().await.expect("checkout");
            let mut guard = PoisonGuard::new(conn);

            let ping = b"*1\r\n$4\r\nPING\r\n";
            let resp = guard.send_command_raw(ping).await.expect("PING");
            assert_eq!(resp.0.to_bytes().as_ref(), b"+PONG\r\n");

            // Do not disarm: simulates a cancelled future.
        }

        assert_eq!(pool.status().size, 0, "poisoned connection should not be in pool");

        {
            let mut conn = pool.get().await.expect("fresh checkout");
            let get_cmd = b"*2\r\n$3\r\nGET\r\n$10\r\npoison_key\r\n";
            let resp = conn.send_command_raw(get_cmd).await.expect("GET");
            assert_eq!(resp.0.to_bytes().as_ref(), b"$5\r\nhello\r\n");
        }
        assert_eq!(pool.status().size, 1);
    }

    /// Disarming the guard must return the connection to the pool normally.
    #[tokio::test]
    async fn test_poison_guard_disarm_returns_to_pool() {
        use crate::pool::RedisConnectionManager;
        use deadpool::managed::Pool;
        use ep_core::pool::PoisonGuard;

        let (_container, host, port) = initialize_redis().await;
        let config = RedisConnection { host, port: Some(port), ..Default::default() };

        let mgr = RedisConnectionManager::new(config);
        let pool: Pool<RedisConnectionManager> = Pool::builder(mgr).max_size(1).build().expect("pool");

        {
            let conn = pool.get().await.expect("checkout");
            let mut guard = PoisonGuard::new(conn);

            let ping = b"*1\r\n$4\r\nPING\r\n";
            guard.send_command_raw(ping).await.expect("PING");

            guard.disarm();
        }

        assert_eq!(pool.status().size, 1, "disarmed connection should be returned to pool");
    }

    #[tokio::test]
    async fn test_select_state_leaks_across_pool_reuse() {
        use crate::pool::RedisConnectionManager;
        use deadpool::managed::Pool;

        let (_container, host, port) = initialize_redis().await;
        let config = RedisConnection { host, port: Some(port), db: Some(0), ..Default::default() };
        let pool: Pool<RedisConnectionManager> = Pool::builder(RedisConnectionManager::new(config)).max_size(1).build().expect("pool");

        {
            let mut conn = pool.get().await.expect("checkout");
            let select_db_five = b"*2\r\n$6\r\nSELECT\r\n$1\r\n5\r\n";
            let select_resp = conn.send_command_raw(select_db_five).await.expect("SELECT 5");
            assert!(select_resp.0.to_bytes().as_ref().starts_with(b"+OK"));

            let set_cmd = b"*3\r\n$3\r\nSET\r\n$16\r\nselect:pool:leak\r\n$4\r\nleak\r\n";
            conn.send_command_raw(set_cmd).await.expect("SET in db 5");
        }

        {
            let mut conn = pool.get().await.expect("reused checkout");
            let get_cmd = b"*2\r\n$3\r\nGET\r\n$16\r\nselect:pool:leak\r\n";
            let get_resp = conn.send_command_raw(get_cmd).await.expect("GET after reuse");
            let resp = get_resp.0.to_bytes();
            assert!(
                resp.as_ref() == b"$-1\r\n" || resp.as_ref() == b"_\r\n",
                "the recycled pooled connection should return a null response from db 0, got {:?}",
                resp
            );
        }
    }

    #[tokio::test]
    async fn test_auth_state_leaks_across_pool_reuse() {
        use crate::pool::RedisConnectionManager;
        use deadpool::managed::Pool;

        let (_container, host, port) = initialize_redis().await;
        let admin_config = RedisConnection { host: host.clone(), port: Some(port), ..Default::default() };
        let mut admin = RedisClient::connect(&admin_config).await.expect("admin connection");
        let create_user = concat!(
            "*8\r\n",
            "$3\r\nACL\r\n",
            "$7\r\nSETUSER\r\n",
            "$17\r\neden_pool_limited\r\n",
            "$2\r\non\r\n",
            "$12\r\n>limitedpass\r\n",
            "$2\r\n~*\r\n",
            "$4\r\n+GET\r\n",
            "$5\r\n+PING\r\n"
        );
        let create_resp = admin.send_command_raw(create_user.as_bytes()).await.expect("ACL SETUSER");
        assert!(create_resp.0.to_bytes().as_ref().starts_with(b"+OK"));

        let pool: Pool<RedisConnectionManager> =
            Pool::builder(RedisConnectionManager::new(RedisConnection { host, port: Some(port), ..Default::default() }))
                .max_size(1)
                .build()
                .expect("pool");

        {
            let mut conn = pool.get().await.expect("checkout");
            let auth_cmd = concat!("*3\r\n", "$4\r\nAUTH\r\n", "$17\r\neden_pool_limited\r\n", "$11\r\nlimitedpass\r\n");
            let auth_resp = conn.send_command_raw(auth_cmd.as_bytes()).await.expect("AUTH limited user");
            assert!(auth_resp.0.to_bytes().as_ref().starts_with(b"+OK"));
        }

        {
            let mut conn = pool.get().await.expect("reused checkout");
            let set_cmd = b"*3\r\n$3\r\nSET\r\n$14\r\nauth:pool:test\r\n$1\r\n1\r\n";
            let set_resp = conn.send_command_raw(set_cmd).await.expect("SET on reused connection");
            assert!(
                set_resp.0.to_bytes().as_ref().starts_with(b"+OK"),
                "the recycled pooled connection should restore the default ACL context"
            );
        }
    }
}

#[cfg(test)]
mod unit_tests {
    use super::{empty_read_budget, first_command_name};

    #[test]
    fn empty_read_budget_stays_at_five_polls() {
        assert_eq!(empty_read_budget(), 5);
    }

    #[test]
    fn first_command_name_extracts_resp_array_command() {
        assert_eq!(first_command_name(b"*1\r\n$4\r\nPING\r\n").as_deref(), Some("PING"));
        assert_eq!(first_command_name(b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n").as_deref(), Some("GET"));
    }

    #[test]
    fn first_command_name_returns_none_for_non_resp_or_truncated_input() {
        assert!(first_command_name(b"PING\r\n").is_none());
        assert!(first_command_name(b"*1\r\n$4\r\nPIN").is_none());
        assert!(first_command_name(b"*1\r\n+PING\r\n").is_none());
    }
}
