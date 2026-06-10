use crate::RedisOperation;
/// Redis protocol with zero-copy frame scanning support.
///
/// This module provides both traditional full parsing and zero-copy
/// frame boundary detection for passthrough scenarios.
use crate::api::lib::multi_key_policy::{
    FrameAction, RespWireVersion, ResponseCombiner, UNSUPPORTED_MULTI_KEY_ERROR_BYTES, plan_frame, plan_pipeline,
};
use crate::api::{RedisApi, RedisConflictData};
use crate::ep::{RedisAsync, RedisTx};
use crate::protocol::decoder::DecoderRespFrame;
pub use crate::protocol::decoder::RedisCommandArgs;
use crate::protocol::encoder::EncoderRespFrame;
use crate::protocol::scanner::FrameType;
pub use crate::protocol::view::{RedisArgView, RedisCommandView, RedisCommandViewMeta};
use bytes::{Bytes, BytesMut};
use eden_logger_internal::{ctx_with_trace, log_trace};
use endpoint_types::protocol::EpProtocol;
use endpoint_types::request::EpWireRequest;
use ep_core::ReqType;
use ep_core::pool::PoisonGuard;
use error::{EpError, ResultEP};
use format::endpoint::EpKind;
use function_name::named;
use redis_core::config::MultiKeyExecution;
use redis_core::{RedisClient, RedisConnectionManager};
use redis_protocol::resp2::decode::decode as decode_resp2;
use redis_protocol::resp2::encode::encode as encode_resp2;
use redis_protocol::resp2::types::Resp2Frame;
use redis_protocol::resp3::decode::complete::decode;
use redis_protocol::resp3::encode::complete::encode as encode_resp3;
use redis_protocol::resp3::types::Resp3Frame;

pub mod decoder;
pub mod encoder;
pub mod scanner;
pub mod view;

/// Classify whether an endpoint error is transient and worth retrying.
///
/// Retryable: IO failures (broken pipe, connection reset), connection errors,
/// pool errors surfaced as `Request`.
/// Non-retryable: timeouts (already waited), protocol/parse errors (deterministic),
/// auth errors (credentials won't change mid-request).
fn is_retryable(err: &EpError) -> bool {
    matches!(err, EpError::Io(_) | EpError::Connect(_) | EpError::Request(_))
}

type ParseBufferConditionalFrame<'a> = (Option<Box<dyn RedisOperation>>, &'a [u8], usize);

const DISCARD_COMMAND_BYTES: &[u8] = b"*1\r\n$7\r\nDISCARD\r\n";
const EXECABORT_QUEUE_ERROR_BYTES: &[u8] = b"-EXECABORT Transaction discarded because of previous errors.\r\n";

/// Bytes that make up a Redis input or output.
///
/// Uses `Bytes` internally for zero-copy cloning in dual-write scenarios.
#[derive(Debug, Clone)]
pub struct RedisBytes(Bytes);

/// Result of executing raw bytes on a pinned connection.
///
/// `local_transaction_queue_error` is true when deconstruction mode rejects a
/// split or unsupported multi-key command locally while the caller is inside
/// `MULTI`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PinnedRawExecutionResult {
    pub response: Bytes,
    pub local_transaction_queue_error: bool,
}

/// Fast extraction of command name from RESP-encoded bytes without full parsing.
///
/// RESP format for commands: `*<elements>\r\n$<len>\r\n<command>\r\n...`
/// Example: `*1\r\n$4\r\nPING\r\n` -> "PING"
///
/// Uses memchr for fast \r\n scanning.
///
/// # Returns
/// - `Some((start, end))` - byte range of the command name within the input
/// - `None` - if the bytes don't contain a valid RESP command
#[inline]
pub fn extract_resp_command_range(bytes: &[u8]) -> Option<(usize, usize)> {
    // RESP format: *<num>\r\n$<len>\r\n<CMD>\r\n...

    // Skip array marker and count: find first \r\n
    let crlf1 = memchr::memchr(b'\r', bytes)?;
    if bytes.get(crlf1 + 1)? != &b'\n' {
        return None;
    }
    let after_array = crlf1 + 2;

    // Check for bulk string marker
    if bytes.get(after_array)? != &b'$' {
        return None;
    }

    // Find second \r\n (after length)
    let crlf2 = memchr::memchr(b'\r', &bytes[after_array..])?;
    if bytes.get(after_array + crlf2 + 1)? != &b'\n' {
        return None;
    }
    let cmd_start = after_array + crlf2 + 2;

    // Find end of command
    let crlf3 = memchr::memchr(b'\r', &bytes[cmd_start..])?;
    let cmd_end = cmd_start + crlf3;

    Some((cmd_start, cmd_end))
}

/// Fast extraction of command name as a borrowed string slice.
///
/// Returns the command name exactly as it appears in the RESP bytes (preserves case).
/// Use this when you need zero-allocation command extraction.
#[inline]
pub fn extract_resp_command_str(bytes: &[u8]) -> Option<&str> {
    let (start, end) = extract_resp_command_range(bytes)?;
    std::str::from_utf8(&bytes[start..end]).ok()
}

/// Fast extraction of command name as borrowed bytes.
///
/// Returns the command exactly as it appears in the RESP bytes. This is the
/// preferred hot-path accessor when the caller can classify with
/// ASCII-insensitive byte matching and does not need UTF-8 validation.
#[inline]
pub fn extract_resp_command_bytes(bytes: &[u8]) -> Option<&[u8]> {
    let (start, end) = extract_resp_command_range(bytes)?;
    bytes.get(start..end)
}

/// Fast extraction of command name as an owned uppercase String.
///
/// Uses a stack buffer for short commands (≤16 chars) to avoid heap allocation
/// in the common case.
#[inline]
pub fn extract_resp_command_uppercase(bytes: &[u8]) -> Option<String> {
    let (start, end) = extract_resp_command_range(bytes)?;
    let cmd_bytes = &bytes[start..end];

    // Use stack buffer for common short commands to avoid heap allocation
    if cmd_bytes.len() <= 16 {
        let mut buf = [0u8; 16];
        buf[..cmd_bytes.len()].copy_from_slice(cmd_bytes);
        buf[..cmd_bytes.len()].make_ascii_uppercase();
        std::str::from_utf8(&buf[..cmd_bytes.len()]).ok().map(|s| s.to_string())
    } else {
        // Fallback for unusually long commands
        std::str::from_utf8(cmd_bytes).ok().map(|s| s.to_uppercase())
    }
}

impl RedisBytes {
    pub fn new(bytes: Bytes) -> Self {
        Self(bytes)
    }
    pub fn into_bytes(self) -> Bytes {
        self.0
    }
    pub fn bytes(&self) -> &[u8] {
        &self.0
    }
    /// Parse all keys in the given request
    pub fn conflicts(&self) -> ResultEP<Vec<RedisConflictData>> {
        RedisProtocol::parse_conflicts_from_buffer(self.bytes())
    }

    /// Send raw bytes using an existing pinned Redis connection
    /// Returns Bytes for zero-copy efficiency
    pub async fn send_raw_bytes_on_conn(&self, conn: &mut RedisClient) -> ResultEP<Bytes> {
        execute_pinned(&self.0, conn, false, /* reconnect = */ true).await
    }

    /// Send raw bytes on an existing pinned Redis connection without reconnecting on failure.
    /// Returns Bytes for zero-copy efficiency
    pub async fn send_raw_bytes_on_conn_no_reconnect(&self, conn: &mut RedisClient) -> ResultEP<Bytes> {
        self.send_raw_bytes_on_conn_no_reconnect_with_tx_state(conn, false).await
    }

    /// Pinned-connection variant that knows whether the proxy currently
    /// holds an open `MULTI` block.
    ///
    /// Inside `MULTI` Redis queues each command and returns `+QUEUED`,
    /// then `EXEC` returns one reply per queued command. Deconstructing a
    /// multi-key command into per-key parts inside `MULTI` would queue N
    /// entries instead of 1 and break the response array length the
    /// client expects from `EXEC`. So when `in_multi == true`, split and
    /// unsupported multi-key commands are rejected before they reach
    /// Redis. Callers that own transaction state should use the reporting
    /// variant to track those local rejects as transaction queue errors and
    /// abort the later `EXEC`.
    /// Outside `MULTI` the policy is taken from the connection.
    pub async fn send_raw_bytes_on_conn_no_reconnect_with_tx_state(&self, conn: &mut RedisClient, in_multi: bool) -> ResultEP<Bytes> {
        Ok(self.send_raw_bytes_on_conn_no_reconnect_with_tx_report(conn, in_multi).await?.response)
    }

    /// Same as [`Self::send_raw_bytes_on_conn_no_reconnect_with_tx_state`],
    /// but also reports whether a local in-`MULTI` policy rejection occurred.
    ///
    /// This reporting variant is for proxy/session owners. The byte-only
    /// wrapper preserves raw API compatibility and does not own the transaction
    /// queue-error lifecycle.
    pub async fn send_raw_bytes_on_conn_no_reconnect_with_tx_report(
        &self,
        conn: &mut RedisClient,
        in_multi: bool,
    ) -> ResultEP<PinnedRawExecutionResult> {
        execute_pinned_report(&self.0, conn, in_multi, /* reconnect = */ false).await
    }
}

/// Send a buffer of one or more pre-encoded RESP frames on a pinned
/// connection, applying the multi-key policy if it's enabled and we're not
/// inside a `MULTI` block.
///
/// In Native mode, `reconnect == true` uses
/// [`RedisClient::send_command_raw`] (which may reconnect on transport
/// failure), and `false` uses
/// [`RedisClient::send_command_raw_no_reconnect`]. In Deconstruct mode
/// all backend sends use the no-reconnect variant to avoid replaying
/// partially-applied split commands.
async fn execute_pinned(buffer: &Bytes, conn: &mut RedisClient, in_multi: bool, reconnect: bool) -> ResultEP<Bytes> {
    Ok(execute_pinned_report(buffer, conn, in_multi, reconnect).await?.response)
}

async fn execute_pinned_report(
    buffer: &Bytes,
    conn: &mut RedisClient,
    in_multi: bool,
    reconnect: bool,
) -> ResultEP<PinnedRawExecutionResult> {
    let conn_mode = conn.multi_key_execution();

    if in_multi {
        if matches!(conn_mode, MultiKeyExecution::Deconstruct) {
            return execute_pinned_in_multi(buffer, conn, reconnect).await;
        }
        return forward_pinned_result(conn, buffer, reconnect).await;
    }

    // Native: fast forward, byte-identical to the regular raw command path.
    if matches!(conn_mode, MultiKeyExecution::Native) {
        return forward_pinned_result(conn, buffer, reconnect).await;
    }

    let actions = plan_pipeline(buffer, conn_mode)?;
    let mut response = BytesMut::with_capacity(buffer.len());

    for action in actions {
        match action {
            FrameAction::Forward(bytes) => {
                let resp = forward_pinned(conn, &bytes, false).await?;
                response.extend_from_slice(&resp);
            }
            FrameAction::Reject(err) => {
                response.extend_from_slice(&err);
            }
            FrameAction::Split { original: _, parts, combiner, constraint: _ } => {
                let combined = combine_split_parts_pinned(conn, &parts, combiner, false).await?;
                response.extend_from_slice(&combined);
            }
        }
    }

    Ok(PinnedRawExecutionResult {
        response: response.freeze(),
        local_transaction_queue_error: false,
    })
}

async fn execute_pinned_in_multi(buffer: &Bytes, conn: &mut RedisClient, _reconnect: bool) -> ResultEP<PinnedRawExecutionResult> {
    let actions = plan_pipeline(buffer, MultiKeyExecution::Deconstruct)?;
    let mut response = BytesMut::with_capacity(buffer.len());
    let mut local_transaction_queue_error = false;

    for action in actions {
        match action {
            FrameAction::Forward(bytes) => {
                let resp = forward_pinned(conn, &bytes, false).await?;
                response.extend_from_slice(&resp);
            }
            FrameAction::Reject(err) => {
                local_transaction_queue_error = true;
                response.extend_from_slice(&err);
            }
            FrameAction::Split { .. } => {
                local_transaction_queue_error = true;
                response.extend_from_slice(crate::api::lib::multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
            }
        }
    }

    Ok(PinnedRawExecutionResult { response: response.freeze(), local_transaction_queue_error })
}

async fn forward_pinned_result(conn: &mut RedisClient, bytes: &[u8], reconnect: bool) -> ResultEP<PinnedRawExecutionResult> {
    Ok(PinnedRawExecutionResult {
        response: forward_pinned(conn, bytes, reconnect).await?,
        local_transaction_queue_error: false,
    })
}

async fn forward_pinned(conn: &mut RedisClient, bytes: &[u8], reconnect: bool) -> ResultEP<Bytes> {
    if reconnect {
        conn.send_command_raw(bytes).await.map(|(m, _latency)| m.to_bytes())
    } else {
        conn.send_command_raw_no_reconnect(bytes).await.map(|m| m.to_bytes())
    }
}

async fn combine_split_parts_pinned(
    conn: &mut RedisClient,
    parts: &[Bytes],
    combiner: ResponseCombiner,
    reconnect: bool,
) -> ResultEP<Bytes> {
    let mut part_responses = Vec::with_capacity(parts.len());
    let mut protocol_slot: Option<RespWireVersion> = None;
    for part in parts {
        let resp = if reconnect {
            conn.send_command_raw(part).await.map(|(m, _latency)| m)?
        } else {
            conn.send_command_raw_no_reconnect(part).await?
        };
        RespWireVersion::require_consistent(&mut protocol_slot, RespWireVersion::from_resp3_flag(resp.is_resp3()))?;
        part_responses.push(resp.to_bytes());
    }
    let protocol = protocol_slot.ok_or_else(|| EpError::parse("split produced no parts"))?;
    combiner.combine_bytes(part_responses, protocol)
}

impl From<Vec<u8>> for RedisBytes {
    fn from(v: Vec<u8>) -> Self {
        RedisBytes(Bytes::from(v))
    }
}

impl From<Bytes> for RedisBytes {
    /// Zero-copy conversion from Bytes - just increments the Arc reference count.
    fn from(v: Bytes) -> Self {
        RedisBytes(v)
    }
}

impl EpWireRequest<RedisAsync> for RedisBytes {
    fn kind(&self) -> EpKind {
        EpKind::Redis
    }

    fn request_type(&self) -> ResultEP<ReqType> {
        // Fast path: extract command name without full RESP parsing
        // This avoids allocating and parsing the entire frame just to determine read/write
        if let Some(cmd) = extract_resp_command_bytes(&self.0)
            && let Ok(api) = RedisApi::try_from_case_insensitive_bytes(cmd)
        {
            return Ok(api.request_type());
        }
        // Default to Write for safety (writes are more restrictive than reads)
        Ok(ReqType::Write)
    }

    async fn send_raw_bytes(&self, context: &RedisAsync) -> ResultEP<(Bytes, u64)> {
        let mode = context.multi_key_execution();
        let max_retries = context.max_retries();

        if matches!(mode, MultiKeyExecution::Native) {
            return send_raw_native(&self.0, context, max_retries).await;
        }

        send_raw_deconstruct_transaction_aware(&self.0, context).await
    }
}

/// Native fast path: byte-for-byte equivalent to direct raw forwarding.
///
/// No parsing, no allocation, single retry-aware `send_command_raw` call.
/// We split this out so the Deconstruct path is purely additive; Native
/// users pay no overhead for the policy plumbing.
async fn send_raw_native(buffer: &Bytes, context: &RedisAsync, max_retries: u32) -> ResultEP<(Bytes, u64)> {
    let mut last_err: Option<EpError> = None;
    // Track elapsed time so retries don't exceed the caller's timeout
    // budget. The caller wraps this call in a 10s tokio::time::timeout.
    // If a pool checkout + command takes close to that budget, retrying
    // will just race with the timeout and produce confusing cancellation
    // behavior.
    let start = std::time::Instant::now();

    for attempt in 0..=max_retries {
        if attempt > 0 && start.elapsed().as_secs() >= 8 {
            break;
        }

        let client = context.get().await.map_err(EpError::request)?;
        let mut guard = PoisonGuard::new(client);

        match guard.send_command_raw(buffer).await {
            Ok((response, network_latency_us)) => {
                guard.disarm();
                return Ok((response.to_bytes(), network_latency_us));
            }
            Err(e) if is_retryable(&e) && attempt < max_retries => {
                last_err = Some(e);
            }
            Err(e) => return Err(e),
        }
    }

    Err(last_err.unwrap_or_else(|| EpError::request("send_raw_bytes: no attempts executed")))
}

/// Deconstruct path: scan the raw batch frame-by-frame on a single pooled
/// connection, preserving response frame parity while suppressing local
/// deconstruction inside a `MULTI` block.
async fn send_raw_deconstruct_transaction_aware(request_buffer: &Bytes, context: &RedisAsync) -> ResultEP<(Bytes, u64)> {
    let client = context.get().await.map_err(EpError::request)?;
    let mut guard = PoisonGuard::new(client);

    let mut response = BytesMut::with_capacity(request_buffer.len());
    let mut total_latency_us: u64 = 0;
    let mut offset = 0usize;
    let mut in_multi = false;
    let mut local_transaction_queue_error = false;

    while offset < request_buffer.len() {
        let remaining = &request_buffer[offset..];
        let (args, consumed) = match RedisProtocol::parse_buffer(remaining) {
            Ok(Some(parsed)) => parsed,
            Ok(None) | Err(_) => {
                let resp = send_guard_no_reconnect(&mut guard, remaining, &mut total_latency_us).await?;
                response.extend_from_slice(&resp);
                break;
            }
        };

        let command = args.command().clone();
        let frame = Bytes::copy_from_slice(&remaining[..consumed]);

        if in_multi {
            match command {
                RedisApi::Discard => {
                    let resp = send_guard_no_reconnect(&mut guard, &frame, &mut total_latency_us).await?;
                    response.extend_from_slice(&resp);
                    in_multi = false;
                    local_transaction_queue_error = false;
                    offset += consumed;
                    continue;
                }
                RedisApi::Exec => {
                    if local_transaction_queue_error {
                        let _ = send_guard_no_reconnect(&mut guard, DISCARD_COMMAND_BYTES, &mut total_latency_us).await?;
                        response.extend_from_slice(EXECABORT_QUEUE_ERROR_BYTES);
                    } else {
                        let resp = send_guard_no_reconnect(&mut guard, &frame, &mut total_latency_us).await?;
                        response.extend_from_slice(&resp);
                    }
                    in_multi = false;
                    local_transaction_queue_error = false;
                    offset += consumed;
                    continue;
                }
                _ => {}
            }

            match plan_frame(frame, MultiKeyExecution::Deconstruct)? {
                FrameAction::Forward(bytes) => {
                    let resp = send_guard_no_reconnect(&mut guard, &bytes, &mut total_latency_us).await?;
                    response.extend_from_slice(&resp);
                }
                FrameAction::Reject(err) => {
                    local_transaction_queue_error = true;
                    response.extend_from_slice(&err);
                }
                FrameAction::Split { .. } => {
                    local_transaction_queue_error = true;
                    response.extend_from_slice(UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
                }
            }

            offset += consumed;
            continue;
        }

        match plan_frame(frame, MultiKeyExecution::Deconstruct)? {
            FrameAction::Forward(bytes) => {
                let resp = send_guard_no_reconnect(&mut guard, &bytes, &mut total_latency_us).await?;
                if matches!(command, RedisApi::Multi) && !resp.starts_with(b"-") {
                    in_multi = true;
                    local_transaction_queue_error = false;
                }
                response.extend_from_slice(&resp);
            }
            FrameAction::Reject(err) => {
                response.extend_from_slice(&err);
            }
            FrameAction::Split { original: _, parts, combiner, constraint: _ } => {
                let combined = combine_split_parts_guard_no_reconnect(&mut guard, &parts, combiner, &mut total_latency_us).await?;
                response.extend_from_slice(&combined);
            }
        }

        offset += consumed;
    }

    guard.disarm();
    Ok((response.freeze(), total_latency_us))
}

async fn send_guard_no_reconnect(
    guard: &mut PoisonGuard<RedisConnectionManager>,
    bytes: &[u8],
    total_latency_us: &mut u64,
) -> ResultEP<Bytes> {
    let started_at = std::time::Instant::now();
    let resp = guard.send_command_raw_no_reconnect(bytes).await?;
    *total_latency_us = total_latency_us.saturating_add(started_at.elapsed().as_micros() as u64);
    Ok(resp.to_bytes())
}

async fn combine_split_parts_guard_no_reconnect(
    guard: &mut PoisonGuard<RedisConnectionManager>,
    parts: &[Bytes],
    combiner: ResponseCombiner,
    total_latency_us: &mut u64,
) -> ResultEP<Bytes> {
    let mut part_responses = Vec::with_capacity(parts.len());
    let mut protocol_slot: Option<RespWireVersion> = None;
    for part in parts {
        let started_at = std::time::Instant::now();
        let resp = guard.send_command_raw_no_reconnect(part).await?;
        *total_latency_us = total_latency_us.saturating_add(started_at.elapsed().as_micros() as u64);
        RespWireVersion::require_consistent(&mut protocol_slot, RespWireVersion::from_resp3_flag(resp.is_resp3()))?;
        part_responses.push(resp.to_bytes());
    }
    let protocol = protocol_slot.ok_or_else(|| EpError::parse("split produced no parts"))?;
    combiner.combine_bytes(part_responses, protocol)
}

/// A frame that can be either raw bytes (zero-copy) or fully parsed.
#[derive(Debug)]
pub enum MaybeFrame<'a> {
    /// Raw bytes without parsing - for passthrough.
    Raw { bytes: &'a [u8], frame_type: FrameType },
    /// Fully parsed frame - when inspection is needed.
    Parsed(DecoderRespFrame),
}

impl<'a> MaybeFrame<'a> {
    /// Get the raw bytes of this frame.
    pub fn as_bytes(&self) -> &[u8] {
        match self {
            Self::Raw { bytes, .. } => bytes,
            Self::Parsed(_) => &[], // Caller should use encode if needed
        }
    }

    /// Get frame type without full parsing.
    pub fn frame_type(&self) -> FrameType {
        match self {
            Self::Raw { frame_type, .. } => *frame_type,
            Self::Parsed(frame) => frame_type_from_decoded(frame),
        }
    }

    /// Convert to owned bytes (may need encoding for parsed frames).
    pub fn into_bytes(self) -> ResultEP<Vec<u8>> {
        match self {
            Self::Raw { bytes, .. } => Ok(bytes.to_vec()),
            Self::Parsed(frame) => {
                let encoder_frame = match frame {
                    DecoderRespFrame::Resp2(f) => EncoderRespFrame::Resp2(f),
                    DecoderRespFrame::Resp3(f) => EncoderRespFrame::Resp3(f),
                };
                RedisProtocol::encode_to_buffer(&encoder_frame)
            }
        }
    }

    /// Force full parsing if not already parsed.
    pub fn parse(self) -> ResultEP<DecoderRespFrame> {
        match self {
            Self::Parsed(frame) => Ok(frame),
            Self::Raw { bytes, .. } => {
                RedisProtocol::decode_buffer(bytes).map(|(frame, _)| frame).ok_or_else(|| EpError::parse("Failed to parse raw frame"))
            }
        }
    }
}

fn frame_type_from_decoded(frame: &DecoderRespFrame) -> FrameType {
    match frame {
        DecoderRespFrame::Resp2(f) => match f {
            redis_protocol::resp2::types::OwnedFrame::SimpleString(_) => FrameType::SimpleString,
            redis_protocol::resp2::types::OwnedFrame::Error(_) => FrameType::Error,
            redis_protocol::resp2::types::OwnedFrame::Integer(_) => FrameType::Integer,
            redis_protocol::resp2::types::OwnedFrame::BulkString(_) => FrameType::BulkString,
            redis_protocol::resp2::types::OwnedFrame::Array(_) => FrameType::Array,
            redis_protocol::resp2::types::OwnedFrame::Null => FrameType::Null,
        },
        DecoderRespFrame::Resp3(f) => match f {
            redis_protocol::resp3::types::OwnedFrame::SimpleString { .. } => FrameType::SimpleString,
            redis_protocol::resp3::types::OwnedFrame::SimpleError { .. } => FrameType::Error,
            redis_protocol::resp3::types::OwnedFrame::Number { .. } => FrameType::Integer,
            redis_protocol::resp3::types::OwnedFrame::BlobString { .. } => FrameType::BulkString,
            redis_protocol::resp3::types::OwnedFrame::Array { .. } => FrameType::Array,
            redis_protocol::resp3::types::OwnedFrame::Null => FrameType::Null,
            redis_protocol::resp3::types::OwnedFrame::Boolean { .. } => FrameType::Boolean,
            redis_protocol::resp3::types::OwnedFrame::Double { .. } => FrameType::Double,
            redis_protocol::resp3::types::OwnedFrame::BigNumber { .. } => FrameType::BigNumber,
            redis_protocol::resp3::types::OwnedFrame::BlobError { .. } => FrameType::BlobError,
            redis_protocol::resp3::types::OwnedFrame::VerbatimString { .. } => FrameType::VerbatimString,
            redis_protocol::resp3::types::OwnedFrame::Map { .. } => FrameType::Map,
            redis_protocol::resp3::types::OwnedFrame::Set { .. } => FrameType::Set,
            redis_protocol::resp3::types::OwnedFrame::Push { .. } => FrameType::Push,
            _ => FrameType::SimpleString,
        },
    }
}

#[derive(Debug)]
pub struct RedisProtocol {}

impl EpProtocol<DecoderRespFrame, RedisCommandArgs, EncoderRespFrame, RedisAsync, RedisApi, RedisTx, RedisConflictData, dyn RedisOperation>
    for RedisProtocol
{
    fn decode_buffer(buffer: &[u8]) -> Option<(DecoderRespFrame, usize)> {
        // Try RESP3 first (most common case)
        if let Ok(Some((frame, size))) = decode(buffer) {
            return Some((DecoderRespFrame::Resp3(frame), size));
        }

        // Only try RESP2 if RESP3 definitely failed (not just incomplete)
        decode_resp2(buffer).ok().flatten().map(|(frame, size)| (DecoderRespFrame::Resp2(frame), size))
    }

    fn parse_buffer(buffer: &[u8]) -> ResultEP<Option<(RedisCommandArgs, usize)>> {
        if let Some((frame, bytes_consumed)) = Self::decode_buffer(buffer) {
            Ok(Some((RedisCommandArgs::try_from(frame)?, bytes_consumed)))
        } else {
            Ok(None)
        }
    }

    #[named]
    fn parse_buffer_to_operation(buffer: &[u8]) -> ResultEP<Option<(Box<dyn RedisOperation>, usize)>> {
        if let Some((frame, bytes_consumed)) = Self::decode_buffer(buffer) {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_trace!(
                _ctx,
                "ParseBufferToOperation: parsing {frame:?}",
                audience = eden_logger_internal::LogAudience::Internal
            );
            let redis_args = RedisCommandArgs::try_from(frame)?;
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_trace!(
                _ctx,
                "ParseBufferToOperation: parsed redis args {redis_args:?}",
                audience = eden_logger_internal::LogAudience::Internal
            );
            Ok(Some((redis_args.command.decode_from_args(redis_args.args)?, bytes_consumed)))
        } else {
            Ok(None)
        }
    }
    #[named]
    fn parse_conflict_from_buffer(buffer: &[u8]) -> ResultEP<RedisConflictData> {
        if let Some((frame, _bytes_consumed)) = Self::decode_buffer(buffer) {
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_trace!(
                _ctx,
                "ParseBufferToOperation: parsing {frame:?}",
                audience = eden_logger_internal::LogAudience::Internal
            );
            let redis_args = RedisCommandArgs::try_from(frame)?;
            let _ctx = ctx_with_trace!().with_feature("redis");
            log_trace!(
                _ctx,
                "ParseBufferToOperation: parsed redis args {redis_args:?}",
                audience = eden_logger_internal::LogAudience::Internal
            );
            Ok(RedisConflictData::new(redis_args.command.keys_from_args(&redis_args.args)?))
        } else {
            Ok(RedisConflictData::default())
        }
    }

    fn encode_to_buffer(response: &EncoderRespFrame) -> ResultEP<Vec<u8>> {
        match response {
            EncoderRespFrame::Resp2(frame) => {
                let mut buf = vec![0; frame.encode_len(false)];
                let len = encode_resp2(&mut buf, frame, false).map_err(|e| EpError::parse(format!("RESP2 encode error: {e:?}")))?;
                buf.truncate(len);
                Ok(buf)
            }
            EncoderRespFrame::Resp3(frame) => {
                let mut buf = vec![0; frame.encode_len(false)];
                let len = encode_resp3(&mut buf, frame, false).map_err(|e| EpError::parse(format!("RESP3 encode error: {e:?}")))?;
                buf.truncate(len);
                Ok(buf)
            }
        }
    }
}

impl RedisProtocol {
    pub fn parse_command_view_meta(buffer: &[u8]) -> ResultEP<Option<(RedisCommandViewMeta, usize)>> {
        view::parse_command_view_meta(buffer)
    }

    pub fn parse_command_kind(buffer: &[u8]) -> ResultEP<Option<(RedisApi, usize)>> {
        view::parse_command_kind(buffer)
    }

    pub fn parse_command_view_meta_from_frame(frame: &[u8]) -> ResultEP<RedisCommandViewMeta> {
        view::parse_command_view_meta_from_frame(frame)
    }

    pub fn parse_command_view<'a>(buffer: &'a [u8]) -> ResultEP<Option<(RedisCommandView<'a>, usize)>> {
        view::parse_command_view(buffer)
    }

    /// Scan frame boundary without parsing content (zero-copy).
    ///
    /// Returns (frame_type, bytes_consumed) or None if incomplete.
    /// Use this for passthrough scenarios where you don't need to inspect content.
    #[inline]
    pub fn scan_frame_boundary(buffer: &[u8]) -> Option<(FrameType, usize)> {
        scanner::scan_frame_boundary(buffer)
    }

    /// Scan frame with conditional parsing.
    ///
    /// # Arguments
    /// * `buffer` - Input bytes
    /// * `parse_content` - If true, fully parse; if false, just detect boundary
    ///
    /// # Returns
    /// * `Ok(Some((frame, consumed)))` - Frame scanned/parsed
    /// * `Ok(None)` - Incomplete, need more data
    /// * `Err(_)` - Malformed frame
    pub fn scan_frame<'a>(buffer: &'a [u8], parse_content: bool) -> ResultEP<Option<(MaybeFrame<'a>, usize)>> {
        if parse_content {
            // Full parsing path
            match Self::decode_buffer(buffer) {
                Some((frame, consumed)) => Ok(Some((MaybeFrame::Parsed(frame), consumed))),
                None => Ok(None),
            }
        } else {
            // Zero-copy boundary scan
            match scanner::scan_frame_boundary(buffer) {
                Some((frame_type, consumed)) => Ok(Some((MaybeFrame::Raw { bytes: &buffer[..consumed], frame_type }, consumed))),
                None => Ok(None),
            }
        }
    }

    /// Scan multiple frames from buffer with conditional parsing.
    ///
    /// Useful for pipeline processing.
    pub fn scan_frames<'a>(buffer: &'a [u8], parse_content: bool) -> ResultEP<(Vec<MaybeFrame<'a>>, usize)> {
        let mut frames = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            match Self::scan_frame(&buffer[offset..], parse_content)? {
                Some((frame, consumed)) => {
                    frames.push(frame);
                    offset += consumed;
                }
                None => break,
            }
        }

        Ok((frames, offset))
    }

    /// Parse buffer to operation with conditional full parsing.
    ///
    /// # Arguments
    /// * `buffer` - Input bytes
    /// * `need_operation` - If true, fully parse to get operation; if false, just scan boundary
    ///
    /// # Returns
    /// For need_operation=true: parsed operation
    /// For need_operation=false: just boundary info (operation will be None)
    #[named]
    pub fn parse_buffer_conditional(buffer: &[u8], need_operation: bool) -> ResultEP<Option<ParseBufferConditionalFrame<'_>>> {
        if need_operation {
            // Full parsing path - need the operation
            if let Some((frame, bytes_consumed)) = Self::decode_buffer(buffer) {
                let _ctx = ctx_with_trace!().with_feature("redis");
                log_trace!(_ctx, "ParseBufferConditional: full parse", audience = eden_logger_internal::LogAudience::Internal);
                let redis_args = RedisCommandArgs::try_from(frame)?;
                let operation = redis_args.command.decode_from_args(redis_args.args)?;
                Ok(Some((Some(operation), &buffer[..bytes_consumed], bytes_consumed)))
            } else {
                Ok(None)
            }
        } else {
            // Zero-copy path - just find boundary
            match scanner::scan_frame_boundary(buffer) {
                Some((_frame_type, consumed)) => {
                    let _ctx = ctx_with_trace!().with_feature("redis");
                    log_trace!(
                        _ctx,
                        "ParseBufferConditional: zero-copy scan",
                        audience = eden_logger_internal::LogAudience::Internal,
                        frame_type = format!("{:?}", _frame_type)
                    );
                    Ok(Some((None, &buffer[..consumed], consumed)))
                }
                None => Ok(None),
            }
        }
    }

    // =========================================================================
    // Existing pipeline helpers
    // =========================================================================

    /// Parse a pipeline request to count number of commands and validate structure
    pub fn parse_pipeline_count(buffer: &[u8]) -> ResultEP<usize> {
        let mut count = 0;
        let mut offset = 0;

        while offset < buffer.len() {
            // Find start of command (array marker '*')
            if buffer[offset] == b'*' {
                count += 1;

                // Skip to next line (after \r\n)
                while offset < buffer.len() && buffer[offset] != b'\n' {
                    offset += 1;
                }

                if offset >= buffer.len() {
                    return Err(EpError::parse("incomplete command - missing newline"));
                }
            }
            offset += 1;
        }

        if count == 0 {
            return Err(EpError::parse("no commands found in pipeline request"));
        }

        Ok(count)
    }

    /// Parse a pipeline response (multiple RESP frames) into individual response bytes
    pub fn parse_pipeline_response(buffer: &[u8]) -> ResultEP<Vec<Vec<u8>>> {
        let mut responses = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            match Self::decode_buffer(&buffer[offset..]) {
                Some((_, bytes_consumed)) => {
                    // Extract the raw bytes for this response
                    responses.push(buffer[offset..offset + bytes_consumed].to_vec());
                    offset += bytes_consumed;
                }
                None => {
                    if offset == 0 {
                        // No complete frame at all
                        return Err(EpError::parse("incomplete pipeline response"));
                    }
                    return Err(EpError::parse(format!(
                        "incomplete frame at offset {}, {} bytes remaining",
                        offset,
                        buffer.len() - offset
                    )));
                }
            }
        }

        if responses.is_empty() {
            return Err(EpError::parse("empty pipeline response"));
        }

        Ok(responses)
    }

    /// Zero-copy pipeline response parsing - returns slices into original buffer.
    pub fn parse_pipeline_response_zerocopy(buffer: &[u8]) -> ResultEP<Vec<&[u8]>> {
        let mut responses = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            match scanner::scan_frame_boundary(&buffer[offset..]) {
                Some((_, bytes_consumed)) => {
                    responses.push(&buffer[offset..offset + bytes_consumed]);
                    offset += bytes_consumed;
                }
                None => {
                    if offset == 0 {
                        return Err(EpError::parse("incomplete pipeline response"));
                    }
                    return Err(EpError::parse(format!(
                        "incomplete frame at offset {}, {} bytes remaining",
                        offset,
                        buffer.len() - offset
                    )));
                }
            }
        }

        if responses.is_empty() {
            return Err(EpError::parse("empty pipeline response"));
        }

        Ok(responses)
    }
    /// Parse multiple operations in the case of a pipeline
    #[named]
    #[allow(dead_code)]
    fn parse_buffer_to_operations(buffer: &[u8]) -> ResultEP<Vec<(Box<dyn RedisOperation>, usize)>> {
        let mut responses = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            match Self::decode_buffer(&buffer[offset..]) {
                Some((frame, bytes_consumed)) => {
                    // Extract the raw bytes for this response
                    let redis_args = RedisCommandArgs::try_from(frame)?;
                    offset += bytes_consumed;
                    responses.push((redis_args.command.decode_from_args(redis_args.args)?, bytes_consumed));
                }
                None => {
                    if offset == 0 {
                        // No complete frame at all
                        return Err(EpError::parse("incomplete pipeline response"));
                    }
                    // Partial data at end - incomplete pipeline
                    return Err(EpError::parse(format!(
                        "incomplete frame at offset {}, {} bytes remaining",
                        offset,
                        buffer.len() - offset
                    )));
                }
            }
        }

        if responses.is_empty() {
            return Err(EpError::parse("empty pipeline response"));
        }

        Ok(responses)
    }
    #[named]
    fn parse_conflicts_from_buffer(buffer: &[u8]) -> ResultEP<Vec<RedisConflictData>> {
        let mut responses = Vec::new();
        let mut offset = 0;

        while offset < buffer.len() {
            match Self::decode_buffer(&buffer[offset..]) {
                Some((frame, bytes_consumed)) => {
                    // Extract the raw bytes for this response
                    let redis_args = RedisCommandArgs::try_from(frame)?;
                    offset += bytes_consumed;
                    responses.push(RedisConflictData::new(redis_args.command.keys_from_args(&redis_args.args)?));
                }
                None => {
                    if offset == 0 {
                        // No complete frame at all
                        return Err(EpError::parse("incomplete pipeline response"));
                    }
                    // Partial data at end - incomplete pipeline
                    return Err(EpError::parse(format!(
                        "incomplete frame at offset {}, {} bytes remaining",
                        offset,
                        buffer.len() - offset
                    )));
                }
            }
        }

        if responses.is_empty() {
            return Err(EpError::parse("empty pipeline response"));
        }

        Ok(responses)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::api::lib::{DelInput, DelOutput, DiscardInput, GetInput, MgetInput, MgetOutput, MultiInput, RedisCommandInput, SetInput};
    use crate::api::value::RedisJsonValue;
    use crate::command::cmd;
    use crate::test_utils::{RespVersion, setup_with_multi_key_execution};
    use ep_core::ReqType;
    use redis_core::config::MultiKeyExecution;
    use serial_test::serial;

    fn string_arg(value: &str) -> RedisJsonValue {
        RedisJsonValue::String(value.to_string())
    }

    #[test]
    fn test_redis_bytes_request_type_for_get() {
        // Test GET command (read operation)
        // *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
        let get_bytes = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let redis_bytes = RedisBytes::from(get_bytes.to_vec());

        let req_type = redis_bytes.request_type().expect("Failed to determine request type");
        assert_eq!(req_type, ReqType::Read, "GET command should be a Read operation");
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn deconstruct_raw_pipeline_preserves_response_frame_order() {
        let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(
            &SetInput {
                key: "mk:a".into(),
                value: RedisJsonValue::String("va".into()),
                ..Default::default()
            }
            .command(),
        );
        pipeline.extend_from_slice(
            &SetInput {
                key: "mk:b".into(),
                value: RedisJsonValue::String("vb".into()),
                ..Default::default()
            }
            .command(),
        );
        pipeline.extend_from_slice(&MgetInput { keys: vec!["mk:a".into(), "mk:b".into()] }.command());
        pipeline.extend_from_slice(&DelInput { keys: vec!["mk:a".into(), "mk:b".into()] }.command());

        let result = ctx.raw(&pipeline).await.expect("raw deconstruct pipeline");
        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("pipeline response");
        assert_eq!(responses.len(), 4);
        assert_eq!(responses[0], b"+OK\r\n");
        assert_eq!(responses[1], b"+OK\r\n");

        let mget = MgetOutput::decode(responses[2]).expect("decode MGET");
        assert_eq!(mget.len(), 2);
        assert_eq!(mget.get(0), Some(&RedisJsonValue::String("va".into())));
        assert_eq!(mget.get(1), Some(&RedisJsonValue::String("vb".into())));

        let del = DelOutput::decode(responses[3]).expect("decode DEL");
        assert_eq!(del.deleted(), 2);

        ctx.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn pinned_in_multi_rejects_supported_multikey_as_unsupported() {
        let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;
        let mut conn = ctx.pinned_connection().await.expect("pinned connection");

        let multi = RedisBytes::from(MultiInput {}.command())
            .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut conn, false)
            .await
            .expect("MULTI");
        assert_eq!(multi.as_ref(), b"+OK\r\n");

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&MgetInput { keys: vec!["mk:tx:a".into(), "mk:tx:b".into()] }.command());
        pipeline.extend_from_slice(&GetInput { key: "mk:tx:a".into() }.command());

        let queued = RedisBytes::from(pipeline)
            .send_raw_bytes_on_conn_no_reconnect_with_tx_report(&mut conn, true)
            .await
            .expect("pipeline in MULTI");
        assert!(queued.local_transaction_queue_error, "local split rejection must mark the transaction queue dirty");
        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&queued.response).expect("pipeline response");
        assert_eq!(responses.len(), 2);
        assert_eq!(responses[0], crate::api::lib::multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
        assert_eq!(responses[1], b"+QUEUED\r\n");

        let discard = RedisBytes::from(DiscardInput {}.command())
            .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut conn, true)
            .await
            .expect("DISCARD");
        assert_eq!(discard.as_ref(), b"+OK\r\n");

        ctx.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn pinned_in_multi_rejects_unsupported_multikey_without_losing_frame_parity() {
        let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;
        let mut conn = ctx.pinned_connection().await.expect("pinned connection");

        let multi = RedisBytes::from(MultiInput {}.command())
            .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut conn, false)
            .await
            .expect("MULTI");
        assert_eq!(multi.as_ref(), b"+OK\r\n");

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&cmd("PING").get_packed_command());
        pipeline.extend_from_slice(
            &cmd("MSET")
                .arg(string_arg("mk:bad:a"))
                .arg(string_arg("1"))
                .arg(string_arg("mk:bad:b"))
                .arg(string_arg("2"))
                .get_packed_command(),
        );
        pipeline.extend_from_slice(&GetInput { key: "mk:bad:a".into() }.command());

        let result = RedisBytes::from(pipeline)
            .send_raw_bytes_on_conn_no_reconnect_with_tx_report(&mut conn, true)
            .await
            .expect("pipeline in MULTI");
        assert!(result.local_transaction_queue_error, "local reject must mark the transaction queue dirty");
        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result.response).expect("pipeline response");
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0], b"+QUEUED\r\n");
        assert_eq!(responses[1], crate::api::lib::multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
        assert_eq!(responses[2], b"+QUEUED\r\n");

        let discard = RedisBytes::from(DiscardInput {}.command())
            .send_raw_bytes_on_conn_no_reconnect_with_tx_state(&mut conn, true)
            .await
            .expect("DISCARD");
        assert_eq!(discard.as_ref(), b"+OK\r\n");

        ctx.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn raw_deconstruct_multi_pipeline_rejects_split_and_execaborts() {
        let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

        let _ = ctx.raw(&cmd("DEL").arg("tx:key").arg("tx:other").get_packed_command()).await.expect("cleanup keys");

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&MultiInput {}.command());
        pipeline.extend_from_slice(
            &SetInput {
                key: "tx:key".into(),
                value: string_arg("value"),
                ..Default::default()
            }
            .command(),
        );
        pipeline.extend_from_slice(&MgetInput { keys: vec!["tx:key".into(), "tx:other".into()] }.command());
        pipeline.extend_from_slice(&GetInput { key: "tx:key".into() }.command());
        pipeline.extend_from_slice(&cmd("EXEC").get_packed_command());

        let result = ctx.raw(&pipeline).await.expect("raw transaction pipeline");
        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("pipeline response");
        assert_eq!(responses.len(), 5);
        assert_eq!(responses[0], b"+OK\r\n");
        assert_eq!(responses[1], b"+QUEUED\r\n");
        assert_eq!(responses[2], crate::api::lib::multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
        assert_eq!(responses[3], b"+QUEUED\r\n");
        assert_eq!(responses[4], EXECABORT_QUEUE_ERROR_BYTES);

        let get = ctx.raw(&GetInput { key: "tx:key".into() }.command()).await.expect("GET after EXECABORT");
        assert_eq!(get.as_ref(), b"$-1\r\n", "queued SET must be discarded");

        ctx.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn raw_deconstruct_multi_pipeline_discard_clears_local_reject() {
        let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&MultiInput {}.command());
        pipeline.extend_from_slice(&MgetInput { keys: vec!["tx:a".into(), "tx:b".into()] }.command());
        pipeline.extend_from_slice(&DiscardInput {}.command());
        pipeline.extend_from_slice(&cmd("PING").get_packed_command());

        let result = ctx.raw(&pipeline).await.expect("raw discard pipeline");
        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("pipeline response");
        assert_eq!(responses.len(), 4);
        assert_eq!(responses[0], b"+OK\r\n");
        assert_eq!(responses[1], crate::api::lib::multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
        assert_eq!(responses[2], b"+OK\r\n");
        assert_eq!(responses[3], b"+PONG\r\n");

        ctx.stop().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    #[serial]
    async fn raw_deconstruct_multi_pipeline_rejects_unsupported_without_backend_mutation() {
        let mut ctx = setup_with_multi_key_execution(RespVersion::Resp2, None, MultiKeyExecution::Deconstruct).await;

        let _ = ctx.raw(&cmd("DEL").arg("tx:mset:a").arg("tx:mset:b").get_packed_command()).await.expect("cleanup keys");

        let mut pipeline = Vec::new();
        pipeline.extend_from_slice(&MultiInput {}.command());
        pipeline.extend_from_slice(&cmd("MSET").arg("tx:mset:a").arg("1").arg("tx:mset:b").arg("2").get_packed_command());
        pipeline.extend_from_slice(&cmd("EXEC").get_packed_command());

        let result = ctx.raw(&pipeline).await.expect("raw unsupported transaction pipeline");
        let responses = RedisProtocol::parse_pipeline_response_zerocopy(&result).expect("pipeline response");
        assert_eq!(responses.len(), 3);
        assert_eq!(responses[0], b"+OK\r\n");
        assert_eq!(responses[1], crate::api::lib::multi_key_policy::UNSUPPORTED_MULTI_KEY_ERROR_BYTES);
        assert_eq!(responses[2], EXECABORT_QUEUE_ERROR_BYTES);

        let a = ctx.raw(&GetInput { key: "tx:mset:a".into() }.command()).await.expect("GET tx:mset:a");
        let b = ctx.raw(&GetInput { key: "tx:mset:b".into() }.command()).await.expect("GET tx:mset:b");
        assert_eq!(a.as_ref(), b"$-1\r\n");
        assert_eq!(b.as_ref(), b"$-1\r\n");

        ctx.stop().await;
    }

    #[test]
    fn test_redis_bytes_request_type_for_set() {
        // Test SET command (write operation)
        // *3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n
        let set_bytes = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let redis_bytes = RedisBytes::from(set_bytes.to_vec());

        let req_type = redis_bytes.request_type().expect("Failed to determine request type");
        assert_eq!(req_type, ReqType::Write, "SET command should be a Write operation");
    }

    #[test]
    fn test_redis_bytes_request_type_for_hget() {
        // Test HGET command (read operation)
        // *3\r\n$4\r\nHGET\r\n$4\r\nhash\r\n$5\r\nfield\r\n
        let hget_bytes = b"*3\r\n$4\r\nHGET\r\n$4\r\nhash\r\n$5\r\nfield\r\n";
        let redis_bytes = RedisBytes::from(hget_bytes.to_vec());

        let req_type = redis_bytes.request_type().expect("Failed to determine request type");
        assert_eq!(req_type, ReqType::Read, "HGET command should be a Read operation");
    }

    #[test]
    fn test_redis_bytes_request_type_for_hset() {
        // Test HSET command (write operation)
        // *4\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n
        let hset_bytes = b"*4\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n";
        let redis_bytes = RedisBytes::from(hset_bytes.to_vec());

        let req_type = redis_bytes.request_type().expect("Failed to determine request type");
        assert_eq!(req_type, ReqType::Write, "HSET command should be a Write operation");
    }

    #[test]
    fn test_redis_bytes_request_type_for_del() {
        // Test DEL command (write operation)
        // *2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n
        let del_bytes = b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n";
        let redis_bytes = RedisBytes::from(del_bytes.to_vec());

        let req_type = redis_bytes.request_type().expect("Failed to determine request type");
        assert_eq!(req_type, ReqType::Write, "DEL command should be a Write operation");
    }

    // Tests for extract_resp_command_* functions

    #[test]
    fn test_extract_resp_command_range_simple() {
        // PING command: *1\r\n$4\r\nPING\r\n
        // Offsets:       0123 4567 89...
        let bytes = b"*1\r\n$4\r\nPING\r\n";
        let range = extract_resp_command_range(bytes);
        assert_eq!(range, Some((8, 12)));
        assert_eq!(&bytes[8..12], b"PING");
    }

    #[test]
    fn test_extract_resp_command_range_with_args() {
        // GET key: *2\r\n$3\r\nGET\r\n$3\r\nkey\r\n
        // Offsets:  0123 4567 8..
        let bytes = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let range = extract_resp_command_range(bytes);
        assert_eq!(range, Some((8, 11)));
        assert_eq!(&bytes[8..11], b"GET");
    }

    #[test]
    fn test_extract_resp_command_range_lowercase() {
        // Lowercase command: *1\r\n$4\r\nping\r\n
        let bytes = b"*1\r\n$4\r\nping\r\n";
        let range = extract_resp_command_range(bytes);
        assert_eq!(range, Some((8, 12)));
        assert_eq!(&bytes[8..12], b"ping");
    }

    #[test]
    fn test_extract_resp_command_range_invalid_no_array() {
        // Missing array marker
        let bytes = b"$4\r\nPING\r\n";
        assert_eq!(extract_resp_command_range(bytes), None);
    }

    #[test]
    fn test_extract_resp_command_range_invalid_no_bulk_string() {
        // Missing bulk string marker after array
        let bytes = b"*1\r\nPING\r\n";
        assert_eq!(extract_resp_command_range(bytes), None);
    }

    #[test]
    fn test_extract_resp_command_range_incomplete() {
        // Incomplete RESP data
        let bytes = b"*1\r\n$4\r\nPI";
        assert_eq!(extract_resp_command_range(bytes), None);
    }

    #[test]
    fn test_extract_resp_command_range_empty() {
        let bytes = b"";
        assert_eq!(extract_resp_command_range(bytes), None);
    }

    #[test]
    fn test_extract_resp_command_str_simple() {
        let bytes = b"*1\r\n$4\r\nPING\r\n";
        assert_eq!(extract_resp_command_str(bytes), Some("PING"));
    }

    #[test]
    fn test_extract_resp_command_str_preserves_case() {
        // Should preserve original case
        let bytes = b"*1\r\n$4\r\nping\r\n";
        assert_eq!(extract_resp_command_str(bytes), Some("ping"));

        let bytes = b"*1\r\n$4\r\nPiNg\r\n";
        assert_eq!(extract_resp_command_str(bytes), Some("PiNg"));
    }

    #[test]
    fn test_extract_resp_command_str_with_args() {
        let bytes = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        assert_eq!(extract_resp_command_str(bytes), Some("SET"));
    }

    #[test]
    fn test_extract_resp_command_bytes_preserves_wire_bytes() {
        let bytes = b"*2\r\n$4\r\nPiNg\r\n$5\r\nhello\r\n";
        assert_eq!(extract_resp_command_bytes(bytes), Some(&b"PiNg"[..]));
    }

    #[test]
    fn test_extract_resp_command_str_invalid() {
        let bytes = b"not valid resp";
        assert_eq!(extract_resp_command_str(bytes), None);
    }

    #[test]
    fn test_redis_bytes_request_type_classifies_without_command_string() {
        let get = RedisBytes::from(Bytes::from_static(b"*2\r\n$3\r\nget\r\n$3\r\nkey\r\n"));
        assert_eq!(get.request_type().expect("GET request type"), ReqType::Read);

        let set = RedisBytes::from(Bytes::from_static(b"*3\r\n$3\r\nSeT\r\n$3\r\nkey\r\n$5\r\nvalue\r\n"));
        assert_eq!(set.request_type().expect("SET request type"), ReqType::Write);
    }

    #[test]
    fn test_extract_resp_command_uppercase_simple() {
        let bytes = b"*1\r\n$4\r\nPING\r\n";
        assert_eq!(extract_resp_command_uppercase(bytes), Some("PING".to_string()));
    }

    #[test]
    fn test_extract_resp_command_uppercase_converts_lowercase() {
        let bytes = b"*1\r\n$4\r\nping\r\n";
        assert_eq!(extract_resp_command_uppercase(bytes), Some("PING".to_string()));
    }

    #[test]
    fn test_extract_resp_command_uppercase_converts_mixed_case() {
        let bytes = b"*1\r\n$4\r\nPiNg\r\n";
        assert_eq!(extract_resp_command_uppercase(bytes), Some("PING".to_string()));
    }

    #[test]
    fn test_extract_resp_command_uppercase_long_command() {
        // Test command longer than 16 chars (uses heap allocation path)
        // DEBUGSEGFAULTALIASED is 20 chars (hypothetical long command)
        let bytes = b"*1\r\n$20\r\ndebugsegfaultaliased\r\n";
        assert_eq!(extract_resp_command_uppercase(bytes), Some("DEBUGSEGFAULTALIASED".to_string()));
    }

    #[test]
    fn test_extract_resp_command_uppercase_exactly_16_chars() {
        // Test command exactly 16 chars (boundary case for stack buffer)
        let bytes = b"*1\r\n$16\r\nabcdefghijklmnop\r\n";
        assert_eq!(extract_resp_command_uppercase(bytes), Some("ABCDEFGHIJKLMNOP".to_string()));
    }

    #[test]
    fn test_extract_resp_command_uppercase_17_chars() {
        // Test command with 17 chars (just over stack buffer limit)
        let bytes = b"*1\r\n$17\r\nabcdefghijklmnopq\r\n";
        assert_eq!(extract_resp_command_uppercase(bytes), Some("ABCDEFGHIJKLMNOPQ".to_string()));
    }

    #[test]
    fn test_extract_resp_command_uppercase_invalid() {
        let bytes = b"not valid resp";
        assert_eq!(extract_resp_command_uppercase(bytes), None);
    }

    #[test]
    fn test_extract_resp_command_all_common_commands() {
        // Test extraction works for various common Redis commands
        let test_cases = [
            (b"*1\r\n$4\r\nPING\r\n".as_slice(), "PING"),
            (b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n".as_slice(), "GET"),
            (b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n".as_slice(), "SET"),
            (b"*2\r\n$3\r\nDEL\r\n$3\r\nkey\r\n".as_slice(), "DEL"),
            (b"*3\r\n$4\r\nHGET\r\n$4\r\nhash\r\n$5\r\nfield\r\n".as_slice(), "HGET"),
            (b"*4\r\n$4\r\nHSET\r\n$4\r\nhash\r\n$5\r\nfield\r\n$5\r\nvalue\r\n".as_slice(), "HSET"),
            (b"*2\r\n$5\r\nLPUSH\r\n$4\r\nlist\r\n".as_slice(), "LPUSH"),
            (b"*2\r\n$4\r\nSADD\r\n$3\r\nset\r\n".as_slice(), "SADD"),
            (b"*1\r\n$4\r\nINFO\r\n".as_slice(), "INFO"),
            (b"*1\r\n$6\r\nDBSIZE\r\n".as_slice(), "DBSIZE"),
        ];

        for (bytes, expected) in test_cases {
            assert_eq!(
                extract_resp_command_uppercase(bytes),
                Some(expected.to_string()),
                "Failed for command: {}",
                expected
            );
        }
    }
}
