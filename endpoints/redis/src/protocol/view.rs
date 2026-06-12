//! Zero-copy views over an in-memory RESP buffer.
//!
//! The bridge owns a persistent parse buffer and runs
//! [`parse_command_view_meta`] against the bytes as they arrive from
//! the client socket. The result is a [`RedisCommandViewMeta`] — a
//! small `Copy`-friendly description of one command (offsets and
//! sizes within the buffer) — paired with the number of bytes the
//! frame consumed. The caller can then either:
//!
//! - Carry the [`RedisCommandViewMeta`] alongside a `Bytes` slice and
//!   later call [`RedisCommandViewMeta::bind`] to access fields
//!   without copying any payload, or
//! - Materialize an owned [`RedisCommandArgs`] via
//!   [`RedisCommandView::to_owned_args`] when ownership is required
//!   downstream (analytics, audit).
//!
//! ## Why these types exist
//!
//! Parsing on the bridge read side lets us overlap RESP scanning with
//! the next socket read, removing a serial parse stage from the proxy
//! hot path. The view types are designed to be cheap to ship across
//! the bridge → processor channel: only the pre-parsed metadata moves,
//! the underlying byte buffer travels once as a `Bytes` slice.
//!
//! ## Lifetimes
//!
//! [`RedisCommandView<'a>`] and [`RedisArgView<'a>`] borrow from the
//! `&'a [u8]` they were created against. [`RedisCommandViewMeta`] is
//! owned and `'static`-clean; bind it to a buffer with
//! [`RedisCommandViewMeta::bind`] when you need to read argument
//! payloads.

use crate::api::{RedisApi, RedisJsonValue};
use crate::protocol::scanner::{self, FrameType};
use crate::protocol::{RedisCommandArgs, RedisProtocol};
use endpoint_types::protocol::EpProtocol;
use error::{EpError, ParseError, ResultEP};

/// Pre-parsed description of a single RESP command frame.
///
/// Holds the command opcode plus offsets into the original buffer
/// rather than copying argument bytes, so it is cheap to ship across
/// the bridge → processor channel. Bind to a buffer with [`bind`] to
/// read argument payloads.
///
/// [`bind`]: RedisCommandViewMeta::bind
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RedisCommandViewMeta {
    command: RedisApi,
    args_offset: usize,
    args_count: usize,
    first_arg_payload_range: Option<(usize, usize)>,
    has_ttl: bool,
}

impl RedisCommandViewMeta {
    /// Construct a meta record without payload hints. Most callers
    /// should go through [`parse_command_view_meta`], which fills in
    /// `first_arg_payload_range` and `has_ttl` from the parse pass.
    pub fn new(command: RedisApi, args_offset: usize, args_count: usize) -> Self {
        Self::new_with_hints(command, args_offset, args_count, None, false)
    }

    fn new_with_hints(
        command: RedisApi,
        args_offset: usize,
        args_count: usize,
        first_arg_payload_range: Option<(usize, usize)>,
        has_ttl: bool,
    ) -> Self {
        Self {
            command,
            args_offset,
            args_count,
            first_arg_payload_range,
            has_ttl,
        }
    }

    /// The Redis command opcode parsed from the frame.
    pub fn command(&self) -> &RedisApi {
        &self.command
    }

    /// Number of arguments excluding the command name.
    pub fn args_count(&self) -> usize {
        self.args_count
    }

    /// Bind this meta to the buffer it was parsed from. The returned
    /// [`RedisCommandView`] borrows from `raw` for argument access.
    /// `raw` must be the same byte slice (or a slice covering the
    /// same frame bytes) that produced this meta.
    pub fn bind<'a>(&'a self, raw: &'a [u8]) -> RedisCommandView<'a> {
        RedisCommandView { meta: RedisCommandViewMetaRef::Borrowed(self), raw }
    }

    /// Number of keys this command operates on, derived from
    /// `args_count` and the command's keyspace shape (e.g. `MSET`
    /// pairs key/value so `args_count / 2`; `DEL` takes a variadic
    /// key list so `args_count`). Used for routing and metrics.
    pub fn key_count(&self) -> usize {
        use RedisApi::*;
        match self.command {
            Mget | Del | Unlink | Exists | Touch | Watch | Sdiff | Sinter | Sunion | Pfcount | Pfmerge => self.args_count,
            Mset | Msetnx => self.args_count / 2,
            _ => 1,
        }
    }

    /// Bytes of the first argument payload, if the parse pass
    /// recorded its range. Returns `None` for commands with no first
    /// argument or when the payload range was not captured. Used by
    /// audit/sampling paths that want to inspect the first argument
    /// (typically the key) without re-parsing.
    pub fn first_arg_payload<'a>(&self, raw: &'a [u8]) -> Option<&'a [u8]> {
        let (start, end) = self.first_arg_payload_range?;
        raw.get(start..end).filter(|bytes| !bytes.is_empty())
    }

    /// Whether the parser observed an inline TTL flag (e.g. `EX`,
    /// `PX`, `EXAT`, `PXAT`) in the command's argument list.
    pub fn has_ttl_flag(&self) -> bool {
        self.has_ttl
    }
}

/// Borrowed view of a single RESP argument frame.
///
/// Pairs the raw frame bytes with the frame type observed during the
/// parse pass, so accessors can decode the payload without re-scanning
/// the frame header.
#[derive(Debug, Clone, Copy)]
pub struct RedisArgView<'a> {
    raw: &'a [u8],
    frame_type: FrameType,
}

impl<'a> RedisArgView<'a> {
    /// Full RESP frame bytes including the type byte and CRLF.
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// RESP frame type observed during the parse pass.
    pub fn frame_type(&self) -> FrameType {
        self.frame_type
    }

    /// Payload bytes for string-shaped frames (`SimpleString` or
    /// `BulkString`), with framing/length prefixes stripped. Returns
    /// `None` for non-string frames.
    #[inline]
    pub fn as_bytes(&self) -> Option<&'a [u8]> {
        match self.frame_type {
            FrameType::SimpleString => simple_line_payload(self.raw),
            FrameType::BulkString => bulk_payload(self.raw),
            _ => None,
        }
    }

    /// `as_bytes` interpreted as UTF-8. Returns `None` if the bytes
    /// are not valid UTF-8 or the frame is not string-shaped.
    pub fn as_str(&self) -> Option<&'a str> {
        std::str::from_utf8(self.as_bytes()?).ok()
    }

    /// Parse the payload as a signed 64-bit integer. Only succeeds
    /// for `Integer` frames; string-shaped numeric values return
    /// `None`.
    pub fn as_i64(&self) -> Option<i64> {
        match self.frame_type {
            FrameType::Integer => self.as_str()?.parse().ok(),
            _ => None,
        }
    }

    /// Decode the frame into an owned [`RedisJsonValue`]. Used by
    /// audit/analytics flows that need a structured representation
    /// of the argument; performs a full RESP decode.
    pub fn to_owned_json(&self) -> ResultEP<RedisJsonValue> {
        let Some((frame, consumed)) = RedisProtocol::decode_buffer(self.raw) else {
            return Err(EpError::Parse(ParseError::Custom("failed to decode Redis command argument".to_string())));
        };
        if consumed != self.raw.len() {
            return Err(EpError::Parse(ParseError::Custom(
                "decoded Redis command argument did not consume the full frame".to_string(),
            )));
        }
        RedisJsonValue::try_from(frame)
    }

    /// Lossy byte representation suitable for sampling logs: prefers
    /// the string payload, falls back to the integer rendering, and
    /// finally to the frame-type debug string for non-textual frames.
    pub fn to_sampled_bytes(&self) -> Vec<u8> {
        if let Some(bytes) = self.as_bytes() {
            bytes.to_vec()
        } else if let Some(value) = self.as_i64() {
            value.to_string().into_bytes()
        } else {
            format!("{:?}", self.frame_type).into_bytes()
        }
    }
}

#[derive(Debug, Clone)]
enum RedisCommandViewMetaRef<'a> {
    Borrowed(&'a RedisCommandViewMeta),
    Owned(RedisCommandViewMeta),
}

impl RedisCommandViewMetaRef<'_> {
    fn as_ref(&self) -> &RedisCommandViewMeta {
        match self {
            Self::Borrowed(meta) => meta,
            Self::Owned(meta) => meta,
        }
    }
}

/// A [`RedisCommandViewMeta`] bound to the buffer that produced it.
///
/// Created either by [`RedisCommandViewMeta::bind`] (borrowing an
/// existing meta) or by [`parse_command_view`] (parsing and binding
/// in one step). Provides argument iteration and owned-conversion
/// helpers without copying argument bytes until the caller asks.
#[derive(Debug, Clone)]
pub struct RedisCommandView<'a> {
    meta: RedisCommandViewMetaRef<'a>,
    raw: &'a [u8],
}

impl<'a> RedisCommandView<'a> {
    fn new_owned(meta: RedisCommandViewMeta, raw: &'a [u8]) -> Self {
        Self { meta: RedisCommandViewMetaRef::Owned(meta), raw }
    }

    fn meta(&self) -> &RedisCommandViewMeta {
        self.meta.as_ref()
    }

    /// The Redis command opcode.
    pub fn command(&self) -> &RedisApi {
        self.meta().command()
    }

    /// The raw bytes of the command frame this view was bound to.
    pub fn raw(&self) -> &'a [u8] {
        self.raw
    }

    /// Number of arguments excluding the command name.
    pub fn args_count(&self) -> usize {
        self.meta().args_count()
    }

    /// Number of keys this command operates on. See
    /// [`RedisCommandViewMeta::key_count`].
    pub fn key_count(&self) -> usize {
        self.meta().key_count()
    }

    /// Iterator over argument frames in submission order. Each
    /// iteration scans one frame from the buffer, so consumers that
    /// need random access should collect the iterator.
    pub fn args(&self) -> RedisArgIter<'a> {
        let meta = self.meta();
        RedisArgIter {
            raw: self.raw,
            offset: meta.args_offset,
            remaining: meta.args_count,
        }
    }

    /// Return the argument at `index`, or `None` if out of range.
    /// Walks the argument iterator linearly; use [`args`] when
    /// fetching multiple arguments.
    ///
    /// [`args`]: RedisCommandView::args
    pub fn arg(&self, index: usize) -> Option<RedisArgView<'a>> {
        self.args().nth(index)
    }

    /// Convenience for [`arg(0)`](Self::arg) — typically the first
    /// key for keyed commands.
    pub fn first_arg(&self) -> Option<RedisArgView<'a>> {
        self.arg(0)
    }

    /// Materialize the command and all its arguments into an owned
    /// [`RedisCommandArgs`]. Performs a full RESP decode of every
    /// argument; reach for it only when downstream code needs
    /// owned/structured access.
    pub fn to_owned_args(&self) -> ResultEP<RedisCommandArgs> {
        let mut args = Vec::with_capacity(self.args_count());
        for arg in self.args() {
            args.push(arg.to_owned_json()?);
        }
        Ok(RedisCommandArgs::new(self.command().clone(), args))
    }
}

impl<'a> TryFrom<RedisCommandView<'a>> for RedisCommandArgs {
    type Error = EpError;

    fn try_from(view: RedisCommandView<'a>) -> Result<Self, Self::Error> {
        view.to_owned_args()
    }
}

/// Iterator yielded by [`RedisCommandView::args`]. Walks argument
/// frames in submission order, scanning each frame's boundary on
/// demand.
#[derive(Debug, Clone)]
pub struct RedisArgIter<'a> {
    raw: &'a [u8],
    offset: usize,
    remaining: usize,
}

impl<'a> Iterator for RedisArgIter<'a> {
    type Item = RedisArgView<'a>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.remaining == 0 || self.offset >= self.raw.len() {
            return None;
        }

        let (frame_type, consumed) = scanner::scan_frame_boundary(&self.raw[self.offset..])?;
        let start = self.offset;
        let end = start + consumed;
        self.offset = end;
        self.remaining -= 1;

        Some(RedisArgView { raw: &self.raw[start..end], frame_type })
    }
}

/// Parse the next RESP array frame in `buffer` into a
/// [`RedisCommandViewMeta`].
///
/// Returns:
/// - `Ok(Some((meta, consumed)))` when a complete command frame is
///   present. `consumed` is the number of bytes the frame occupied;
///   the caller should advance its read cursor by that amount.
/// - `Ok(None)` when more bytes are needed (incomplete frame).
/// - `Err(...)` on protocol-level errors (e.g. non-array frame, bad
///   length header, malformed argument). The buffer is not advanced
///   on error; the caller decides whether to drop the connection.
///
/// The parser does not allocate: it scans `buffer` in place and
/// returns offsets into it.
pub fn parse_command_view_meta(buffer: &[u8]) -> ResultEP<Option<(RedisCommandViewMeta, usize)>> {
    if buffer.is_empty() {
        return Ok(None);
    }
    if buffer.first() != Some(&b'*') {
        return Err(EpError::Parse(ParseError::Custom("expected array frame for Redis command".to_string())));
    }

    let Some(header_end) = find_crlf(buffer) else {
        return Ok(None);
    };
    let element_count = parse_array_element_count(&buffer[1..header_end])?;
    let mut offset = header_end + 2;
    let mut first = None;
    let mut second = None;
    let mut first_len = 0usize;
    let mut second_len = 0usize;
    let mut second_start = 0usize;
    let mut third = None;
    let mut third_start = 0usize;
    let mut set_has_ttl = false;

    for index in 0..element_count {
        let arg_start = offset;
        let Some(arg) = scan_arg_at(buffer, offset) else {
            return Ok(None);
        };
        let arg_len = arg.raw.len();
        if index == 0 {
            first_len = arg_len;
            first = Some(arg);
        } else if index == 1 {
            second_len = arg_len;
            second_start = arg_start;
            second = Some(arg);
        } else if index == 2 {
            third_start = arg_start;
            third = Some(arg);
        } else if index >= 3
            && first.and_then(|arg| arg.as_bytes()).is_some_and(|command| command.eq_ignore_ascii_case(b"SET"))
            && arg.as_bytes().is_some_and(is_set_ttl_option)
        {
            set_has_ttl = true;
        }
        offset += arg_len;
    }

    let first = first.ok_or_else(|| EpError::Parse(ParseError::Custom("missing Redis command name".to_string())))?;
    let first_bytes = first
        .as_bytes()
        .ok_or_else(|| EpError::Parse(ParseError::Custom("expected Redis command name to be a string".to_string())))?;
    let second_bytes = second.and_then(|arg| arg.as_bytes());
    let (command, words_consumed) = redis_command_from_bytes(first_bytes, second_bytes)?;

    let args_offset = header_end + 2 + first_len + if words_consumed == 2 { second_len } else { 0 };
    let first_arg_payload_range = match words_consumed {
        1 => second.and_then(|arg| arg_payload_range(second_start, arg)),
        2 => third.and_then(|arg| arg_payload_range(third_start, arg)),
        _ => None,
    };
    let has_ttl = matches!(command, RedisApi::Setex | RedisApi::Psetex) || (matches!(command, RedisApi::Set) && set_has_ttl);
    let meta = RedisCommandViewMeta::new_with_hints(
        command,
        args_offset,
        element_count.saturating_sub(words_consumed),
        first_arg_payload_range,
        has_ttl,
    );
    Ok(Some((meta, offset)))
}

/// Parse only the Redis command kind and frame boundary.
///
/// This is the narrow form used by direct gateway admission paths that only
/// need to know whether a command can use a shared backend lane. It preserves
/// [`parse_command_view_meta`] completeness semantics, but avoids collecting
/// key ranges and TTL hints that those paths do not use.
#[inline]
pub fn parse_command_kind(buffer: &[u8]) -> ResultEP<Option<(RedisApi, usize)>> {
    if buffer.is_empty() {
        return Ok(None);
    }
    if buffer.first() != Some(&b'*') {
        return Err(EpError::Parse(ParseError::Custom("expected array frame for Redis command".to_string())));
    }

    let Some(header_end) = find_crlf(buffer) else {
        return Ok(None);
    };
    let element_count = parse_array_element_count(&buffer[1..header_end])?;
    let mut offset = header_end + 2;

    let Some((first_bytes, consumed)) = scan_string_arg_at(buffer, offset) else {
        return Ok(None);
    };
    let first_bytes =
        first_bytes.ok_or_else(|| EpError::Parse(ParseError::Custom("expected Redis command name to be a string".to_string())))?;
    offset += consumed;

    let mut second_bytes = None;
    if element_count > 1 {
        let Some((payload, consumed)) = scan_string_arg_at(buffer, offset) else {
            return Ok(None);
        };
        second_bytes = payload;
        offset += consumed;
    }

    let command = redis_command_from_bytes(first_bytes, second_bytes).map(|(command, _words_consumed)| command);

    for _ in 2..element_count {
        let Some(consumed) = scan_arg_len_at(buffer, offset) else {
            return Ok(None);
        };
        offset += consumed;
    }

    Ok(Some((command?, offset)))
}

/// Parse a single complete command frame, requiring the frame to
/// fill `frame` exactly. Errors on incomplete or trailing-byte input;
/// use [`parse_command_view_meta`] for the streaming/buffered case.
pub fn parse_command_view_meta_from_frame(frame: &[u8]) -> ResultEP<RedisCommandViewMeta> {
    let Some((meta, consumed)) = parse_command_view_meta(frame)? else {
        return Err(EpError::Parse(ParseError::Custom("incomplete Redis command frame".to_string())));
    };
    if consumed != frame.len() {
        return Err(EpError::Parse(ParseError::Custom("Redis command frame contains trailing bytes".to_string())));
    }
    Ok(meta)
}

/// Parse and bind in one step: produce a [`RedisCommandView`] borrowing
/// from `buffer`. Equivalent to [`parse_command_view_meta`] followed
/// by [`RedisCommandViewMeta::bind`], for callers that don't need to
/// retain the meta independently of the buffer.
pub fn parse_command_view<'a>(buffer: &'a [u8]) -> ResultEP<Option<(RedisCommandView<'a>, usize)>> {
    let Some((meta, consumed)) = parse_command_view_meta(buffer)? else {
        return Ok(None);
    };
    Ok(Some((RedisCommandView::new_owned(meta, &buffer[..consumed]), consumed)))
}

fn parse_array_element_count(raw_count: &[u8]) -> ResultEP<usize> {
    let count = parse_resp_i64(raw_count).ok_or_else(|| EpError::Parse(ParseError::Custom("invalid Redis array header".to_string())))?;

    if count <= 0 {
        return Err(EpError::Parse(ParseError::Custom("empty Redis command array".to_string())));
    }

    Ok(count as usize)
}

fn scan_arg_at(frame: &[u8], offset: usize) -> Option<RedisArgView<'_>> {
    if offset >= frame.len() {
        return None;
    }
    let arg = &frame[offset..];
    let (frame_type, consumed) = scan_arg_header(arg)?;
    Some(RedisArgView { raw: &frame[offset..offset + consumed], frame_type })
}

#[inline]
fn scan_arg_len_at(frame: &[u8], offset: usize) -> Option<usize> {
    if offset >= frame.len() {
        return None;
    }
    scan_arg_header(&frame[offset..]).map(|(_frame_type, consumed)| consumed)
}

#[inline]
fn scan_arg_header(arg: &[u8]) -> Option<(FrameType, usize)> {
    match arg.first().copied()? {
        b'$' => Some((FrameType::BulkString, scan_bulk_string_len(arg)?)),
        b'+' => Some((FrameType::SimpleString, scan_simple_line_len(arg)?)),
        b':' => Some((FrameType::Integer, scan_simple_line_len(arg)?)),
        _ => scanner::scan_frame_boundary(arg),
    }
}

#[inline]
fn scan_string_arg_at(frame: &[u8], offset: usize) -> Option<(Option<&[u8]>, usize)> {
    if offset >= frame.len() {
        return None;
    }
    let arg = &frame[offset..];
    match arg.first().copied()? {
        b'$' => {
            let header_end = find_crlf(arg)?;
            let len = parse_resp_i64(&arg[1..header_end])?;
            if len < 0 {
                return Some((None, header_end + 2));
            }
            let data_start = header_end + 2;
            let data_end = data_start + len as usize;
            if data_end + 2 > arg.len() {
                return None;
            }
            Some((arg.get(data_start..data_end), data_end + 2))
        }
        b'+' => {
            let end = find_crlf(arg)?;
            Some((arg.get(1..end), end + 2))
        }
        _ => {
            let (_frame_type, consumed) = scan_arg_header(arg)?;
            Some((None, consumed))
        }
    }
}

fn arg_payload_range(start: usize, arg: RedisArgView<'_>) -> Option<(usize, usize)> {
    match arg.frame_type {
        FrameType::SimpleString => {
            let end = find_crlf(arg.raw)?;
            Some((start + 1, start + end))
        }
        FrameType::BulkString => {
            let header_end = find_crlf(arg.raw)?;
            let len = parse_resp_i64(&arg.raw[1..header_end])?;
            if len < 0 {
                return None;
            }
            let data_start = start + header_end + 2;
            let data_end = data_start + len as usize;
            Some((data_start, data_end))
        }
        _ => None,
    }
}

fn is_set_ttl_option(arg: &[u8]) -> bool {
    arg.eq_ignore_ascii_case(b"EX")
        || arg.eq_ignore_ascii_case(b"PX")
        || arg.eq_ignore_ascii_case(b"EXAT")
        || arg.eq_ignore_ascii_case(b"PXAT")
        || arg.eq_ignore_ascii_case(b"KEEPTTL")
}

fn simple_line_payload(frame: &[u8]) -> Option<&[u8]> {
    let end = find_crlf(frame)?;
    frame.get(1..end)
}

fn bulk_payload(frame: &[u8]) -> Option<&[u8]> {
    let header_end = find_crlf(frame)?;
    let len = parse_resp_i64(&frame[1..header_end])?;
    if len < 0 {
        return None;
    }
    let len = len as usize;
    let data_start = header_end + 2;
    let data_end = data_start + len;
    if data_end + 2 > frame.len() {
        return None;
    }
    frame.get(data_start..data_end)
}

fn scan_simple_line_len(frame: &[u8]) -> Option<usize> {
    find_crlf(frame).map(|pos| pos + 2)
}

fn scan_bulk_string_len(frame: &[u8]) -> Option<usize> {
    let header_end = find_crlf(frame)?;
    let len = parse_resp_i64(&frame[1..header_end])?;
    if len < 0 {
        return Some(header_end + 2);
    }
    let frame_end = header_end + 2 + len as usize + 2;
    if frame.len() < frame_end { None } else { Some(frame_end) }
}

fn parse_resp_i64(raw: &[u8]) -> Option<i64> {
    if raw.is_empty() {
        return None;
    }

    let mut value = 0i64;
    let mut index = 0usize;
    let negative = raw[0] == b'-';
    if negative {
        if raw.len() == 1 {
            return None;
        }
        index = 1;
    }

    while index < raw.len() {
        let digit = raw[index].wrapping_sub(b'0');
        if digit > 9 {
            return None;
        }
        value = value.checked_mul(10)?.checked_add(digit as i64)?;
        index += 1;
    }

    Some(if negative { -value } else { value })
}

fn redis_command_from_bytes(command: &[u8], subcommand: Option<&[u8]>) -> Result<(RedisApi, usize), EpError> {
    RedisApi::try_from_command_words_bytes(command, subcommand)
}

fn find_crlf(frame: &[u8]) -> Option<usize> {
    let cr = memchr::memchr(b'\r', frame)?;
    if frame.get(cr + 1) == Some(&b'\n') { Some(cr) } else { None }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_basic_command_view() {
        let command = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let (view, consumed) = parse_command_view(command).expect("parse should succeed").expect("command should be complete");
        assert_eq!(consumed, command.len());
        assert_eq!(view.command(), &RedisApi::Set);
        assert_eq!(view.args_count(), 2);
        assert_eq!(view.first_arg().and_then(|arg| arg.as_str()), Some("key"));
    }

    #[test]
    fn parses_two_word_command_view() {
        let command = b"*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$4\r\nrust\r\n";
        let (view, _) = parse_command_view(command).expect("parse should succeed").expect("command should be complete");
        assert_eq!(view.command(), &RedisApi::ClientSetinfo);
        assert_eq!(view.args_count(), 2);
        assert_eq!(view.arg(1).and_then(|arg| arg.as_str()), Some("rust"));
    }

    #[test]
    fn parses_command_kind_without_full_metadata() {
        let command = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let (api, consumed) = parse_command_kind(command).expect("parse should succeed").expect("command should be complete");
        assert_eq!(api, RedisApi::Set);
        assert_eq!(consumed, command.len());
    }

    #[test]
    fn parses_command_kind_for_two_word_commands() {
        let command = b"*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$4\r\nrust\r\n";
        let (api, consumed) = parse_command_kind(command).expect("parse should succeed").expect("command should be complete");
        assert_eq!(api, RedisApi::ClientSetinfo);
        assert_eq!(consumed, command.len());
    }

    #[test]
    fn command_kind_waits_for_complete_unknown_command_frame() {
        let partial = b"*2\r\n$7\r\nUNKNOWN\r\n$5\r\nvalue";
        assert!(parse_command_kind(partial).expect("incomplete frame should not error").is_none());

        let complete = b"*2\r\n$7\r\nUNKNOWN\r\n$5\r\nvalue\r\n";
        assert!(parse_command_kind(complete).is_err());
    }

    #[test]
    fn converts_view_back_to_owned_args() {
        let command = b"*2\r\n$3\r\nGET\r\n$3\r\nkey\r\n";
        let (view, _) = parse_command_view(command).expect("parse should succeed").expect("command should be complete");
        let owned = view.to_owned_args().expect("view should convert back to owned args");
        assert_eq!(owned.command(), &RedisApi::Get);
        assert_eq!(owned.args().len(), 1);
        assert!(matches!(&owned.args()[0], RedisJsonValue::String(value) if value == "key"));
    }

    #[test]
    fn command_meta_captures_first_arg_payload() {
        let command = b"*3\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n";
        let (meta, consumed) = parse_command_view_meta(command).expect("parse should succeed").expect("command should be complete");
        assert_eq!(consumed, command.len());
        assert_eq!(meta.first_arg_payload(command), Some(&b"key"[..]));
    }

    #[test]
    fn command_meta_captures_two_word_first_arg_payload() {
        let command = b"*4\r\n$6\r\nCLIENT\r\n$7\r\nSETINFO\r\n$8\r\nLIB-NAME\r\n$4\r\nrust\r\n";
        let (meta, consumed) = parse_command_view_meta(command).expect("parse should succeed").expect("command should be complete");
        assert_eq!(consumed, command.len());
        assert_eq!(meta.first_arg_payload(command), Some(&b"LIB-NAME"[..]));
    }

    #[test]
    fn command_meta_captures_set_ttl_flag() {
        let command = b"*5\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nEX\r\n$2\r\n60\r\n";
        let (meta, _) = parse_command_view_meta(command).expect("parse should succeed").expect("command should be complete");
        assert!(meta.has_ttl_flag());
    }

    #[test]
    fn command_meta_does_not_mark_set_without_ttl() {
        let command = b"*4\r\n$3\r\nSET\r\n$3\r\nkey\r\n$5\r\nvalue\r\n$2\r\nNX\r\n";
        let (meta, _) = parse_command_view_meta(command).expect("parse should succeed").expect("command should be complete");
        assert!(!meta.has_ttl_flag());
    }
}
