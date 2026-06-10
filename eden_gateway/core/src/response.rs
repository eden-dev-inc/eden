//! Shared response-flow primitives for gateway protocol adapters.
//!
//! Protocol crates still own frame parsing and policy decisions. This module
//! captures the endpoint-neutral hot-path shape: observe chunks, count bytes,
//! enqueue bridge responses, and mark mirror paths that can drain without
//! keeping successful response bodies.
//!
//! New gateway endpoints should keep frame readers in their endpoint-core
//! crate, then use these primitives in the gateway layer:
//! - implement [`GatewayResponseProfile`] on the protocol processor so the
//!   gateway declares its observer and mirror response mode at compile time;
//! - forward complete frames/chunks through [`ObservedResponse`] when the
//!   client should receive bytes immediately;
//! - implement [`WireResponseObserver`] for protocol-specific inspection;
//! - choose [`GatewayMirrorResponseMode::DrainOnly`] for mirrors that only
//!   need latency/error accounting;
//! - use [`GatewayQueueResponseSender`] for bridge responses that need the
//!   original request timestamp preserved.

use bytes::{Bytes, BytesMut};
use eden_logger_internal::{LogAudience, LogContext, log_error};
use std::future::poll_fn;
use std::io::IoSlice;
use std::mem;
use std::pin::Pin;
use std::time::Instant;
use tokio::io::{self, AsyncWrite, AsyncWriteExt};

use crate::traits::BytesQueueSender;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct GatewayResponsePolicySpec {
    protocol: &'static str,
    mirror_response_mode: Option<GatewayMirrorResponseMode>,
}

impl GatewayResponsePolicySpec {
    pub const fn new(protocol: &'static str, mirror_response_mode: Option<GatewayMirrorResponseMode>) -> Self {
        Self { protocol, mirror_response_mode }
    }

    #[inline]
    pub fn protocol(&self) -> &'static str {
        self.protocol
    }

    #[inline]
    pub fn mirror_response_mode(&self) -> Option<GatewayMirrorResponseMode> {
        self.mirror_response_mode
    }

    #[inline]
    pub fn needs_mirror_success_bytes(&self) -> bool {
        self.mirror_response_mode.is_some_and(GatewayMirrorResponseMode::needs_success_bytes)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum GatewayMirrorResponseMode {
    /// Keep successful mirror response bytes so the caller can compare them
    /// against the primary response.
    CompareResponse,
    /// Drain the mirror response for ordering/latency/error accounting, but
    /// do not materialize successful response bytes.
    DrainOnly,
}

impl GatewayMirrorResponseMode {
    pub fn needs_success_bytes(self) -> bool {
        matches!(self, Self::CompareResponse)
    }
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub struct GatewayResponseStats {
    chunks: usize,
    bytes: u64,
}

impl GatewayResponseStats {
    #[inline]
    pub fn chunks(&self) -> usize {
        self.chunks
    }

    #[inline]
    pub fn bytes(&self) -> u64 {
        self.bytes
    }

    #[inline]
    fn record_chunk(&mut self, len: usize) {
        self.chunks = self.chunks.saturating_add(1);
        self.bytes = self.bytes.saturating_add(len as u64);
    }
}

/// Protocol-specific response observer called once per forwarded response
/// chunk/frame. Implementations should be cheap and allocation-free.
pub trait WireResponseObserver {
    fn observe(&mut self, bytes: &[u8]);
}

impl WireResponseObserver for () {
    #[inline]
    fn observe(&mut self, _bytes: &[u8]) {}
}

pub trait GatewayResponsePolicy {
    fn response_policy_spec(&self) -> GatewayResponsePolicySpec;

    fn new_response_observer_box(&self) -> Box<dyn WireResponseObserver + Send>;
}

pub trait GatewayResponseProfile: Send + Sync + 'static {
    type Observer: WireResponseObserver + Default + Send + 'static;

    fn response_policy_spec(&self) -> GatewayResponsePolicySpec;

    #[inline]
    fn new_response_observer(&self) -> Self::Observer {
        Self::Observer::default()
    }

    #[inline]
    fn observed_response(&self) -> ObservedResponse<Self::Observer> {
        ObservedResponse::new(self.new_response_observer())
    }
}

impl<T> GatewayResponsePolicy for T
where
    T: GatewayResponseProfile,
{
    #[inline]
    fn response_policy_spec(&self) -> GatewayResponsePolicySpec {
        GatewayResponseProfile::response_policy_spec(self)
    }

    #[inline]
    fn new_response_observer_box(&self) -> Box<dyn WireResponseObserver + Send> {
        Box::new(self.new_response_observer())
    }
}

#[derive(Debug, Clone)]
pub struct ObservedResponse<O> {
    observer: O,
    stats: GatewayResponseStats,
}

impl<O> ObservedResponse<O> {
    pub fn new(observer: O) -> Self {
        Self { observer, stats: GatewayResponseStats::default() }
    }

    #[inline]
    pub fn stats(&self) -> GatewayResponseStats {
        self.stats
    }

    #[inline]
    pub fn observer(&self) -> &O {
        &self.observer
    }

    pub fn into_parts(self) -> (O, GatewayResponseStats) {
        (self.observer, self.stats)
    }
}

impl<O: WireResponseObserver> ObservedResponse<O> {
    #[inline]
    pub fn observe_chunk(&mut self, bytes: &[u8]) {
        self.stats.record_chunk(bytes.len());
        self.observer.observe(bytes);
    }
}

pub async fn write_observed_response_chunk<W, O>(writer: &mut W, chunk: Bytes, observed: &mut ObservedResponse<O>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
    O: WireResponseObserver,
{
    observed.observe_chunk(&chunk);
    writer.write_all(&chunk).await
}

/// Bounded write coalescer for gateway response streams.
///
/// Protocol observers still see every original chunk, but small chunks that
/// are already available can be written to the client as one buffer. This
/// reduces syscall/wakeup pressure without changing framing semantics.
pub struct ObservedResponseWriteBuffer {
    chunks: Vec<Bytes>,
    pending_bytes: usize,
    max_bytes: usize,
    max_chunks: usize,
}

impl ObservedResponseWriteBuffer {
    pub fn with_limits(max_bytes: usize, max_chunks: usize) -> Self {
        Self {
            chunks: Vec::with_capacity(max_chunks.max(1)),
            pending_bytes: 0,
            max_bytes: max_bytes.max(1),
            max_chunks: max_chunks.max(1),
        }
    }

    #[inline]
    pub fn is_empty(&self) -> bool {
        self.chunks.is_empty()
    }

    #[inline]
    pub fn pending_bytes(&self) -> usize {
        self.pending_bytes
    }

    #[inline]
    pub fn pending_chunks(&self) -> usize {
        self.chunks.len()
    }

    pub async fn push<W, O>(&mut self, writer: &mut W, chunk: Bytes, observed: &mut ObservedResponse<O>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        O: WireResponseObserver,
    {
        if chunk.len() >= self.max_bytes {
            self.flush(writer, observed).await?;
            observed.observe_chunk(&chunk);
            return writer.write_all(&chunk).await;
        }

        let would_exceed_bytes = self.pending_bytes.saturating_add(chunk.len()) > self.max_bytes;
        if would_exceed_bytes || self.chunks.len() >= self.max_chunks {
            self.flush(writer, observed).await?;
        }

        self.pending_bytes = self.pending_bytes.saturating_add(chunk.len());
        self.chunks.push(chunk);

        if self.pending_bytes >= self.max_bytes || self.chunks.len() >= self.max_chunks {
            self.flush(writer, observed).await?;
        }

        Ok(())
    }

    pub async fn flush<W, O>(&mut self, writer: &mut W, observed: &mut ObservedResponse<O>) -> io::Result<()>
    where
        W: AsyncWrite + Unpin,
        O: WireResponseObserver,
    {
        match self.chunks.len() {
            0 => Ok(()),
            1 => {
                self.pending_bytes = 0;
                if let Some(chunk) = self.chunks.pop() {
                    observed.observe_chunk(&chunk);
                    writer.write_all(&chunk).await
                } else {
                    Ok(())
                }
            }
            _ => {
                let pending_bytes = mem::take(&mut self.pending_bytes);
                let chunks = mem::take(&mut self.chunks);
                for chunk in &chunks {
                    observed.observe_chunk(chunk);
                }

                if writer.is_write_vectored() {
                    write_all_vectored_chunks(writer, &chunks).await
                } else {
                    write_all_combined_chunks(writer, pending_bytes, chunks).await
                }
            }
        }
    }
}

async fn write_all_combined_chunks<W>(writer: &mut W, pending_bytes: usize, chunks: Vec<Bytes>) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut combined = BytesMut::with_capacity(pending_bytes);
    for chunk in chunks {
        combined.extend_from_slice(&chunk);
    }
    writer.write_all(&combined).await
}

const MAX_VECTORED_CHUNKS_PER_WRITE: usize = 16;

pub async fn write_all_vectored_chunks<W>(writer: &mut W, chunks: &[Bytes]) -> io::Result<()>
where
    W: AsyncWrite + Unpin,
{
    let mut chunk_index = 0usize;
    let mut offset = 0usize;

    while chunk_index < chunks.len() {
        while chunk_index < chunks.len() && offset >= chunks[chunk_index].len() {
            chunk_index = chunk_index.saturating_add(1);
            offset = 0;
        }
        if chunk_index >= chunks.len() {
            return Ok(());
        }

        let written = poll_fn(|cx| {
            let mut slices = [IoSlice::new(&[]); MAX_VECTORED_CHUNKS_PER_WRITE];
            let mut slice_count = 0usize;

            let first = &chunks[chunk_index][offset..];
            if !first.is_empty() {
                slices[slice_count] = IoSlice::new(first);
                slice_count = slice_count.saturating_add(1);
            }
            for chunk in &chunks[chunk_index.saturating_add(1)..] {
                if slice_count >= MAX_VECTORED_CHUNKS_PER_WRITE {
                    break;
                }
                if !chunk.is_empty() {
                    slices[slice_count] = IoSlice::new(chunk);
                    slice_count = slice_count.saturating_add(1);
                }
            }

            Pin::new(&mut *writer).poll_write_vectored(cx, &slices[..slice_count])
        })
        .await?;

        if written == 0 {
            return Err(io::ErrorKind::WriteZero.into());
        }

        let mut remaining = written;
        while remaining > 0 && chunk_index < chunks.len() {
            let available = chunks[chunk_index].len().saturating_sub(offset);
            if remaining < available {
                offset = offset.saturating_add(remaining);
                break;
            }

            remaining = remaining.saturating_sub(available);
            chunk_index = chunk_index.saturating_add(1);
            offset = 0;
        }
    }

    Ok(())
}

pub struct GatewayQueueResponseSender;

impl GatewayQueueResponseSender {
    pub fn send(sender: &BytesQueueSender, response: Bytes, request_received_at: Instant, ctx: &LogContext) -> bool {
        if let Err(err) = sender.send_with_request_received_at(response, request_received_at) {
            log_error!(
                ctx.clone(),
                "Failed to enqueue gateway response",
                audience = LogAudience::Internal,
                error = err.to_string()
            );
            return false;
        }

        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::IoSlice;
    use std::pin::Pin;
    use std::task::{Context, Poll};
    use tokio::io::AsyncWrite;

    #[derive(Default)]
    struct FindsBang {
        found: bool,
    }

    impl WireResponseObserver for FindsBang {
        fn observe(&mut self, bytes: &[u8]) {
            self.found |= bytes.contains(&b'!');
        }
    }

    #[test]
    fn observed_response_tracks_chunks_bytes_and_observer_state() {
        let mut observed = ObservedResponse::new(FindsBang::default());

        observed.observe_chunk(b"hello");
        observed.observe_chunk(b"!");

        assert_eq!(observed.stats().chunks(), 2);
        assert_eq!(observed.stats().bytes(), 6);
        assert!(observed.observer().found);
    }

    #[derive(Default)]
    struct RecordingWriter {
        writes: usize,
        bytes: Vec<u8>,
    }

    impl AsyncWrite for RecordingWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            self.writes = self.writes.saturating_add(1);
            self.bytes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[derive(Default)]
    struct VectoredRecordingWriter {
        writes: usize,
        vectored_writes: usize,
        bytes: Vec<u8>,
        max_write: Option<usize>,
    }

    impl AsyncWrite for VectoredRecordingWriter {
        fn poll_write(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
            self.writes = self.writes.saturating_add(1);
            self.bytes.extend_from_slice(buf);
            Poll::Ready(Ok(buf.len()))
        }

        fn poll_write_vectored(mut self: Pin<&mut Self>, _cx: &mut Context<'_>, bufs: &[IoSlice<'_>]) -> Poll<io::Result<usize>> {
            self.vectored_writes = self.vectored_writes.saturating_add(1);
            let max_write = self.max_write.unwrap_or(usize::MAX);
            let mut written = 0usize;
            for buf in bufs {
                if written >= max_write {
                    break;
                }

                let take = max_write.saturating_sub(written).min(buf.len());
                self.bytes.extend_from_slice(&buf[..take]);
                written = written.saturating_add(take);
            }

            Poll::Ready(Ok(written))
        }

        fn is_write_vectored(&self) -> bool {
            true
        }

        fn poll_flush(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }

        fn poll_shutdown(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<io::Result<()>> {
            Poll::Ready(Ok(()))
        }
    }

    #[tokio::test]
    async fn observed_response_write_buffer_coalesces_small_chunks() {
        let mut writer = RecordingWriter::default();
        let mut observed = ObservedResponse::new(FindsBang::default());
        let mut buffer = ObservedResponseWriteBuffer::with_limits(1024, 8);

        buffer.push(&mut writer, Bytes::from_static(b"+OK\r\n"), &mut observed).await.expect("first chunk should buffer");
        buffer.push(&mut writer, Bytes::from_static(b":1\r\n"), &mut observed).await.expect("second chunk should buffer");

        assert_eq!(writer.writes, 0);
        assert_eq!(buffer.pending_chunks(), 2);
        assert_eq!(buffer.pending_bytes(), 9);

        buffer.flush(&mut writer, &mut observed).await.expect("flush should write");

        assert_eq!(writer.writes, 1);
        assert_eq!(writer.bytes, b"+OK\r\n:1\r\n");
        assert_eq!(observed.stats().chunks(), 2);
        assert_eq!(observed.stats().bytes(), 9);
        assert!(!observed.observer().found);
    }

    #[tokio::test]
    async fn observed_response_write_buffer_uses_vectored_flush_when_supported() {
        let mut writer = VectoredRecordingWriter::default();
        let mut observed = ObservedResponse::new(FindsBang::default());
        let mut buffer = ObservedResponseWriteBuffer::with_limits(1024, 8);

        buffer.push(&mut writer, Bytes::from_static(b"+OK\r\n"), &mut observed).await.expect("first chunk should buffer");
        buffer.push(&mut writer, Bytes::from_static(b":1\r\n"), &mut observed).await.expect("second chunk should buffer");
        buffer.flush(&mut writer, &mut observed).await.expect("flush should write");

        assert_eq!(writer.writes, 0);
        assert_eq!(writer.vectored_writes, 1);
        assert_eq!(writer.bytes, b"+OK\r\n:1\r\n");
        assert_eq!(observed.stats().chunks(), 2);
        assert_eq!(observed.stats().bytes(), 9);
    }

    #[tokio::test]
    async fn observed_response_write_buffer_handles_partial_vectored_flushes() {
        let mut writer = VectoredRecordingWriter { max_write: Some(4), ..Default::default() };
        let mut observed = ObservedResponse::new(FindsBang::default());
        let mut buffer = ObservedResponseWriteBuffer::with_limits(1024, 8);

        buffer.push(&mut writer, Bytes::from_static(b"abc"), &mut observed).await.expect("first chunk should buffer");
        buffer.push(&mut writer, Bytes::from_static(b"def"), &mut observed).await.expect("second chunk should buffer");
        buffer.push(&mut writer, Bytes::from_static(b"ghi"), &mut observed).await.expect("third chunk should buffer");
        buffer.flush(&mut writer, &mut observed).await.expect("flush should write");

        assert_eq!(writer.writes, 0);
        assert!(writer.vectored_writes > 1);
        assert_eq!(writer.bytes, b"abcdefghi");
        assert_eq!(observed.stats().chunks(), 3);
        assert_eq!(observed.stats().bytes(), 9);
    }

    #[tokio::test]
    async fn observed_response_write_buffer_writes_large_chunks_directly() {
        let mut writer = RecordingWriter::default();
        let mut observed = ObservedResponse::new(FindsBang::default());
        let mut buffer = ObservedResponseWriteBuffer::with_limits(8, 8);

        buffer.push(&mut writer, Bytes::from_static(b"+OK\r\n"), &mut observed).await.expect("small chunk should buffer");
        buffer
            .push(&mut writer, Bytes::from_static(b"$8\r\npayload!\r\n"), &mut observed)
            .await
            .expect("large chunk should write directly");

        assert_eq!(writer.writes, 2);
        assert_eq!(writer.bytes, b"+OK\r\n$8\r\npayload!\r\n");
        assert!(buffer.is_empty());
        assert_eq!(observed.stats().chunks(), 2);
        assert_eq!(observed.stats().bytes(), 19);
        assert!(observed.observer().found);
    }

    #[test]
    fn mirror_response_mode_declares_whether_success_bytes_are_needed() {
        assert!(GatewayMirrorResponseMode::CompareResponse.needs_success_bytes());
        assert!(!GatewayMirrorResponseMode::DrainOnly.needs_success_bytes());
    }

    #[test]
    fn response_policy_spec_reports_mirror_byte_requirements() {
        let compare = GatewayResponsePolicySpec::new("postgres", Some(GatewayMirrorResponseMode::CompareResponse));
        let drain = GatewayResponsePolicySpec::new("redis", Some(GatewayMirrorResponseMode::DrainOnly));
        let none = GatewayResponsePolicySpec::new("llm", None);

        assert_eq!(compare.protocol(), "postgres");
        assert!(compare.needs_mirror_success_bytes());
        assert!(!drain.needs_mirror_success_bytes());
        assert!(!none.needs_mirror_success_bytes());
    }
}
