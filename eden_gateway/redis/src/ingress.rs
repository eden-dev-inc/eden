//! Pre-parsed Redis ingress batches shipped from the bridge read side to
//! the redis processor.
//!
//! The bridge owns the persistent parse buffer and runs
//! `RedisProtocol::parse_command_view_meta` against bytes as they arrive
//! from the client socket. Each `RedisIngressBatch` carries the contiguous
//! `Bytes` slice that was consumed from the buffer plus the
//! `RedisCommandViewMeta` for every frame inside it, in order. The processor
//! consumes batches directly without re-parsing, so parse and materialize
//! work overlaps with subsequent socket reads instead of running as a
//! single block after chunk delivery.

use bytes::Bytes;
use eden_core::error::EpError;
use endpoints::endpoint::ep_redis::protocol::RedisCommandViewMeta;
use std::time::Instant;

#[derive(Debug)]
pub struct RedisIngressBatch {
    batch_bytes: Bytes,
    commands: Vec<(RedisCommandViewMeta, usize)>,
    parse_error: Option<EpError>,
    enqueued_at: Instant,
    /// Wall clock at which the bridge first observed this batch's bytes from
    /// the client socket (stamped right after `read_buf` returns, before the
    /// bridge runs the parse loop). Threaded through to the response
    /// channel so the bridge can compute end-to-end proxy-induced latency
    /// on write completion.
    received_at: Instant,
}

impl RedisIngressBatch {
    pub fn new(
        batch_bytes: Bytes,
        commands: Vec<(RedisCommandViewMeta, usize)>,
        parse_error: Option<EpError>,
        received_at: Instant,
    ) -> Self {
        Self {
            batch_bytes,
            commands,
            parse_error,
            enqueued_at: Instant::now(),
            received_at,
        }
    }

    pub fn parse_error_only(parse_error: EpError, received_at: Instant) -> Self {
        Self {
            batch_bytes: Bytes::new(),
            commands: Vec::new(),
            parse_error: Some(parse_error),
            enqueued_at: Instant::now(),
            received_at,
        }
    }

    pub fn batch_bytes(&self) -> &Bytes {
        &self.batch_bytes
    }

    pub fn commands(&self) -> &[(RedisCommandViewMeta, usize)] {
        &self.commands
    }

    pub fn parse_error(&self) -> Option<&EpError> {
        self.parse_error.as_ref()
    }

    pub fn len_bytes(&self) -> usize {
        self.batch_bytes.len()
    }

    pub fn command_count(&self) -> usize {
        self.commands.len()
    }

    pub fn queue_wait_us(&self) -> u64 {
        self.enqueued_at.elapsed().as_micros().min(u64::MAX as u128) as u64
    }

    pub fn received_at(&self) -> Instant {
        self.received_at
    }

    pub fn into_parts(self) -> (Bytes, Vec<(RedisCommandViewMeta, usize)>, Option<EpError>, Instant) {
        (self.batch_bytes, self.commands, self.parse_error, self.received_at)
    }
}
