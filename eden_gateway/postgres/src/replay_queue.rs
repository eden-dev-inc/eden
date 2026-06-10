//! Background retry queue for failed dual-write secondary operations (DW-2).
//!
//! When a dual-write succeeds on the authoritative side but fails on the
//! secondary, the failed write is enqueued here for background retry with
//! exponential backoff. After [`DEFAULT_MAX_ATTEMPTS`] retries the entry is moved
//! to the dead-letter log.
//!
//! # Hardening (Round 2)
//!
//! - **Bounded queue** — prevents OOM under sustained secondary failure.
//! - **Response error inspection** — catches SQL-level errors (constraint
//!   violations, FK errors) in otherwise-successful TCP exchanges.
//! - **Transaction batch replay** — wraps multi-write transaction buffers in
//!   BEGIN/COMMIT for atomicity on the secondary.
//! - **Graceful drain** — logs remaining entries on connection close instead
//!   of silently dropping them.

use bytes::Bytes;
use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_logger_internal::{LogAudience, LogContext, log_error, log_warn};
use endpoints::endpoint::postgres::ep::PostgresEp;
use ep_core::ReqType;
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, Instant};
use tokio::sync::Mutex;

/// Default maximum number of retry attempts per entry.
const DEFAULT_MAX_ATTEMPTS: u32 = 10;

/// Initial backoff delay between retries.
const INITIAL_BACKOFF: Duration = Duration::from_millis(100);

/// Maximum backoff delay between retries.
const MAX_BACKOFF: Duration = Duration::from_secs(5);

/// How often the background worker polls the queue.
const POLL_INTERVAL: Duration = Duration::from_millis(50);

/// Maximum number of entries in the queue. New entries are dead-lettered
/// when this limit is reached, preventing unbounded memory growth under
/// sustained secondary failure.
const MAX_QUEUE_SIZE: usize = 1000;

/// The payload to replay — either a single write or a transaction batch.
pub enum ReplayPayload {
    /// A single write command.
    Single(Bytes),
    /// Multiple writes that must be replayed atomically inside a transaction.
    /// Session commands (SET/RESET) are prepended before the writes.
    TransactionBatch { session_commands: Vec<Bytes>, writes: Vec<Bytes> },
}

/// A single failed write awaiting retry.
pub struct ReplayEntry {
    /// The replay payload (single command or transaction batch).
    payload: ReplayPayload,
    /// Endpoint to send the retry to.
    target_endpoint: EndpointCacheUuid,
    /// Table name (if extractable) for observability.
    pub table_name: Option<String>,
    /// Number of attempts so far (starts at 0).
    attempt_count: u32,
    /// When to next attempt this entry.
    next_retry: Instant,
    /// When the entry was first created.
    created_at: Instant,
}

impl ReplayEntry {
    /// Create a new single-write replay entry with initial backoff.
    pub fn new(pg_bytes: Bytes, target_endpoint: EndpointCacheUuid, table_name: Option<String>) -> Self {
        Self {
            payload: ReplayPayload::Single(pg_bytes),
            target_endpoint,
            table_name,
            attempt_count: 0,
            next_retry: Instant::now() + INITIAL_BACKOFF,
            created_at: Instant::now(),
        }
    }

    /// Create a transaction batch replay entry (DW-25).
    ///
    /// Session commands and writes are replayed atomically inside a
    /// BEGIN/COMMIT block on a single connection.
    pub fn new_transaction_batch(session_commands: Vec<Bytes>, writes: Vec<Bytes>, target_endpoint: EndpointCacheUuid) -> Self {
        Self {
            payload: ReplayPayload::TransactionBatch { session_commands, writes },
            target_endpoint,
            table_name: None,
            attempt_count: 0,
            next_retry: Instant::now() + INITIAL_BACKOFF,
            created_at: Instant::now(),
        }
    }
}

/// In-memory queue with background retry worker.
pub struct ReplayQueue {
    entries: Arc<Mutex<VecDeque<ReplayEntry>>>,
    max_attempts: u32,
    dead_letter_count: Arc<AtomicU64>,
    replay_success_count: Arc<AtomicU64>,
    queue_depth: Arc<AtomicU64>,
}

impl Default for ReplayQueue {
    fn default() -> Self {
        Self::new()
    }
}

impl ReplayQueue {
    /// Create a new replay queue.
    pub fn new() -> Self {
        Self {
            entries: Arc::new(Mutex::new(VecDeque::new())),
            max_attempts: DEFAULT_MAX_ATTEMPTS,
            dead_letter_count: Arc::new(AtomicU64::new(0)),
            replay_success_count: Arc::new(AtomicU64::new(0)),
            queue_depth: Arc::new(AtomicU64::new(0)),
        }
    }

    /// Add a failed write to the queue.
    ///
    /// If the queue is at capacity, the entry is immediately dead-lettered
    /// to prevent unbounded memory growth.
    pub async fn enqueue(&self, entry: ReplayEntry, ctx: &LogContext) {
        let mut q = self.entries.lock().await;
        if q.len() >= MAX_QUEUE_SIZE {
            self.dead_letter_count.fetch_add(1, Ordering::Relaxed);
            log_error!(
                ctx.clone(),
                "DW-2 replay queue full: entry dead-lettered immediately",
                audience = LogAudience::Internal,
                queue_size = MAX_QUEUE_SIZE,
                table = entry.table_name.as_deref().unwrap_or("unknown")
            );
            return;
        }
        q.push_back(entry);
        self.queue_depth.store(q.len() as u64, Ordering::Relaxed);
    }

    /// Current number of entries awaiting retry.
    pub fn queue_depth(&self) -> u64 {
        self.queue_depth.load(Ordering::Relaxed)
    }

    /// Shared counter for queue depth — used by session affinity (DW-5/SA-1).
    pub fn queue_depth_counter(&self) -> Arc<AtomicU64> {
        Arc::clone(&self.queue_depth)
    }

    /// Number of entries that exhausted all retries.
    pub fn dead_letter_count(&self) -> u64 {
        self.dead_letter_count.load(Ordering::Relaxed)
    }

    /// Number of entries successfully replayed.
    pub fn replay_success_count(&self) -> u64 {
        self.replay_success_count.load(Ordering::Relaxed)
    }

    /// Drain all remaining entries from the queue (DW-17).
    ///
    /// Called before aborting the background worker on connection close.
    /// Returns the entries so the caller can log them.
    pub async fn drain_remaining(&self) -> Vec<ReplayEntry> {
        let mut q = self.entries.lock().await;
        let entries: Vec<ReplayEntry> = q.drain(..).collect();
        self.queue_depth.store(0, Ordering::Relaxed);
        entries
    }

    /// Spawn a background worker that retries queued entries.
    ///
    /// The worker runs until the returned [`tokio::task::JoinHandle`] is
    /// dropped or aborted. Typically tied to the proxy connection lifetime.
    pub fn start_background_worker(self: &Arc<Self>, ep: PostgresEp, ctx: LogContext) -> tokio::task::JoinHandle<()> {
        let queue = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(POLL_INTERVAL).await;
                queue.process_ready_entries(&ep, &ctx).await;
            }
        })
    }

    /// Process all entries whose retry time has arrived.
    async fn process_ready_entries(&self, ep: &PostgresEp, ctx: &LogContext) {
        let now = Instant::now();
        let mut ready = Vec::new();

        // Collect entries that are ready for retry.
        {
            let mut q = self.entries.lock().await;
            let mut i = 0;
            while i < q.len() {
                if q[i].next_retry <= now {
                    if let Some(entry) = q.remove(i) {
                        ready.push(entry);
                    }
                } else {
                    i += 1;
                }
            }
            self.queue_depth.store(q.len() as u64, Ordering::Relaxed);
        }

        for mut entry in ready {
            let result = self.replay_entry(ep, &entry).await;

            match result {
                Ok(_) => {
                    self.replay_success_count.fetch_add(1, Ordering::Relaxed);
                }
                Err(e) => {
                    entry.attempt_count += 1;
                    if entry.attempt_count >= self.max_attempts {
                        self.dead_letter_count.fetch_add(1, Ordering::Relaxed);
                        log_error!(
                            ctx.clone(),
                            "DW-2 replay exhausted: write dead-lettered",
                            audience = LogAudience::Internal,
                            max_attempts = self.max_attempts,
                            age_ms = entry.created_at.elapsed().as_millis() as u64,
                            table = entry.table_name.as_deref().unwrap_or("unknown"),
                            error = e.to_string()
                        );
                    } else {
                        // Exponential backoff: 100ms, 200ms, 400ms, ... capped at MAX_BACKOFF.
                        let backoff = INITIAL_BACKOFF * 2u32.saturating_pow(entry.attempt_count);
                        let backoff = backoff.min(MAX_BACKOFF);
                        entry.next_retry = Instant::now() + backoff;

                        log_warn!(
                            ctx.clone(),
                            "DW-2 replay retry: rescheduled",
                            audience = LogAudience::Internal,
                            attempt = entry.attempt_count,
                            max_attempts = self.max_attempts,
                            backoff_ms = backoff.as_millis() as u64,
                            table = entry.table_name.as_deref().unwrap_or("unknown")
                        );

                        let mut q = self.entries.lock().await;
                        q.push_back(entry);
                        self.queue_depth.store(q.len() as u64, Ordering::Relaxed);
                    }
                }
            }
        }
    }

    /// Execute a single replay entry against the target endpoint.
    ///
    /// For `TransactionBatch` payloads, sends BEGIN + session commands +
    /// writes + COMMIT on a single connection. If any step fails, sends
    /// ROLLBACK and returns the error.
    async fn replay_entry(&self, ep: &PostgresEp, entry: &ReplayEntry) -> Result<(), eden_core::error::EpError> {
        let mut conn = ep.raw_connection(&entry.target_endpoint, ReqType::Write).await?;

        match &entry.payload {
            ReplayPayload::Single(pg_bytes) => {
                let (response, _) = conn.send_query_raw(pg_bytes).await?;
                if response_has_error(&response) {
                    return Err(eden_core::error::EpError::request("replay response contained ErrorResponse"));
                }
                Ok(())
            }
            ReplayPayload::TransactionBatch { session_commands, writes } => {
                // Send BEGIN
                let begin_msg = postgres_core::client::build_query_message("BEGIN");
                let (resp, _) = conn.send_query_raw(&begin_msg).await?;
                if response_has_error(&resp) {
                    return Err(eden_core::error::EpError::request("replay BEGIN failed"));
                }

                // Send session commands (SET/RESET/DISCARD) for context
                for session_cmd in session_commands {
                    let (resp, _) = conn.send_query_raw(session_cmd).await?;
                    if response_has_error(&resp) {
                        // Session command failed — ROLLBACK and retry
                        let rollback_msg = postgres_core::client::build_query_message("ROLLBACK");
                        let _ = conn.send_query_raw(&rollback_msg).await;
                        return Err(eden_core::error::EpError::request("replay session command failed"));
                    }
                }

                // Send each write in order
                for write_bytes in writes {
                    let (resp, _) = conn.send_query_raw(write_bytes).await?;
                    if response_has_error(&resp) {
                        // Write failed — ROLLBACK and retry
                        let rollback_msg = postgres_core::client::build_query_message("ROLLBACK");
                        let _ = conn.send_query_raw(&rollback_msg).await;
                        return Err(eden_core::error::EpError::request("replay write failed inside transaction"));
                    }
                }

                // COMMIT
                let commit_msg = postgres_core::client::build_query_message("COMMIT");
                let (resp, _) = conn.send_query_raw(&commit_msg).await?;
                if response_has_error(&resp) {
                    return Err(eden_core::error::EpError::request("replay COMMIT failed"));
                }

                Ok(())
            }
        }
    }
}

/// Check if a raw PG wire response contains an ErrorResponse message.
///
/// Scans the response bytes for a message of type `b'E'` (ErrorResponse).
/// Used to detect SQL-level errors (constraint violations, FK errors, etc.)
/// in otherwise-successful TCP exchanges.
pub(crate) fn response_has_error(response: &[u8]) -> bool {
    let mut pos = 0;
    while pos < response.len() {
        if response.len() < pos + 5 {
            break;
        }
        let msg_type = response[pos];
        let length = i32::from_be_bytes([response[pos + 1], response[pos + 2], response[pos + 3], response[pos + 4]]);
        // PG wire protocol length includes itself (4 bytes minimum).
        // Negative or too-small values indicate a malformed response.
        if length < 4 {
            break;
        }
        let total = 1 + length as usize;

        if msg_type == b'E' {
            return true;
        }

        pos += total;
    }
    false
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_response_has_error_empty() {
        assert!(!response_has_error(&[]));
    }

    #[test]
    fn test_response_has_error_with_error() {
        // Build a minimal ErrorResponse: type 'E' + length(4 + 1) + severity byte + null
        let mut resp = Vec::new();
        resp.push(b'E');
        let len: i32 = 4 + 1; // self-length + null byte
        resp.extend_from_slice(&len.to_be_bytes());
        resp.push(0); // null terminator
        assert!(response_has_error(&resp));
    }

    #[test]
    fn test_response_has_error_without_error() {
        // Build a minimal ReadyForQuery: type 'Z' + length(5) + status byte
        let mut resp = Vec::new();
        resp.push(b'Z');
        let len: i32 = 5;
        resp.extend_from_slice(&len.to_be_bytes());
        resp.push(b'I');
        assert!(!response_has_error(&resp));
    }

    #[test]
    fn test_response_has_error_error_after_data() {
        // Build CommandComplete + ErrorResponse
        let mut resp = Vec::new();
        // CommandComplete: type 'C' + length(4 + 7) + "INSERT\0"
        resp.push(b'C');
        let len: i32 = 4 + 7;
        resp.extend_from_slice(&len.to_be_bytes());
        resp.extend_from_slice(b"INSERT\0");
        // ErrorResponse
        resp.push(b'E');
        let len2: i32 = 4 + 1;
        resp.extend_from_slice(&len2.to_be_bytes());
        resp.push(0);
        assert!(response_has_error(&resp));
    }

    #[test]
    fn test_response_has_error_malformed_negative_length() {
        // Malformed message with negative length — should bail out safely.
        let mut resp = Vec::new();
        resp.push(b'C'); // CommandComplete type
        resp.extend_from_slice(&(-1_i32).to_be_bytes()); // negative length
        assert!(!response_has_error(&resp));
    }

    #[test]
    fn test_response_has_error_malformed_zero_length() {
        // Malformed message with zero length (below minimum 4).
        let mut resp = Vec::new();
        resp.push(b'C');
        resp.extend_from_slice(&0_i32.to_be_bytes());
        assert!(!response_has_error(&resp));
    }
}
