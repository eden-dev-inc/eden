//! PgBouncer-style prepared statement cache for connection multiplexing.
//!
//! Allows multiple proxy clients to share backend connections by remapping
//! prepared statement names. Each client uses its own statement names (e.g.
//! `sqlx_s_1`), while the proxy assigns globally-unique backend names
//! (e.g. `eden_s_42`) per-backend connection.
//!
//! At SYNC time, the batch is analyzed: named PARSEs are cached, named BINDs
//! and DESCRIBEs are rewritten to reference backend names, named CLOSEs are
//! intercepted (synthetic CloseComplete), and skipped PARSEs get synthetic
//! ParseComplete responses.

use bytes::{Bytes, BytesMut};
use dashmap::DashMap;
use std::collections::HashMap;
use std::sync::LazyLock;
use std::sync::atomic::{AtomicU64, Ordering};
use xxhash_rust::xxh3::Xxh3;

use crate::types::{CloseComplete, Parse, ParseComplete};

use crate::frontend as pg_scan;

// ──────────────────────────────────────────────────────────────────────────────
// Types
// ──────────────────────────────────────────────────────────────────────────────

/// Hash identifying a prepared statement by its content (SQL + param types).
type StmtHash = u64;
type FastHashMap<K, V> = HashMap<K, V, ahash::RandomState>;

/// Backend connection identity from BackendKeyData (process_id, secret_key).
pub type BackendId = (i32, i32);

/// Global counter for generating unique backend statement names.
static BACKEND_STMT_COUNTER: AtomicU64 = AtomicU64::new(0);

/// Global registry mapping backend connections to their prepared statement caches.
pub static BACKEND_CACHES: LazyLock<DashMap<BackendId, BackendStmtCache>> = LazyLock::new(DashMap::new);

// ──────────────────────────────────────────────────────────────────────────────
// Per-client state
// ──────────────────────────────────────────────────────────────────────────────

/// Information about a statement that a client has PARSEd.
#[derive(Clone, Debug)]
struct ClientStatement {
    sql: String,
    param_types: Vec<i32>,
    hash: StmtHash,
}

/// Client-side prepared statement registry.
/// Maps client statement name → statement identity.
/// One per proxy client session (local variable in processor).
#[derive(Clone)]
pub struct ClientStmtMap {
    stmts: FastHashMap<String, ClientStatement>,
}

impl Default for ClientStmtMap {
    fn default() -> Self {
        Self::new()
    }
}

impl ClientStmtMap {
    pub fn new() -> Self {
        Self { stmts: HashMap::with_hasher(ahash::RandomState::new()) }
    }

    fn insert(&mut self, name: String, sql: String, param_types: Vec<i32>) {
        let hash = compute_stmt_hash(&sql, &param_types);
        self.stmts.insert(name, ClientStatement { sql, param_types, hash });
    }

    fn remove(&mut self, name: &str) {
        self.stmts.remove(name);
    }

    fn get(&self, name: &str) -> Option<&ClientStatement> {
        self.stmts.get(name)
    }

    /// Look up the SQL text for a named prepared statement.
    ///
    /// Used by the processor to classify BIND-only batches (no PARSE in the batch)
    /// by retrieving the SQL from the original PARSE that created the statement.
    pub fn get_sql(&self, name: &str) -> Option<&str> {
        self.stmts.get(name).map(|s| s.sql.as_str())
    }
}

// ──────────────────────────────────────────────────────────────────────────────
// Per-backend state
// ──────────────────────────────────────────────────────────────────────────────

/// Prepared statement cache for a single backend connection.
/// Maps statement content hash → backend-assigned name.
pub struct BackendStmtCache {
    stmts: FastHashMap<StmtHash, BackendStmtEntry>,
}

struct BackendStmtEntry {
    backend_name: String,
    /// The SQL and param_types, stored for hash collision verification.
    sql: String,
    param_types: Vec<i32>,
}

impl Default for BackendStmtCache {
    fn default() -> Self {
        Self::new()
    }
}

impl BackendStmtCache {
    pub fn new() -> Self {
        Self { stmts: HashMap::with_hasher(ahash::RandomState::new()) }
    }

    /// Look up the backend name for a statement hash.
    fn get_backend_name(&self, hash: StmtHash, sql: &str, param_types: &[i32]) -> Option<&str> {
        self.stmts.get(&hash).and_then(|entry| {
            // Verify content match to guard against hash collisions
            if entry.sql == sql && entry.param_types == param_types {
                Some(entry.backend_name.as_str())
            } else {
                None
            }
        })
    }

    /// Register a new statement on this backend. Returns the assigned backend name.
    fn insert(&mut self, hash: StmtHash, sql: String, param_types: Vec<i32>) -> &str {
        let id = BACKEND_STMT_COUNTER.fetch_add(1, Ordering::Relaxed);
        let backend_name = format!("eden_s_{id}");
        self.stmts.insert(hash, BackendStmtEntry { backend_name, sql, param_types });
        &self.stmts.get(&hash).expect("just inserted").backend_name
    }
}

/// Remove the cached statement metadata for a backend (e.g. on connection loss).
pub fn invalidate_backend_cache(backend_id: BackendId) {
    BACKEND_CACHES.remove(&backend_id);
}

/// Check if a merged response contains a schema-mismatch ErrorResponse that
/// should trigger statement cache invalidation and retry (DW-7).
///
/// Scans for ErrorResponse messages (`'E'`) and extracts the SQLSTATE `'C'`
/// field. Returns `true` if any error matches:
/// - `42P01` (undefined table)
/// - `42703` (undefined column)
/// - `42804` (datatype mismatch)
/// - `42P18` (indeterminate datatype)
pub fn has_schema_mismatch_error(response: &[u8]) -> bool {
    const SCHEMA_CODES: &[&[u8]] = &[b"42P01", b"42703", b"42804", b"42P18"];

    let mut pos = 0;
    while pos < response.len() {
        let msg_type = response[pos];
        let Some(msg_len) = pg_scan::scan_pg_message(&response[pos..]) else {
            break;
        };

        if msg_type == b'E' {
            // Parse ErrorResponse fields: skip type(1) + length(4), then
            // read field_type(1) + null-terminated value pairs until 0x00.
            let mut fpos = pos + 5;
            while fpos < pos + msg_len {
                let field_type = response[fpos];
                fpos += 1;
                if field_type == 0 {
                    break; // End of fields
                }
                // Find the null terminator for this field value.
                let value_start = fpos;
                while fpos < pos + msg_len && response[fpos] != 0 {
                    fpos += 1;
                }
                if field_type == b'C' {
                    let code = &response[value_start..fpos];
                    if SCHEMA_CODES.contains(&code) {
                        return true;
                    }
                }
                fpos += 1; // skip null terminator
            }
        }

        pos += msg_len;
    }

    false
}

// ──────────────────────────────────────────────────────────────────────────────
// Hashing
// ──────────────────────────────────────────────────────────────────────────────

fn compute_stmt_hash(sql: &str, param_types: &[i32]) -> StmtHash {
    let mut hasher = Xxh3::new();
    hasher.update(&(sql.len() as u64).to_le_bytes());
    hasher.update(sql.as_bytes());
    hasher.update(&(param_types.len() as u64).to_le_bytes());
    for oid in param_types {
        hasher.update(&oid.to_le_bytes());
    }
    hasher.digest()
}

// ──────────────────────────────────────────────────────────────────────────────
// Batch analysis
// ──────────────────────────────────────────────────────────────────────────────

/// A single operation extracted from an extended query batch.
enum BatchOp<'a> {
    /// Named PARSE — may be skipped if backend already has it.
    Parse {
        client_name: &'a str,
        sql: &'a str,
        param_types: Vec<i32>,
        hash: StmtHash,
    },
    /// Unnamed PARSE — always forwarded.
    UnnamedParse { raw_msg: &'a [u8] },
    /// Named BIND — statement name must be rewritten.
    NamedBind {
        client_stmt_name: &'a str,
        raw_msg: &'a [u8],
        /// Byte offset of statement name within raw_msg.
        stmt_name_offset: usize,
        /// Byte length of the statement name (without null terminator).
        stmt_name_len: usize,
    },
    /// Unnamed BIND — forwarded as-is.
    UnnamedBind { raw_msg: &'a [u8] },
    /// Named DESCRIBE S — statement name must be rewritten.
    NamedDescribeStmt {
        client_stmt_name: &'a str,
        raw_msg: &'a [u8],
        name_offset: usize,
        name_len: usize,
    },
    /// Named CLOSE S — intercepted; synthetic CloseComplete sent to client.
    NamedCloseStmt { client_name: &'a str },
    /// Any message forwarded as-is (EXECUTE, DESCRIBE P, CLOSE P, FLUSH, SYNC).
    Passthrough { raw_msg: &'a [u8] },
}

/// Walk the raw batch buffer and decompose it into typed operations.
fn analyze_batch(buf: &[u8]) -> Vec<BatchOp<'_>> {
    let mut ops = Vec::new();
    let mut pos = 0;

    while pos < buf.len() {
        let remaining = &buf[pos..];
        let msg_len = match pg_scan::frontend_message_len(remaining) {
            Some(len) => len,
            None => break,
        };
        let msg = &buf[pos..pos + msg_len];
        let msg_type = msg[0];

        match msg_type {
            b'P' => {
                if let Some((name, sql, param_types)) = pg_scan::extract_parse_full(msg) {
                    if name.is_empty() {
                        ops.push(BatchOp::UnnamedParse { raw_msg: msg });
                    } else {
                        let hash = compute_stmt_hash(sql, &param_types);
                        ops.push(BatchOp::Parse { client_name: name, sql, param_types, hash });
                    }
                } else {
                    ops.push(BatchOp::Passthrough { raw_msg: msg });
                }
            }
            b'B' => {
                if let Some((stmt_name, offset, len)) = pg_scan::extract_bind_stmt_name(msg) {
                    if stmt_name.is_empty() {
                        ops.push(BatchOp::UnnamedBind { raw_msg: msg });
                    } else {
                        ops.push(BatchOp::NamedBind {
                            client_stmt_name: stmt_name,
                            raw_msg: msg,
                            stmt_name_offset: offset,
                            stmt_name_len: len,
                        });
                    }
                } else {
                    ops.push(BatchOp::Passthrough { raw_msg: msg });
                }
            }
            b'D' => {
                if let Some((kind, name, offset, len)) = pg_scan::extract_describe_or_close_target(msg) {
                    if kind == b'S' && !name.is_empty() {
                        ops.push(BatchOp::NamedDescribeStmt {
                            client_stmt_name: name,
                            raw_msg: msg,
                            name_offset: offset,
                            name_len: len,
                        });
                    } else {
                        ops.push(BatchOp::Passthrough { raw_msg: msg });
                    }
                } else {
                    ops.push(BatchOp::Passthrough { raw_msg: msg });
                }
            }
            b'C' => {
                if let Some((kind, name, _, _)) = pg_scan::extract_describe_or_close_target(msg) {
                    if kind == b'S' && !name.is_empty() {
                        ops.push(BatchOp::NamedCloseStmt { client_name: name });
                    } else {
                        // CLOSE P (portal) or unnamed — forward as-is
                        ops.push(BatchOp::Passthrough { raw_msg: msg });
                    }
                } else {
                    ops.push(BatchOp::Passthrough { raw_msg: msg });
                }
            }
            // EXECUTE, FLUSH, SYNC, and anything else — pass through
            _ => {
                ops.push(BatchOp::Passthrough { raw_msg: msg });
            }
        }

        pos += msg_len;
    }

    ops
}

// ──────────────────────────────────────────────────────────────────────────────
// Batch rewriting
// ──────────────────────────────────────────────────────────────────────────────

/// Whether a particular response message is real (from backend) or synthetic.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ResponseSlot {
    /// Real message expected from backend — pass through.
    Real,
    /// Synthetic ParseComplete — emit without consuming backend data.
    SyntheticParseComplete,
    /// Synthetic CloseComplete — emit without consuming backend data.
    SyntheticCloseComplete,
    /// Consume one real backend message but discard it (don't forward to client).
    /// Used when we inject a PARSE for a statement the backend doesn't have:
    /// the backend sends ParseComplete, but the client didn't send PARSE.
    Discard,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum StatementCacheMuxSafety {
    Safe,
    Malformed,
    MissingSync,
    UnsafeNamedPortal,
    UnsupportedMessage(u8),
}

impl StatementCacheMuxSafety {
    pub const fn is_safe(self) -> bool {
        matches!(self, Self::Safe)
    }
}

/// Return whether an extended-query batch can be rewritten by the prepared
/// statement cache inside a shared backend worker.
///
/// This accepts named statements because statement names are remapped per
/// backend connection. Named portals are rejected because portals are also
/// backend-local state and are not currently remapped.
pub fn statement_cache_mux_safety(buf: &[u8]) -> StatementCacheMuxSafety {
    let mut pos = 0usize;
    let mut seen_sync = false;

    while pos < buf.len() {
        if pos + 5 > buf.len() {
            return StatementCacheMuxSafety::Malformed;
        }
        let length = i32::from_be_bytes([buf[pos + 1], buf[pos + 2], buf[pos + 3], buf[pos + 4]]);
        if length < 4 {
            return StatementCacheMuxSafety::Malformed;
        }
        let total = 1 + length as usize;
        if pos + total > buf.len() {
            return StatementCacheMuxSafety::Malformed;
        }

        let msg = &buf[pos..pos + total];
        match msg[0] {
            b'P' => {
                if pg_scan::extract_parse_full(msg).is_none() {
                    return StatementCacheMuxSafety::Malformed;
                }
            }
            b'B' => {
                let Some((portal_name, _)) = pg_scan::extract_bind_names(msg) else {
                    return StatementCacheMuxSafety::Malformed;
                };
                if !portal_name.is_empty() {
                    return StatementCacheMuxSafety::UnsafeNamedPortal;
                }
            }
            b'E' => {
                let Some(portal_name) = pg_scan::extract_execute_portal(msg) else {
                    return StatementCacheMuxSafety::Malformed;
                };
                if !portal_name.is_empty() {
                    return StatementCacheMuxSafety::UnsafeNamedPortal;
                }
            }
            b'D' | b'C' => {
                let Some((kind, name, _, _)) = pg_scan::extract_describe_or_close_target(msg) else {
                    return StatementCacheMuxSafety::Malformed;
                };
                if kind == b'P' && !name.is_empty() {
                    return StatementCacheMuxSafety::UnsafeNamedPortal;
                }
            }
            b'H' => {}
            b'S' => {
                if pos + total != buf.len() {
                    return StatementCacheMuxSafety::UnsupportedMessage(msg[0]);
                }
                seen_sync = true;
            }
            other => return StatementCacheMuxSafety::UnsupportedMessage(other),
        }

        pos += total;
    }

    if seen_sync {
        StatementCacheMuxSafety::Safe
    } else {
        StatementCacheMuxSafety::MissingSync
    }
}

/// Apply only the client-visible statement state changes from an extended
/// query batch.
///
/// This is used before prepared batches are dispatched to shared backend
/// workers. The worker receives a snapshot of the client statement map, while
/// the gateway keeps the authoritative per-client map for future Bind-only
/// batches.
pub fn apply_client_batch_state(batch_buf: &[u8], client_map: &mut ClientStmtMap) {
    for op in analyze_batch(batch_buf) {
        match op {
            BatchOp::Parse { client_name, sql, param_types, .. } => {
                client_map.insert(client_name.to_string(), sql.to_string(), param_types);
            }
            BatchOp::NamedCloseStmt { client_name } => {
                client_map.remove(client_name);
            }
            _ => {}
        }
    }
}

/// Result of rewriting a batch for a specific backend.
pub struct RewrittenBatch {
    /// The bytes to send to the backend.
    pub backend_bytes: Bytes,
    /// Ordered list of expected responses (real vs synthetic).
    response_slots: Vec<ResponseSlot>,
}

impl RewrittenBatch {
    /// Get the response slots for merging with backend response.
    pub fn response_slots(&self) -> &[ResponseSlot] {
        &self.response_slots
    }
}

/// Rewrite a batch of extended query messages for a specific backend.
///
/// Updates `client_map` with new PARSEs and CLOSE removals.
/// Looks up / inserts into the backend's statement cache.
pub fn rewrite_batch(batch_buf: &[u8], client_map: &mut ClientStmtMap, backend_id: BackendId) -> RewrittenBatch {
    let ops = analyze_batch(batch_buf);
    let mut backend_bytes = BytesMut::with_capacity(batch_buf.len());
    let mut response_slots = Vec::with_capacity(ops.len());

    // Get or create the backend's cache entry.
    let mut cache_entry = BACKEND_CACHES.entry(backend_id).or_default();
    let cache = cache_entry.value_mut();

    for op in &ops {
        match op {
            BatchOp::Parse { client_name, sql, param_types, hash } => {
                // Record in client map
                client_map.insert(client_name.to_string(), sql.to_string(), param_types.clone());

                // Check if backend already has this statement
                if cache.get_backend_name(*hash, sql, param_types).is_some() {
                    // Skip PARSE — backend already has it
                    response_slots.push(ResponseSlot::SyntheticParseComplete);
                } else {
                    // Backend needs this statement — send PARSE with backend-assigned name
                    let backend_name = cache.insert(*hash, sql.to_string(), param_types.clone()).to_owned();
                    let parse_msg = Parse::new(&backend_name, *sql, param_types.clone()).encode();
                    backend_bytes.extend_from_slice(&parse_msg);
                    response_slots.push(ResponseSlot::Real);
                }
            }
            BatchOp::UnnamedParse { raw_msg } => {
                backend_bytes.extend_from_slice(raw_msg);
                response_slots.push(ResponseSlot::Real);
            }
            BatchOp::NamedBind { client_stmt_name, raw_msg, stmt_name_offset, stmt_name_len } => {
                // Look up the backend name for this client statement
                if let Some(cs) = client_map.get(client_stmt_name) {
                    let backend_name = if let Some(name) = cache.get_backend_name(cs.hash, &cs.sql, &cs.param_types) {
                        name.to_owned()
                    } else {
                        // Backend doesn't have this statement (different pool connection).
                        // Inject a PARSE so the backend learns it before the BIND.
                        // Use Discard slot: backend sends ParseComplete but the client
                        // didn't send PARSE in this batch and doesn't expect it.
                        let name = cache.insert(cs.hash, cs.sql.clone(), cs.param_types.clone()).to_owned();
                        let parse_msg = Parse::new(&name, &cs.sql, cs.param_types.clone()).encode();
                        backend_bytes.extend_from_slice(&parse_msg);
                        response_slots.push(ResponseSlot::Discard);
                        name
                    };
                    let rewritten = rewrite_name_in_msg(raw_msg, *stmt_name_offset, *stmt_name_len, &backend_name);
                    backend_bytes.extend_from_slice(&rewritten);
                } else {
                    // Client references unknown statement — forward as-is (will error)
                    backend_bytes.extend_from_slice(raw_msg);
                }
                response_slots.push(ResponseSlot::Real);
            }
            BatchOp::UnnamedBind { raw_msg } => {
                backend_bytes.extend_from_slice(raw_msg);
                response_slots.push(ResponseSlot::Real);
            }
            BatchOp::NamedDescribeStmt { client_stmt_name, raw_msg, name_offset, name_len } => {
                if let Some(cs) = client_map.get(client_stmt_name) {
                    let backend_name = if let Some(name) = cache.get_backend_name(cs.hash, &cs.sql, &cs.param_types) {
                        name.to_owned()
                    } else {
                        // Backend doesn't have this statement — inject PARSE first.
                        let name = cache.insert(cs.hash, cs.sql.clone(), cs.param_types.clone()).to_owned();
                        let parse_msg = Parse::new(&name, &cs.sql, cs.param_types.clone()).encode();
                        backend_bytes.extend_from_slice(&parse_msg);
                        response_slots.push(ResponseSlot::Discard);
                        name
                    };
                    let rewritten = rewrite_name_in_msg(raw_msg, *name_offset, *name_len, &backend_name);
                    backend_bytes.extend_from_slice(&rewritten);
                } else {
                    backend_bytes.extend_from_slice(raw_msg);
                }
                response_slots.push(ResponseSlot::Real);
            }
            BatchOp::NamedCloseStmt { client_name } => {
                // Intercept — don't close on backend (other clients may use it)
                client_map.remove(client_name);
                response_slots.push(ResponseSlot::SyntheticCloseComplete);
            }
            BatchOp::Passthrough { raw_msg } => {
                backend_bytes.extend_from_slice(raw_msg);
                // Not all passthrough messages produce a response (SYNC does, EXECUTE does,
                // but we don't need per-message response tracking for these — they are
                // forwarded directly). We only track Parse/Close for synthetic injection.
                // Don't push a response slot for these — they're part of the real response stream.
            }
        }
    }

    RewrittenBatch { backend_bytes: backend_bytes.freeze(), response_slots }
}

// ──────────────────────────────────────────────────────────────────────────────
// Response merging
// ──────────────────────────────────────────────────────────────────────────────

/// Merge synthetic responses into the real backend response.
///
/// The backend response is a sequence of messages ending with ReadyForQuery.
/// For each `SyntheticParseComplete`, we inject ParseComplete into the stream
/// at the position where the backend would have sent one (before the next
/// real response message). For `SyntheticCloseComplete`, same with CloseComplete.
///
/// Real messages from the backend are consumed in order and interleaved with
/// synthetics based on the `response_slots` ordering.
pub fn merge_responses(backend_response: &Bytes, slots: &[ResponseSlot]) -> Bytes {
    // Fast path: no synthetic or discard messages, just return the backend response as-is.
    if slots.iter().all(|s| matches!(s, ResponseSlot::Real)) {
        return backend_response.clone();
    }
    if slots.is_empty() {
        return backend_response.clone();
    }

    let mut result = BytesMut::with_capacity(backend_response.len() + slots.len() * 8);
    let mut backend_pos = 0;

    // Walk through response slots. For each slot:
    // - Real: consume the next complete message from the backend response
    // - Synthetic: emit the synthetic message without consuming backend data
    for slot in slots {
        match slot {
            ResponseSlot::SyntheticParseComplete => {
                result.extend_from_slice(&ParseComplete::encode());
            }
            ResponseSlot::SyntheticCloseComplete => {
                result.extend_from_slice(&CloseComplete::encode());
            }
            ResponseSlot::Real => {
                // Consume the next message from backend response
                if let Some(msg_len) = pg_scan::scan_pg_message(&backend_response[backend_pos..]) {
                    result.extend_from_slice(&backend_response[backend_pos..backend_pos + msg_len]);
                    backend_pos += msg_len;
                }
                // If scan_pg_message returns None, we've run out of backend messages
                // (e.g. error aborted the batch). Remaining synthetics will still be
                // emitted but there's nothing more to consume.
            }
            ResponseSlot::Discard => {
                // Consume the next message from backend but don't forward it.
                // Used for injected PARSEs — the backend sends ParseComplete
                // but the client didn't send PARSE and doesn't expect it.
                if let Some(msg_len) = pg_scan::scan_pg_message(&backend_response[backend_pos..]) {
                    backend_pos += msg_len;
                }
            }
        }
    }

    // Copy any remaining backend messages (ReadyForQuery, trailing data).
    if backend_pos < backend_response.len() {
        result.extend_from_slice(&backend_response[backend_pos..]);
    }

    result.freeze()
}

// ──────────────────────────────────────────────────────────────────────────────
// Message rewriting
// ──────────────────────────────────────────────────────────────────────────────

/// Rewrite a name field inside a wire protocol message (BIND stmt name,
/// DESCRIBE S name, etc.).
///
/// `offset` is the byte position of the name within `msg`.
/// `old_len` is the byte length of the old name (without null terminator).
/// The new name replaces old_name + null_terminator, and the message length
/// field (bytes 1..5) is adjusted.
fn rewrite_name_in_msg(msg: &[u8], offset: usize, old_len: usize, new_name: &str) -> Vec<u8> {
    let new_name_bytes = new_name.as_bytes();
    let size_diff = new_name_bytes.len() as isize - old_len as isize;
    let new_total = (msg.len() as isize + size_diff) as usize;
    let mut result = Vec::with_capacity(new_total);

    // Everything before the old name
    result.extend_from_slice(&msg[..offset]);
    // New name + null terminator
    result.extend_from_slice(new_name_bytes);
    result.push(0);
    // Everything after old name + null terminator
    let after_old = offset + old_len + 1;
    if after_old < msg.len() {
        result.extend_from_slice(&msg[after_old..]);
    }

    // Fix the length field (bytes 1..5): length = total_msg_size - 1 (type byte)
    let new_length = (result.len() - 1) as i32;
    result[1..5].copy_from_slice(&new_length.to_be_bytes());

    result
}

// ──────────────────────────────────────────────────────────────────────────────
// Tests
// ──────────────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;

    fn make_parse(stmt_name: &str, query: &str) -> Vec<u8> {
        let stmt_bytes = stmt_name.as_bytes();
        let query_bytes = query.as_bytes();
        let length = 4 + stmt_bytes.len() + 1 + query_bytes.len() + 1 + 2;
        let mut msg = vec![b'P'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(stmt_bytes);
        msg.push(0);
        msg.extend_from_slice(query_bytes);
        msg.push(0);
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg
    }

    fn make_bind(portal: &str, stmt: &str) -> Vec<u8> {
        let portal_bytes = portal.as_bytes();
        let stmt_bytes = stmt.as_bytes();
        let length = 4 + portal_bytes.len() + 1 + stmt_bytes.len() + 1 + 2 + 2 + 2;
        let mut msg = vec![b'B'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(portal_bytes);
        msg.push(0);
        msg.extend_from_slice(stmt_bytes);
        msg.push(0);
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg.extend_from_slice(&0i16.to_be_bytes());
        msg
    }

    fn make_describe_stmt(name: &str) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let length = 4 + 1 + name_bytes.len() + 1;
        let mut msg = vec![b'D'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.push(b'S');
        msg.extend_from_slice(name_bytes);
        msg.push(0);
        msg
    }

    fn make_close_stmt(name: &str) -> Vec<u8> {
        let name_bytes = name.as_bytes();
        let length = 4 + 1 + name_bytes.len() + 1;
        let mut msg = vec![b'C'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.push(b'S');
        msg.extend_from_slice(name_bytes);
        msg.push(0);
        msg
    }

    fn make_execute() -> Vec<u8> {
        // Execute unnamed portal, no row limit
        let length: i32 = 4 + 1 + 4; // length + portal\0 + max_rows
        let mut msg = vec![b'E'];
        msg.extend_from_slice(&length.to_be_bytes());
        msg.push(0); // unnamed portal
        msg.extend_from_slice(&0i32.to_be_bytes()); // unlimited rows
        msg
    }

    fn make_sync() -> Vec<u8> {
        vec![b'S', 0, 0, 0, 4]
    }

    fn make_parse_complete() -> Vec<u8> {
        vec![b'1', 0, 0, 0, 4]
    }

    fn make_bind_complete() -> Vec<u8> {
        vec![b'2', 0, 0, 0, 4]
    }

    fn make_command_complete(tag: &str) -> Vec<u8> {
        let tag_bytes = tag.as_bytes();
        let length = 4 + tag_bytes.len() + 1;
        let mut msg = vec![b'C'];
        msg.extend_from_slice(&(length as i32).to_be_bytes());
        msg.extend_from_slice(tag_bytes);
        msg.push(0);
        msg
    }

    fn make_ready_for_query(status: u8) -> Vec<u8> {
        vec![b'Z', 0, 0, 0, 5, status]
    }

    #[test]
    fn test_compute_stmt_hash_deterministic() {
        let h1 = compute_stmt_hash("SELECT 1", &[]);
        let h2 = compute_stmt_hash("SELECT 1", &[]);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_compute_stmt_hash_different_sql() {
        let h1 = compute_stmt_hash("SELECT 1", &[]);
        let h2 = compute_stmt_hash("SELECT 2", &[]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_compute_stmt_hash_different_params() {
        let h1 = compute_stmt_hash("SELECT $1", &[23]);
        let h2 = compute_stmt_hash("SELECT $1", &[25]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_analyze_batch_parse_bind_sync() {
        let mut batch = Vec::new();
        batch.extend_from_slice(&make_parse("s1", "SELECT 1"));
        batch.extend_from_slice(&make_bind("", "s1"));
        batch.extend_from_slice(&make_execute());
        batch.extend_from_slice(&make_sync());

        let ops = analyze_batch(&batch);
        assert_eq!(ops.len(), 4);
        assert!(matches!(ops[0], BatchOp::Parse { client_name: "s1", .. }));
        assert!(matches!(ops[1], BatchOp::NamedBind { client_stmt_name: "s1", .. }));
        assert!(matches!(ops[2], BatchOp::Passthrough { .. })); // EXECUTE
        assert!(matches!(ops[3], BatchOp::Passthrough { .. })); // SYNC
    }

    #[test]
    fn test_analyze_batch_unnamed() {
        let mut batch = Vec::new();
        batch.extend_from_slice(&make_parse("", "SELECT 1"));
        batch.extend_from_slice(&make_bind("", ""));
        batch.extend_from_slice(&make_sync());

        let ops = analyze_batch(&batch);
        assert!(matches!(ops[0], BatchOp::UnnamedParse { .. }));
        assert!(matches!(ops[1], BatchOp::UnnamedBind { .. }));
    }

    #[test]
    fn test_analyze_batch_close_stmt() {
        let mut batch = Vec::new();
        batch.extend_from_slice(&make_close_stmt("s1"));
        batch.extend_from_slice(&make_sync());

        let ops = analyze_batch(&batch);
        assert!(matches!(ops[0], BatchOp::NamedCloseStmt { client_name: "s1" }));
    }

    #[test]
    fn test_analyze_batch_describe_stmt() {
        let mut batch = Vec::new();
        batch.extend_from_slice(&make_describe_stmt("s1"));
        batch.extend_from_slice(&make_sync());

        let ops = analyze_batch(&batch);
        assert!(matches!(ops[0], BatchOp::NamedDescribeStmt { client_stmt_name: "s1", .. }));
    }

    #[test]
    fn test_statement_cache_mux_safety_accepts_named_statement() {
        let mut batch = Vec::new();
        batch.extend_from_slice(&make_parse("sqlx_s_1", "SELECT 1"));
        batch.extend_from_slice(&make_bind("", "sqlx_s_1"));
        batch.extend_from_slice(&make_execute());
        batch.extend_from_slice(&make_sync());

        assert_eq!(statement_cache_mux_safety(&batch), StatementCacheMuxSafety::Safe);
    }

    #[test]
    fn test_statement_cache_mux_safety_rejects_named_portal() {
        let mut batch = Vec::new();
        batch.extend_from_slice(&make_parse("sqlx_s_1", "SELECT 1"));
        batch.extend_from_slice(&make_bind("portal_1", "sqlx_s_1"));
        batch.extend_from_slice(&make_execute());
        batch.extend_from_slice(&make_sync());

        assert_eq!(statement_cache_mux_safety(&batch), StatementCacheMuxSafety::UnsafeNamedPortal);
    }

    #[test]
    fn test_rewrite_name_in_msg() {
        // Create a BIND with stmt name "s1" and rewrite to "eden_s_42"
        let msg = make_bind("", "s1");
        let (_, offset, len) = pg_scan::extract_bind_stmt_name(&msg).expect("extract_bind_stmt_name failed");
        let rewritten = rewrite_name_in_msg(&msg, offset, len, "eden_s_42");

        // Verify the rewritten message parses correctly
        let (new_name, _, _) = pg_scan::extract_bind_stmt_name(&rewritten).expect("extract_bind_stmt_name failed");
        assert_eq!(new_name, "eden_s_42");

        // Verify length field is correct
        let new_length = i32::from_be_bytes([rewritten[1], rewritten[2], rewritten[3], rewritten[4]]) as usize;
        assert_eq!(new_length + 1, rewritten.len());
    }

    #[test]
    fn test_rewrite_name_shorter() {
        // Rewrite a long name to a shorter one
        let msg = make_bind("", "very_long_statement_name");
        let (_, offset, len) = pg_scan::extract_bind_stmt_name(&msg).expect("extract_bind_stmt_name failed");
        let rewritten = rewrite_name_in_msg(&msg, offset, len, "s1");

        let (new_name, _, _) = pg_scan::extract_bind_stmt_name(&rewritten).expect("extract_bind_stmt_name failed");
        assert_eq!(new_name, "s1");

        let new_length = i32::from_be_bytes([rewritten[1], rewritten[2], rewritten[3], rewritten[4]]) as usize;
        assert_eq!(new_length + 1, rewritten.len());
    }

    #[test]
    fn test_rewrite_batch_first_parse() {
        // First time seeing a statement — should forward PARSE with backend name
        let backend_id: BackendId = (99999, 1);
        BACKEND_CACHES.remove(&backend_id); // clean slate

        let mut batch = Vec::new();
        batch.extend_from_slice(&make_parse("sqlx_s_1", "SELECT 1"));
        batch.extend_from_slice(&make_bind("", "sqlx_s_1"));
        batch.extend_from_slice(&make_execute());
        batch.extend_from_slice(&make_sync());

        let mut client_map = ClientStmtMap::new();
        let result = rewrite_batch(&batch, &mut client_map, backend_id);

        // Should have one Real (Parse) and one Real (Bind) in the response slots
        assert!(result.response_slots.iter().any(|s| matches!(s, ResponseSlot::Real)));
        // No synthetics for this case (first parse)
        assert!(!result.response_slots.iter().any(|s| matches!(s, ResponseSlot::SyntheticParseComplete)));

        // Client map should now have "sqlx_s_1"
        assert!(client_map.get("sqlx_s_1").is_some());

        // Clean up
        BACKEND_CACHES.remove(&backend_id);
    }

    #[test]
    fn test_rewrite_batch_cached_parse() {
        // Second time seeing same statement — should skip PARSE, emit synthetic ParseComplete
        let backend_id: BackendId = (99998, 2);
        BACKEND_CACHES.remove(&backend_id);

        let mut client_map = ClientStmtMap::new();

        // First batch: PARSE
        let mut batch1 = Vec::new();
        batch1.extend_from_slice(&make_parse("sqlx_s_1", "SELECT 1"));
        batch1.extend_from_slice(&make_sync());
        let _ = rewrite_batch(&batch1, &mut client_map, backend_id);

        // Second batch: same PARSE again
        let mut batch2 = Vec::new();
        batch2.extend_from_slice(&make_parse("sqlx_s_1", "SELECT 1"));
        batch2.extend_from_slice(&make_bind("", "sqlx_s_1"));
        batch2.extend_from_slice(&make_sync());
        let result = rewrite_batch(&batch2, &mut client_map, backend_id);

        // First slot should be synthetic (skipped PARSE)
        assert_eq!(result.response_slots[0], ResponseSlot::SyntheticParseComplete);
        // Second slot is real (BIND)
        assert_eq!(result.response_slots[1], ResponseSlot::Real);

        BACKEND_CACHES.remove(&backend_id);
    }

    #[test]
    fn test_apply_client_batch_state_preserves_bind_only_rewrite_state() {
        let backend_id: BackendId = (99996, 4);
        BACKEND_CACHES.remove(&backend_id);

        let mut client_map = ClientStmtMap::new();
        let mut parse_batch = Vec::new();
        parse_batch.extend_from_slice(&make_parse("P_0", "SELECT 1"));
        parse_batch.extend_from_slice(&make_sync());
        apply_client_batch_state(&parse_batch, &mut client_map);

        let mut bind_batch = Vec::new();
        bind_batch.extend_from_slice(&make_bind("", "P_0"));
        bind_batch.extend_from_slice(&make_execute());
        bind_batch.extend_from_slice(&make_sync());
        let result = rewrite_batch(&bind_batch, &mut client_map, backend_id);

        assert_eq!(result.response_slots[0], ResponseSlot::Discard);
        assert_eq!(result.response_slots[1], ResponseSlot::Real);
        assert!(pg_scan::extract_parse_full(&result.backend_bytes).is_some());

        BACKEND_CACHES.remove(&backend_id);
    }

    #[test]
    fn test_rewrite_batch_close_intercept() {
        let backend_id: BackendId = (99997, 3);
        BACKEND_CACHES.remove(&backend_id);

        let mut client_map = ClientStmtMap::new();
        client_map.insert("s1".to_string(), "SELECT 1".to_string(), vec![]);

        let mut batch = Vec::new();
        batch.extend_from_slice(&make_close_stmt("s1"));
        batch.extend_from_slice(&make_sync());
        let result = rewrite_batch(&batch, &mut client_map, backend_id);

        // Close should be synthetic
        assert_eq!(result.response_slots[0], ResponseSlot::SyntheticCloseComplete);
        // "s1" should be removed from client map
        assert!(client_map.get("s1").is_none());

        BACKEND_CACHES.remove(&backend_id);
    }

    #[test]
    fn test_merge_responses_no_synthetics() {
        let mut backend = Vec::new();
        backend.extend_from_slice(&make_parse_complete());
        backend.extend_from_slice(&make_bind_complete());
        backend.extend_from_slice(&make_command_complete("SELECT 1"));
        backend.extend_from_slice(&make_ready_for_query(b'I'));
        let backend = Bytes::from(backend);

        let slots = vec![ResponseSlot::Real, ResponseSlot::Real, ResponseSlot::Real];
        let merged = merge_responses(&backend, &slots);
        assert_eq!(merged, backend);
    }

    #[test]
    fn test_merge_responses_with_synthetic_parse_complete() {
        // Backend sends: BindComplete + CommandComplete + ReadyForQuery
        // Slots: SyntheticParseComplete, Real (BindComplete), Real (CC), then remaining (RFQ)
        let mut backend = Vec::new();
        backend.extend_from_slice(&make_bind_complete());
        backend.extend_from_slice(&make_command_complete("SELECT 1"));
        backend.extend_from_slice(&make_ready_for_query(b'I'));
        let backend = Bytes::from(backend);

        let slots = vec![
            ResponseSlot::SyntheticParseComplete,
            ResponseSlot::Real, // BindComplete
            ResponseSlot::Real, // CommandComplete
        ];
        let merged = merge_responses(&backend, &slots);

        // Result should start with ParseComplete, then BindComplete, CC, RFQ
        assert_eq!(merged[0], b'1'); // ParseComplete
        assert_eq!(merged[5], b'2'); // BindComplete
    }

    #[test]
    fn test_merge_responses_with_synthetic_close_complete() {
        let mut backend = Vec::new();
        backend.extend_from_slice(&make_ready_for_query(b'I'));
        let backend = Bytes::from(backend);

        let slots = vec![ResponseSlot::SyntheticCloseComplete];
        let merged = merge_responses(&backend, &slots);

        // Should have CloseComplete then ReadyForQuery
        assert_eq!(merged[0], b'3'); // CloseComplete
        assert_eq!(merged[5], b'Z'); // ReadyForQuery
    }

    #[test]
    fn test_rewrite_batch_cross_backend_inject_parse() {
        // Simulate: client PARSEd on backend B1, now BIND goes to backend B2.
        // B2 doesn't have the statement, so rewrite_batch must inject a PARSE.
        let backend_b1: BackendId = (99990, 1);
        let backend_b2: BackendId = (99991, 2);
        BACKEND_CACHES.remove(&backend_b1);
        BACKEND_CACHES.remove(&backend_b2);

        let mut client_map = ClientStmtMap::new();

        // Batch 1 on B1: PARSE + BIND + EXECUTE + SYNC
        let mut batch1 = Vec::new();
        batch1.extend_from_slice(&make_parse("sqlx_s_1", "SELECT 1"));
        batch1.extend_from_slice(&make_bind("", "sqlx_s_1"));
        batch1.extend_from_slice(&make_execute());
        batch1.extend_from_slice(&make_sync());
        let _result1 = rewrite_batch(&batch1, &mut client_map, backend_b1);

        // Batch 2 on B2: BIND + EXECUTE + SYNC (no PARSE — already prepared)
        let mut batch2 = Vec::new();
        batch2.extend_from_slice(&make_bind("", "sqlx_s_1"));
        batch2.extend_from_slice(&make_execute());
        batch2.extend_from_slice(&make_sync());
        let result2 = rewrite_batch(&batch2, &mut client_map, backend_b2);

        // Should have injected a PARSE (Discard slot) before the BIND (Real slot)
        assert_eq!(result2.response_slots[0], ResponseSlot::Discard);
        assert_eq!(result2.response_slots[1], ResponseSlot::Real);

        // Backend bytes should contain the injected PARSE
        assert_eq!(result2.backend_bytes[0], b'P'); // PARSE message

        BACKEND_CACHES.remove(&backend_b1);
        BACKEND_CACHES.remove(&backend_b2);
    }

    /// Build a minimal ErrorResponse with the given SQLSTATE code.
    fn make_error_response(code: &str) -> Vec<u8> {
        let mut msg = Vec::new();
        msg.push(b'E');
        // Placeholder for length — we'll fill it after building fields
        msg.extend_from_slice(&[0, 0, 0, 0]);
        // 'S' severity field
        msg.push(b'S');
        msg.extend_from_slice(b"ERROR");
        msg.push(0);
        // 'C' code field
        msg.push(b'C');
        msg.extend_from_slice(code.as_bytes());
        msg.push(0);
        // 'M' message field
        msg.push(b'M');
        msg.extend_from_slice(b"test error");
        msg.push(0);
        // Terminator
        msg.push(0);
        // Fix length (total - 1 for the type byte)
        let length = (msg.len() - 1) as i32;
        msg[1..5].copy_from_slice(&length.to_be_bytes());
        msg
    }

    #[test]
    fn test_has_schema_mismatch_error_undefined_table() {
        let response = make_error_response("42P01");
        assert!(has_schema_mismatch_error(&response));
    }

    #[test]
    fn test_has_schema_mismatch_error_undefined_column() {
        let response = make_error_response("42703");
        assert!(has_schema_mismatch_error(&response));
    }

    #[test]
    fn test_has_schema_mismatch_error_non_matching_code() {
        let response = make_error_response("23505"); // unique_violation
        assert!(!has_schema_mismatch_error(&response));
    }

    #[test]
    fn test_has_schema_mismatch_error_no_error() {
        // ParseComplete + ReadyForQuery — no error present
        let mut response = make_parse_complete();
        response.extend_from_slice(&make_ready_for_query(b'I'));
        assert!(!has_schema_mismatch_error(&response));
    }

    #[test]
    fn test_has_schema_mismatch_error_embedded_in_stream() {
        // ParseComplete + ErrorResponse(42P01) + ReadyForQuery
        let mut response = make_parse_complete();
        response.extend_from_slice(&make_error_response("42P01"));
        response.extend_from_slice(&make_ready_for_query(b'I'));
        assert!(has_schema_mismatch_error(&response));
    }

    #[test]
    fn test_merge_responses_with_discard() {
        // Backend sends: ParseComplete (from injected PARSE) + BindComplete + CC + RFQ
        // Slots: Discard (ParseComplete), Real (BindComplete)
        // Result should NOT contain ParseComplete
        let mut backend = Vec::new();
        backend.extend_from_slice(&make_parse_complete());
        backend.extend_from_slice(&make_bind_complete());
        backend.extend_from_slice(&make_command_complete("SELECT 1"));
        backend.extend_from_slice(&make_ready_for_query(b'I'));
        let backend = Bytes::from(backend);

        let slots = vec![
            ResponseSlot::Discard, // consumes ParseComplete, doesn't forward
            ResponseSlot::Real,    // BindComplete
        ];
        let merged = merge_responses(&backend, &slots);

        // Result should start with BindComplete (ParseComplete was discarded)
        assert_eq!(merged[0], b'2'); // BindComplete
    }
}
