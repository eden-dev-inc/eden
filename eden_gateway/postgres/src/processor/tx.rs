use bytes::Bytes;
use eden_core::format::cache_uuid::EndpointCacheUuid;
use eden_logger_internal::{LogAudience, LogContext, log_error, log_warn};
use endpoints::endpoint::postgres::protocol::PgPinnedConnection;

#[derive(Debug)]
pub(crate) struct PgPinnedTransactionTracker {
    in_transaction: bool,
    pinned: bool,
    endpoint_update_pending: bool,
}

impl PgPinnedTransactionTracker {
    pub(crate) fn new() -> Self {
        Self {
            in_transaction: false,
            pinned: false,
            endpoint_update_pending: false,
        }
    }

    pub(crate) fn needs_pin(&self) -> bool {
        !self.pinned
    }

    pub(crate) fn mark_pinned(&mut self) {
        self.pinned = true;
    }

    pub(crate) fn on_begin(&mut self) {
        self.in_transaction = true;
    }

    pub(crate) fn on_end(&mut self) {
        self.in_transaction = false;
    }

    pub(crate) fn should_release(&self) -> bool {
        !self.in_transaction
    }

    pub(crate) fn release(&mut self) {
        self.pinned = false;
    }

    pub(crate) fn should_defer_endpoint_update(&self) -> bool {
        self.pinned
    }

    pub(crate) fn take_pending_endpoint_update(&mut self) -> bool {
        std::mem::take(&mut self.endpoint_update_pending)
    }

    pub(crate) fn on_connection_error(&mut self) {
        self.in_transaction = false;
        self.pinned = false;
    }

    pub(crate) fn is_in_transaction(&self) -> bool {
        self.in_transaction
    }
}

pub(crate) struct DualWriteTxBuffer {
    buffered_writes: Vec<Bytes>,
    session_commands: Vec<Bytes>,
    savepoint_indices: Vec<usize>,
    secondary_endpoint: Option<EndpointCacheUuid>,
    active: bool,
}

impl DualWriteTxBuffer {
    pub(crate) fn new() -> Self {
        Self {
            buffered_writes: Vec::new(),
            session_commands: Vec::new(),
            savepoint_indices: Vec::new(),
            secondary_endpoint: None,
            active: false,
        }
    }

    pub(crate) fn push(&mut self, pg_bytes: Bytes) {
        if self.active {
            self.buffered_writes.push(pg_bytes);
        }
    }

    pub(crate) fn push_session(&mut self, pg_bytes: Bytes) {
        if self.active {
            self.session_commands.push(pg_bytes);
        }
    }

    pub(crate) fn on_savepoint(&mut self) {
        if self.active {
            self.savepoint_indices.push(self.buffered_writes.len());
        }
    }

    pub(crate) fn on_release_savepoint(&mut self) {
        self.savepoint_indices.pop();
    }

    pub(crate) fn on_rollback_to_savepoint(&mut self) {
        if let Some(idx) = self.savepoint_indices.pop() {
            self.buffered_writes.truncate(idx);
        }
    }

    pub(crate) fn drain(&mut self) -> (Vec<Bytes>, Vec<Bytes>, Option<EndpointCacheUuid>) {
        self.active = false;
        self.savepoint_indices.clear();
        (
            std::mem::take(&mut self.session_commands),
            std::mem::take(&mut self.buffered_writes),
            self.secondary_endpoint.take(),
        )
    }

    pub(crate) fn discard(&mut self) {
        self.active = false;
        self.buffered_writes.clear();
        self.session_commands.clear();
        self.savepoint_indices.clear();
        self.secondary_endpoint = None;
    }

    pub(crate) fn is_active(&self) -> bool {
        self.active
    }
}

pub(crate) async fn cleanup_pinned_conn(conn: &mut Option<PgPinnedConnection>, in_transaction: bool, ctx: &LogContext) {
    let Some(pinned) = conn.as_mut() else {
        return;
    };

    if in_transaction {
        let rollback_msg = postgres_core::client::build_query_message("ROLLBACK");
        if let Err(err) = pinned.send_query_raw(&rollback_msg).await {
            log_warn!(
                ctx.clone(),
                "Pinned connection ROLLBACK failed on cleanup",
                audience = LogAudience::Internal,
                error = err.to_string()
            );
        }
    }

    *conn = None;
}

pub(crate) async fn handle_two_phase_commit(
    auth_conn: &mut PgPinnedConnection,
    sec_conn: &mut PgPinnedConnection,
    gid: &str,
    ctx: &LogContext,
) -> Result<(), String> {
    use crate::replay_queue::response_has_error;

    let prepare_sql = format!("PREPARE TRANSACTION '{}'", gid);
    let prepare_msg = postgres_core::client::build_query_message(&prepare_sql);

    let auth_prepare = auth_conn.send_query_raw(&prepare_msg).await;
    let sec_prepare = sec_conn.send_query_raw(&prepare_msg).await;

    let auth_prepared = match &auth_prepare {
        Ok((resp, _)) => !response_has_error(resp),
        Err(_) => false,
    };
    let sec_prepared = match &sec_prepare {
        Ok((resp, _)) => !response_has_error(resp),
        Err(_) => false,
    };

    if !auth_prepared && !sec_prepared {
        let auth_err = auth_prepare.err().map_or_else(|| "SQL error".to_string(), |e| e.to_string());
        let sec_err = sec_prepare.err().map_or_else(|| "SQL error".to_string(), |e| e.to_string());
        return Err(format!("both PREPARE TRANSACTION failed: auth={}, sec={}", auth_err, sec_err));
    }

    if auth_prepared && !sec_prepared {
        let rollback_sql = format!("ROLLBACK PREPARED '{}'", gid);
        let rollback_msg = postgres_core::client::build_query_message(&rollback_sql);
        let _ = auth_conn.send_query_raw(&rollback_msg).await;
        let sec_err = sec_prepare.err().map_or_else(|| "SQL error".to_string(), |e| e.to_string());
        return Err(format!("secondary PREPARE TRANSACTION failed: {}", sec_err));
    }

    if !auth_prepared && sec_prepared {
        let rollback_sql = format!("ROLLBACK PREPARED '{}'", gid);
        let rollback_msg = postgres_core::client::build_query_message(&rollback_sql);
        let _ = sec_conn.send_query_raw(&rollback_msg).await;
        let auth_err = auth_prepare.err().map_or_else(|| "SQL error".to_string(), |e| e.to_string());
        return Err(format!("authoritative PREPARE TRANSACTION failed: {}", auth_err));
    }

    let commit_sql = format!("COMMIT PREPARED '{}'", gid);
    let commit_msg = postgres_core::client::build_query_message(&commit_sql);

    let auth_commit = auth_conn.send_query_raw(&commit_msg).await;
    let sec_commit = sec_conn.send_query_raw(&commit_msg).await;

    let auth_committed = match &auth_commit {
        Ok((resp, _)) => !response_has_error(resp),
        Err(_) => false,
    };
    let sec_committed = match &sec_commit {
        Ok((resp, _)) => !response_has_error(resp),
        Err(_) => false,
    };

    if auth_committed && sec_committed {
        return Ok(());
    }

    if !auth_committed {
        log_error!(
            ctx.clone(),
            "2PC CRITICAL: COMMIT PREPARED failed on authoritative — in-doubt transaction",
            audience = LogAudience::Internal,
            gid = gid
        );
    }
    if !sec_committed {
        log_error!(
            ctx.clone(),
            "2PC CRITICAL: COMMIT PREPARED failed on secondary — in-doubt transaction",
            audience = LogAudience::Internal,
            gid = gid
        );
    }

    Err(format!("COMMIT PREPARED failed — in-doubt transaction gid={}, check pg_prepared_xacts", gid))
}

pub(crate) async fn handle_two_phase_rollback(auth_conn: &mut PgPinnedConnection, sec_conn: &mut PgPinnedConnection, ctx: &LogContext) {
    let rollback_msg = postgres_core::client::build_query_message("ROLLBACK");
    if let Err(e) = auth_conn.send_query_raw(&rollback_msg).await {
        log_warn!(
            ctx.clone(),
            "2PC: ROLLBACK failed on authoritative connection",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
    }
    if let Err(e) = sec_conn.send_query_raw(&rollback_msg).await {
        log_warn!(
            ctx.clone(),
            "2PC: ROLLBACK failed on secondary connection",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
    }
}

pub(crate) async fn cleanup_2pc_conn(sec: &mut Option<PgPinnedConnection>, in_transaction: bool, ctx: &LogContext) {
    let Some(pinned) = sec.as_mut() else {
        return;
    };

    if in_transaction {
        let rollback_msg = postgres_core::client::build_query_message("ROLLBACK");
        if let Err(err) = pinned.send_query_raw(&rollback_msg).await {
            log_warn!(
                ctx.clone(),
                "2PC secondary connection ROLLBACK failed on cleanup",
                audience = LogAudience::Internal,
                error = err.to_string()
            );
        }
    }

    *sec = None;
}
