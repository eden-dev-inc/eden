//! Connection pinning helpers for Redis WATCH/MULTI session affinity.

/// Action the caller should take after querying the pin tracker.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) enum PinAction {
    /// Connection is already pinned — no I/O needed.
    AlreadyPinned,
    /// Caller must acquire a pinned connection before forwarding.
    AcquirePin,
}

/// Tracks pinned-connection state for WATCH / MULTI / EXEC atomicity.
///
/// Before this tracker, each proxied command could hit a different pooled
/// connection, silently breaking Redis's WATCH/MULTI/EXEC guarantees.
/// The tracker ensures a connection is pinned on WATCH or MULTI and only
/// released after EXEC or DISCARD.
#[derive(Debug)]
pub(super) struct PinnedTransactionTracker {
    /// True while WATCH keys are active (cleared by UNWATCH / EXEC / DISCARD).
    watching: bool,
    /// True while inside a MULTI block (cleared by EXEC / DISCARD).
    in_multi: bool,
    /// True when an underlying pinned connection has been acquired.
    pinned: bool,
    /// Fatal pinned-connection failure; the client connection should be aborted.
    connection_failed: bool,
}

impl PinnedTransactionTracker {
    pub(super) fn new() -> Self {
        Self {
            watching: false,
            in_multi: false,
            pinned: false,
            connection_failed: false,
        }
    }

    /// Returns whether a pinned connection needs to be acquired.
    pub(super) fn pin_action(&self) -> PinAction {
        if self.pinned {
            PinAction::AlreadyPinned
        } else {
            PinAction::AcquirePin
        }
    }

    /// Mark that a pinned connection was successfully acquired.
    pub(super) fn mark_pinned(&mut self) {
        self.pinned = true;
        self.connection_failed = false;
    }

    /// Commit WATCH state (call only after pin is confirmed).
    pub(super) fn confirm_watch(&mut self) {
        self.watching = true;
    }

    /// An UNWATCH command was received.
    pub(super) fn on_unwatch(&mut self) {
        self.watching = false;
    }

    /// Commit MULTI state (call only after pin is confirmed).
    pub(super) fn confirm_multi(&mut self) {
        self.in_multi = true;
    }

    /// EXEC or DISCARD received and was not blocked by policy.
    pub(super) fn on_exec_or_discard(&mut self) {
        self.in_multi = false;
        self.watching = false;
    }

    /// Returns `true` if the pinned connection should be released
    /// (no longer in any transaction).
    pub(super) fn should_release(&self) -> bool {
        !self.watching && !self.in_multi
    }

    /// Release the pinned connection.
    pub(super) fn release(&mut self) {
        self.pinned = false;
        self.connection_failed = false;
    }

    /// Reset all state after a connection error (connection will be poisoned).
    pub(super) fn on_connection_error(&mut self) {
        self.watching = false;
        self.in_multi = false;
        self.pinned = false;
        self.connection_failed = true;
    }

    pub(super) fn should_abort_connection(&self) -> bool {
        self.connection_failed
    }

    pub(super) fn is_watching(&self) -> bool {
        self.watching
    }

    pub(super) fn is_in_multi(&self) -> bool {
        self.in_multi
    }

    #[cfg(test)]
    pub(super) fn is_pinned(&self) -> bool {
        self.pinned
    }
}
