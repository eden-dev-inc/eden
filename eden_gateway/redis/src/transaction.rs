use std::time::Instant;

pub(crate) const MAX_WATCHED_KEYS: usize = 32;

/// Tracks the state of a Redis transaction (MULTI/EXEC/DISCARD) for a single connection.
///
/// This is designed to be held as local state per connection, avoiding the need for
/// global synchronization when tracking transactions.
pub struct TransactionState {
    start_time: Instant,
    command_count: u32,
    /// Key hashes of WATCH'ed keys transferred from pending_watched_keys on MULTI.
    watched_keys: Vec<u64>,
}

impl TransactionState {
    /// Starts a new transaction, recording the start time.
    pub fn new() -> Self {
        Self {
            start_time: Instant::now(),
            command_count: 0,
            watched_keys: Vec::new(),
        }
    }

    /// Records a command within the transaction.
    /// Returns the new command count.
    pub fn record_command(&mut self) -> u32 {
        self.command_count = self.command_count.saturating_add(1);
        self.command_count
    }

    /// Set the watched keys for this transaction (transferred from pending on MULTI).
    pub fn set_watched_keys(&mut self, keys: Vec<u64>) {
        self.watched_keys = keys;
        self.watched_keys.truncate(MAX_WATCHED_KEYS);
    }

    pub fn has_watches(&self) -> bool {
        !self.watched_keys.is_empty()
    }

    /// Finishes the transaction (commit or discard).
    /// Returns (command_count, duration_us).
    pub fn finish(self) -> (u32, u64) {
        let duration_us = self.start_time.elapsed().as_micros() as u64;
        (self.command_count, duration_us)
    }
}

impl Default for TransactionState {
    fn default() -> Self {
        Self::new()
    }
}
