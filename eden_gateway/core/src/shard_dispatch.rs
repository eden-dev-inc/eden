//! Protocol-neutral helpers for shard-dispatched gateway work.

use std::collections::BTreeMap;
use std::sync::{Mutex, MutexGuard};

pub type GatewayShardWork = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct GatewayShardDispatchError {
    shard_index: usize,
}

impl GatewayShardDispatchError {
    pub fn new(shard_index: usize) -> Self {
        Self { shard_index }
    }

    pub fn shard_index(self) -> usize {
        self.shard_index
    }
}

impl std::fmt::Display for GatewayShardDispatchError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "shard {} runtime is unavailable", self.shard_index)
    }
}

impl std::error::Error for GatewayShardDispatchError {}

pub trait GatewayShardDispatcher: Send + Sync {
    fn shard_count(&self) -> usize;

    fn dispatch_to_shard(&self, shard_index: usize, work: GatewayShardWork) -> Result<(), GatewayShardDispatchError>;
}

/// Per-connection monotonic-sequence reorder buffer for responses flowing
/// back from multiple shards.
///
/// Any gateway that dispatches successive batches from one client connection
/// to different shard runtimes can stamp each batch with `issue_seq()` and
/// feed completions into `complete()`. The returned vector is the contiguous
/// in-order run that is ready to write back to the client.
pub struct ConnectionSequencer<T> {
    state: Mutex<SequencerState<T>>,
}

struct SequencerState<T> {
    next_seq: u64,
    next_delivery: u64,
    buffer: BTreeMap<u64, T>,
}

impl<T> Default for ConnectionSequencer<T> {
    fn default() -> Self {
        Self::new()
    }
}

impl<T> ConnectionSequencer<T> {
    pub fn new() -> Self {
        Self {
            state: Mutex::new(SequencerState { next_seq: 0, next_delivery: 0, buffer: BTreeMap::new() }),
        }
    }

    pub fn issue_seq(&self) -> u64 {
        let mut state = self.lock_state();
        let seq = state.next_seq;
        state.next_seq = state.next_seq.saturating_add(1);
        seq
    }

    pub fn complete(&self, seq: u64, value: T) -> Vec<T> {
        let mut state = self.lock_state();
        if seq != state.next_delivery {
            state.buffer.insert(seq, value);
            return Vec::new();
        }

        let mut out = Vec::with_capacity(1 + state.buffer.len());
        out.push(value);
        let mut cursor = state.next_delivery.saturating_add(1);
        while let Some(buffered) = state.buffer.remove(&cursor) {
            out.push(buffered);
            cursor = cursor.saturating_add(1);
        }
        state.next_delivery = cursor;
        out
    }

    pub fn buffered_len(&self) -> usize {
        self.lock_state().buffer.len()
    }

    fn lock_state(&self) -> MutexGuard<'_, SequencerState<T>> {
        match self.state.lock() {
            Ok(guard) => guard,
            Err(poisoned) => poisoned.into_inner(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sequencer_in_order_completion_drains_immediately() {
        let seq = ConnectionSequencer::<&'static str>::new();
        let s0 = seq.issue_seq();
        let s1 = seq.issue_seq();
        let s2 = seq.issue_seq();
        assert_eq!((s0, s1, s2), (0, 1, 2));

        assert_eq!(seq.complete(0, "a"), vec!["a"]);
        assert_eq!(seq.complete(1, "b"), vec!["b"]);
        assert_eq!(seq.complete(2, "c"), vec!["c"]);
        assert_eq!(seq.buffered_len(), 0);
    }

    #[test]
    fn sequencer_out_of_order_buffers_then_drains_when_gap_fills() {
        let seq = ConnectionSequencer::<&'static str>::new();
        for _ in 0..3 {
            let _ = seq.issue_seq();
        }

        assert_eq!(seq.complete(1, "b"), Vec::<&str>::new());
        assert_eq!(seq.complete(2, "c"), Vec::<&str>::new());
        assert_eq!(seq.buffered_len(), 2);
        assert_eq!(seq.complete(0, "a"), vec!["a", "b", "c"]);
        assert_eq!(seq.buffered_len(), 0);
    }

    #[test]
    fn sequencer_partial_drain_leaves_unfilled_gap_buffered() {
        let seq = ConnectionSequencer::<u32>::new();
        for _ in 0..5 {
            let _ = seq.issue_seq();
        }

        assert_eq!(seq.complete(0, 0), vec![0]);
        assert_eq!(seq.complete(2, 2), Vec::<u32>::new());
        assert_eq!(seq.complete(3, 3), Vec::<u32>::new());
        assert_eq!(seq.buffered_len(), 2);
        assert_eq!(seq.complete(1, 1), vec![1, 2, 3]);
        assert_eq!(seq.buffered_len(), 0);
    }
}
