//! Per-row write serializer for dual-write ordering guarantees (DW-4).
//!
//! When dual-writing to both old and new databases, concurrent writes to the
//! same row can arrive in different orders on each side if sent via
//! `tokio::join!`. This module provides a per-row lock to ensure that
//! overlapping writes to the same `(table, pk)` tuple are serialized.
//!
//! Locks are keyed by `"{table}:{pk_values}"` and are automatically
//! garbage-collected when no longer held.

use dashmap::DashMap;
use std::sync::Arc;
use tokio::sync::Mutex;

/// Per-row write serializer.
///
/// Acquires a mutex keyed by `table:pk_values` before executing a dual-write,
/// ensuring that concurrent writes to the same row are serialized across both
/// targets.
pub struct WriteSerializer {
    locks: Arc<DashMap<String, Arc<Mutex<()>>>>,
}

impl WriteSerializer {
    pub fn new() -> Self {
        Self { locks: Arc::new(DashMap::new()) }
    }

    /// Get or create a lock for the given table and PK values.
    ///
    /// Returns `None` if `table` is `None` (un-extractable table → no locking).
    pub fn lock_for(&self, table: Option<&str>, pk_values: &[String]) -> Option<Arc<Mutex<()>>> {
        let table = table?;
        let key = if pk_values.is_empty() {
            table.to_string()
        } else {
            format!("{}:{}", table, pk_values.join(","))
        };
        Some(self.locks.entry(key).or_insert_with(|| Arc::new(Mutex::new(()))).clone())
    }

    /// Remove locks that are no longer held by any write operation.
    ///
    /// A lock is eligible for GC when its strong reference count is 1
    /// (only the DashMap entry holds it — no active writers).
    pub fn gc(&self) {
        self.locks.retain(|_, v| Arc::strong_count(v) > 1);
    }

    /// Current number of tracked lock keys.
    pub fn len(&self) -> usize {
        self.locks.len()
    }

    /// Whether the serializer has no tracked keys.
    pub fn is_empty(&self) -> bool {
        self.locks.is_empty()
    }
}

impl Default for WriteSerializer {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lock_for_none_table() {
        let ws = WriteSerializer::new();
        assert!(ws.lock_for(None, &[]).is_none());
    }

    #[test]
    fn test_lock_for_same_key_returns_same_mutex() {
        let ws = WriteSerializer::new();
        let lock1 = ws.lock_for(Some("users"), &["42".to_string()]);
        let lock2 = ws.lock_for(Some("users"), &["42".to_string()]);
        assert!(lock1.is_some());
        assert!(lock2.is_some());
        assert!(Arc::ptr_eq(&lock1.expect("lock1 should be Some"), &lock2.expect("lock2 should be Some")));
    }

    #[test]
    fn test_lock_for_different_pk_returns_different_mutex() {
        let ws = WriteSerializer::new();
        let lock1 = ws.lock_for(Some("users"), &["1".to_string()]);
        let lock2 = ws.lock_for(Some("users"), &["2".to_string()]);
        assert!(!Arc::ptr_eq(&lock1.expect("lock1 should be Some"), &lock2.expect("lock2 should be Some")));
    }

    #[test]
    fn test_gc_removes_uncontested_locks() {
        let ws = WriteSerializer::new();
        {
            let _lock = ws.lock_for(Some("users"), &["1".to_string()]);
            assert_eq!(ws.len(), 1);
        }
        // After the lock Arc is dropped (only DashMap holds it), GC should clean it up.
        ws.gc();
        assert_eq!(ws.len(), 0);
    }

    #[test]
    fn test_gc_preserves_held_locks() {
        let ws = WriteSerializer::new();
        let _lock = ws.lock_for(Some("users"), &["1".to_string()]);
        ws.gc();
        // Lock is still held by `_lock`, so GC should keep it.
        assert_eq!(ws.len(), 1);
    }
}
