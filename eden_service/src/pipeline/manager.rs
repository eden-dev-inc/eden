//! CDC Pipeline Manager.
//!
//! Owns and supervises CDC WAL consumer tasks. Handles activate/pause/delete
//! lifecycle for CDC-mode snapshots.

use eden_core::error::EpError;
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{RwLock, broadcast};
use uuid::Uuid;

/// Signal sent to a CDC worker task.
#[derive(Debug, Clone)]
pub enum CdcSignal {
    /// Gracefully shut down the consumer (pause or delete).
    Shutdown,
}

/// Handle to a running CDC worker task.
#[derive(Debug)]
pub struct CdcWorkerHandle {
    /// Snapshot UUID this worker serves.
    pub snapshot_uuid: Uuid,
    /// Channel to send signals to the worker.
    pub signal_tx: broadcast::Sender<CdcSignal>,
    /// JoinHandle for the worker task.
    pub join_handle: tokio::task::JoinHandle<()>,
}

impl CdcWorkerHandle {
    /// Signal the worker to shut down.
    pub fn shutdown(&self) {
        let _ = self.signal_tx.send(CdcSignal::Shutdown);
    }

    /// Check if the worker task is still running.
    pub fn is_running(&self) -> bool {
        !self.join_handle.is_finished()
    }
}

/// Manages CDC worker tasks for all active CDC-mode snapshots.
#[derive(Debug)]
pub struct CdcManager {
    workers: Arc<RwLock<HashMap<Uuid, CdcWorkerHandle>>>,
}

impl CdcManager {
    pub fn new() -> Self {
        Self { workers: Arc::new(RwLock::new(HashMap::new())) }
    }

    /// Register a new CDC worker for a snapshot.
    ///
    /// Returns the broadcast receiver that the worker should listen on for signals.
    pub async fn register(&self, snapshot_uuid: Uuid, join_handle: tokio::task::JoinHandle<()>, signal_tx: broadcast::Sender<CdcSignal>) {
        let handle = CdcWorkerHandle { snapshot_uuid, signal_tx, join_handle };
        self.workers.write().await.insert(snapshot_uuid, handle);
    }

    /// Create a new signal channel for a CDC worker.
    pub fn new_signal_channel() -> (broadcast::Sender<CdcSignal>, broadcast::Receiver<CdcSignal>) {
        broadcast::channel(16)
    }

    /// Pause a CDC worker (shut down but keep replication slot).
    pub async fn pause(&self, snapshot_uuid: &Uuid) -> Result<(), EpError> {
        let mut workers = self.workers.write().await;
        if let Some(handle) = workers.remove(snapshot_uuid) {
            handle.shutdown();
            // Wait briefly for graceful shutdown
            let _ = tokio::time::timeout(std::time::Duration::from_secs(5), handle.join_handle).await;
            Ok(())
        } else {
            Err(EpError::parse(format!("No active CDC worker for snapshot {snapshot_uuid}")))
        }
    }

    /// Stop and remove a CDC worker (called before dropping replication slot).
    pub async fn stop(&self, snapshot_uuid: &Uuid) -> Result<(), EpError> {
        // Same as pause — the caller is responsible for dropping the slot
        self.pause(snapshot_uuid).await
    }

    /// Check if a CDC worker is active for a snapshot.
    pub async fn is_active(&self, snapshot_uuid: &Uuid) -> bool {
        let workers = self.workers.read().await;
        workers.get(snapshot_uuid).is_some_and(|h| h.is_running())
    }

    /// Get the number of active CDC workers.
    pub async fn active_count(&self) -> usize {
        let workers = self.workers.read().await;
        workers.values().filter(|h| h.is_running()).count()
    }

    /// Clean up finished worker handles.
    pub async fn cleanup_finished(&self) {
        let mut workers = self.workers.write().await;
        workers.retain(|_, handle| handle.is_running());
    }

    /// Get UUIDs of all active CDC workers.
    pub async fn active_snapshots(&self) -> Vec<Uuid> {
        let workers = self.workers.read().await;
        workers.iter().filter(|(_, h)| h.is_running()).map(|(uuid, _)| *uuid).collect()
    }
}

impl Default for CdcManager {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn test_manager_lifecycle() {
        let manager = CdcManager::new();
        let uuid = Uuid::new_v4();

        let (tx, mut rx) = CdcManager::new_signal_channel();

        // Spawn a dummy worker
        let handle = tokio::spawn(async move {
            // Wait for shutdown signal
            let _ = rx.recv().await;
        });

        manager.register(uuid, handle, tx).await;

        assert!(manager.is_active(&uuid).await);
        assert_eq!(manager.active_count().await, 1);

        manager.pause(&uuid).await.expect("pause");
        assert!(!manager.is_active(&uuid).await);
        assert_eq!(manager.active_count().await, 0);
    }

    #[tokio::test]
    async fn test_pause_nonexistent() {
        let manager = CdcManager::new();
        let result = manager.pause(&Uuid::new_v4()).await;
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_cleanup_finished() {
        let manager = CdcManager::new();
        let uuid = Uuid::new_v4();

        let (tx, _rx) = CdcManager::new_signal_channel();

        // Spawn a worker that finishes immediately
        let handle = tokio::spawn(async {});
        manager.register(uuid, handle, tx).await;

        // Give the task a moment to finish
        tokio::time::sleep(std::time::Duration::from_millis(10)).await;

        manager.cleanup_finished().await;
        assert_eq!(manager.active_count().await, 0);
    }
}
