//! Async event channel messages between API tasks and the TUI.

use crate::migration::{ApiCallStatus, MigrationStatus, SetupStep};

#[derive(Debug)]
pub enum ApiEvent {
    SetupProgress(SetupStep),
    ApiCallUpdate {
        index: usize,
        status: ApiCallStatus,
    },
    SetupComplete {
        auth_token: String,
        source_endpoint_id: String,
        dest_endpoint_id: String,
        interlay_id: String,
        migration_id: String,
    },
    SetupFailed(String),
    MigrationTriggered,
    /// Status update from API. `force` bypasses stale-response protection (for explicit refresh)
    MigrationStatusUpdate {
        status: MigrationStatus,
        force: bool,
    },
    MigrationError(String),
    /// Debug log message from async tasks
    DebugLog(String),
    /// Canary traffic split was updated
    TrafficUpdated {
        old_percentage: f64,
        new_percentage: f64,
    },
    /// Canary traffic update failed
    TrafficUpdateFailed(String),
    /// Migration was manually completed
    MigrationCompleted,
    /// Migration completion failed
    MigrationCompleteFailed(String),
    /// Migration rollback initiated
    MigrationRolledBack,
    /// Migration rollback failed
    MigrationRollbackFailed(String),
    /// Migration was paused
    MigrationPaused,
    /// Migration pause failed
    MigrationPauseFailed(String),
    /// Migration was resumed
    MigrationResumed,
    /// Migration resume failed
    MigrationResumeFailed(String),
    /// Environment was toggled in blue-green migration
    EnvironmentToggled {
        previous_active: String,
        new_active: String,
    },
    /// Environment toggle failed
    EnvironmentToggleFailed(String),
}
