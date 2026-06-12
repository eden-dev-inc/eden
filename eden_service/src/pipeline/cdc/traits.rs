//! Trait abstractions for CDC source and destination databases.
//!
//! These traits decouple the worker loop from any specific database engine,
//! allowing the same backfill → stream → filter → buffer → flush pipeline
//! to work with PostgreSQL, MySQL, MongoDB, or any future endpoint type.

use super::buffer::{ChangeBatch, RowChange};
use super::filter::WhereFilter;
use async_trait::async_trait;
use eden_core::error::EpError;

/// A decoded change event from a CDC source, paired with its position marker.
#[derive(Debug, Clone)]
pub struct SourceEvent {
    /// The row change data (database-agnostic).
    pub change: RowChange,
    /// Position marker for checkpointing (e.g., Postgres LSN, MySQL binlog position, Mongo resume token).
    pub position: String,
}

/// Trait for reading change events from a database source.
///
/// Implementations handle connection management, protocol-specific decoding,
/// and position tracking internally. The worker interacts only through this
/// interface.
///
/// # Lifecycle
///
/// 1. Construct the source with database-specific configuration.
/// 2. Call [`setup`] once to create replication infrastructure.
/// 3. Optionally call [`backfill`] if no prior position exists.
/// 4. Call [`poll_changes`] in a loop to consume change events.
/// 5. Call [`teardown`] when deleting the entity to clean up infrastructure.
#[async_trait]
pub trait CdcSource: Send + Sync {
    /// Set up any required infrastructure (replication slots, publications, change streams, etc.).
    ///
    /// Called once before the worker loop starts. Implementations should be
    /// idempotent (safe to call if infrastructure already exists).
    async fn setup(&mut self) -> Result<(), EpError>;

    /// Perform initial backfill: capture the current position, SELECT matching
    /// rows, and return them as [`SourceEvent`]s along with the starting position.
    ///
    /// The returned `String` is the position from which streaming should begin.
    async fn backfill(&mut self, tables: &[String], filter: Option<&WhereFilter>) -> Result<(Vec<SourceEvent>, String), EpError>;

    /// Poll for new change events since the given position.
    ///
    /// Returns a (possibly empty) vector of events. Each event carries
    /// its own position marker for checkpointing.
    async fn poll_changes(&mut self, from_position: &str, batch_size: u32) -> Result<Vec<SourceEvent>, EpError>;

    /// Tear down infrastructure created by [`setup`] (replication slots, publications, etc.).
    ///
    /// Called when the entity is being deleted. Implementations should be
    /// idempotent (safe to call if infrastructure doesn't exist).
    async fn teardown(&mut self) -> Result<(), EpError>;
}

/// Trait for writing change batches to a destination database.
///
/// Implementations translate the generic [`ChangeBatch`] into
/// database-specific write operations (SQL INSERT/UPDATE/DELETE for
/// relational databases, document operations for NoSQL, etc.).
#[async_trait]
pub trait CdcDestination: Send + Sync {
    /// Write a batch of changes to the destination.
    async fn write_batch(&self, batch: &ChangeBatch) -> Result<(), EpError>;
}
