//! CDC Worker: drives the change consumption, filtering, buffering, and destination write loop.
//!
//! The worker lifecycle:
//! 1. **Backfill** (if no prior position): snapshot current position, SELECT matching rows, bulk write
//! 2. **Stream**: poll for change events via the [`CdcSource`] trait
//! 3. **Filter**: evaluate each row change against the SQL WHERE clause
//! 4. **Buffer**: accumulate matching changes until batch size or flush interval
//! 5. **Flush**: write batch to destination via the [`CdcDestination`] trait, checkpoint position
//! 6. **Loop** until shutdown signal
//!
//! The worker is database-agnostic: all source/destination specifics are behind traits.
use crate::EdenDb;

use super::buffer::{ChangeBatch, ChangeBuffer, ChangeKind, RowChange};
use super::filter::WhereFilter;
use super::traits::{CdcDestination, CdcSource};
use eden_core::error::EpError;
use eden_logger_internal::{LogAudience, log_error, log_info, log_warn};
use endpoint_core::ep_core::database::schema::pipeline::PipelineSchema;
use endpoint_core::ep_core::database::schema::snapshot::{CdcConfig, SnapshotSchema};
use std::sync::Arc;
use tokio::sync::broadcast;

use crate::pipeline::manager::CdcSignal;

/// Which database table this CDC worker manages state in.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum CdcEntityKind {
    Snapshot,
    Pipeline,
}

/// Configuration for the CDC worker (database-agnostic).
#[derive(Debug, Clone)]
pub struct CdcWorkerConfig {
    pub entity_uuid: uuid::Uuid,
    pub entity_kind: CdcEntityKind,
    pub tables: Vec<String>,
    pub batch_size: u32,
    pub flush_interval_ms: u64,
    pub include_deletes: bool,
    pub filter: Option<WhereFilter>,
    /// Position to resume from (e.g., Postgres LSN, MySQL binlog position).
    pub last_position: Option<String>,
    /// Read template UUID for backfill/selection queries.
    pub read_template_uuid: uuid::Uuid,
    /// Write template UUID for destination writes.
    pub write_template_uuid: uuid::Uuid,
}

impl CdcWorkerConfig {
    /// Build worker config from a snapshot schema and its CDC config.
    pub fn from_snapshot(schema: &SnapshotSchema, cdc_config: &CdcConfig) -> Result<Self, EpError> {
        let filter = match schema.filter() {
            Some(f) if !f.is_empty() => Some(WhereFilter::parse(f)?),
            _ => None,
        };

        let read_template_uuid = schema.read_template_uuid().ok_or_else(|| EpError::parse("CDC snapshot requires read_template_uuid"))?;
        let write_template_uuid =
            schema.write_template_uuid().ok_or_else(|| EpError::parse("CDC snapshot requires write_template_uuid"))?;

        Ok(Self {
            entity_uuid: *schema.uuid(),
            entity_kind: CdcEntityKind::Snapshot,
            tables: cdc_config.tables.clone(),
            batch_size: cdc_config.batch_size,
            flush_interval_ms: cdc_config.flush_interval_ms,
            include_deletes: cdc_config.include_deletes,
            filter,
            last_position: schema.last_lsn().clone(),
            read_template_uuid,
            write_template_uuid,
        })
    }

    /// Build worker config from a pipeline schema (always CDC).
    pub fn from_pipeline(schema: &PipelineSchema) -> Result<Self, EpError> {
        let filter = match schema.filter() {
            Some(f) if !f.is_empty() => Some(WhereFilter::parse(f)?),
            _ => None,
        };

        let cdc_config = schema.cdc_config();

        let read_template_uuid = schema.read_template_uuid().ok_or_else(|| EpError::parse("CDC pipeline requires read_template_uuid"))?;
        let write_template_uuid =
            schema.write_template_uuid().ok_or_else(|| EpError::parse("CDC pipeline requires write_template_uuid"))?;

        Ok(Self {
            entity_uuid: *schema.uuid(),
            entity_kind: CdcEntityKind::Pipeline,
            tables: cdc_config.tables.clone(),
            batch_size: cdc_config.batch_size,
            flush_interval_ms: cdc_config.flush_interval_ms,
            include_deletes: cdc_config.include_deletes,
            filter,
            last_position: schema.last_lsn().clone(),
            read_template_uuid,
            write_template_uuid,
        })
    }
}

/// Run the CDC worker loop.
///
/// This is the main entry point spawned by the run handler. It:
/// 1. Performs backfill if no prior position exists
/// 2. Polls for change events in a loop via the [`CdcSource`]
/// 3. Filters, buffers, and flushes changes via the [`CdcDestination`]
/// 4. Checkpoints position after each successful flush
/// 5. Exits on shutdown signal
pub async fn run_cdc_worker(
    config: CdcWorkerConfig,
    mut source: Box<dyn CdcSource>,
    destination: Box<dyn CdcDestination>,
    db: Arc<EdenDb>,
    mut signal_rx: broadcast::Receiver<CdcSignal>,
    ctx: eden_logger_internal::LogContext,
) {
    log_info!(
        ctx.clone(),
        "CDC worker starting",
        audience = LogAudience::Both,
        entity_uuid = config.entity_uuid.to_string(),
        entity_kind = format!("{:?}", config.entity_kind),
        tables = format!("{:?}", config.tables)
    );

    // Phase 1: Backfill if no prior position
    let start_position = match &config.last_position {
        Some(pos) => {
            log_info!(
                ctx.clone(),
                "CDC worker resuming from position",
                audience = LogAudience::Both,
                position = pos.clone()
            );
            pos.clone()
        }
        None => {
            log_info!(ctx.clone(), "CDC worker performing initial backfill", audience = LogAudience::Both);
            match source.backfill(&config.tables, config.filter.as_ref()).await {
                Ok((events, start_pos)) => {
                    // Write backfill events to destination
                    if !events.is_empty() {
                        let changes: Vec<RowChange> = events.iter().map(|e| e.change.clone()).collect();
                        let batch = ChangeBatch { changes, confirm_lsn: start_pos.clone() };
                        if let Err(e) = destination.write_batch(&batch).await {
                            log_error!(
                                ctx.clone(),
                                "CDC backfill write failed, worker stopping",
                                audience = LogAudience::Both,
                                error = e.to_string()
                            );
                            update_status_failed(&db, &config).await;
                            return;
                        }
                    }
                    checkpoint_position(&db, &config, &start_pos).await;
                    log_info!(
                        ctx.clone(),
                        "CDC backfill completed",
                        audience = LogAudience::Both,
                        start_position = start_pos.clone()
                    );
                    start_pos
                }
                Err(e) => {
                    log_error!(
                        ctx.clone(),
                        "CDC backfill failed, worker stopping",
                        audience = LogAudience::Both,
                        error = e.to_string()
                    );
                    update_status_failed(&db, &config).await;
                    return;
                }
            }
        }
    };

    // Phase 2: Change streaming loop
    let mut buffer = ChangeBuffer::new(config.batch_size, config.flush_interval_ms);
    let mut current_position = start_position;
    let poll_interval = std::time::Duration::from_millis(config.flush_interval_ms.max(1000));

    loop {
        tokio::select! {
            signal = signal_rx.recv() => {
                match signal {
                    Ok(CdcSignal::Shutdown) | Err(_) => {
                        log_info!(
                            ctx.clone(),
                            "CDC worker received shutdown signal",
                            audience = LogAudience::Both,
                            entity_uuid = config.entity_uuid.to_string()
                        );
                        // Flush remaining buffered changes before shutting down
                        if let Some(batch) = buffer.take_batch() {
                            if let Err(e) = destination.write_batch(&batch).await {
                                log_error!(
                                    ctx.clone(),
                                    "CDC worker failed to flush on shutdown",
                                    audience = LogAudience::Both,
                                    error = e.to_string()
                                );
                            } else {
                                checkpoint_position(&db, &config, &batch.confirm_lsn).await;
                            }
                        }
                        break;
                    }
                }
            }
            _ = tokio::time::sleep(poll_interval) => {
                // Poll for new change events
                match source.poll_changes(&current_position, config.batch_size).await {
                    Ok(events) => {
                        for event in events {
                            if let Some(change) = filter_change(event.change, &config) {
                                buffer.push(change, event.position.clone());
                                current_position = event.position;
                            }
                        }
                    }
                    Err(e) => {
                        log_warn!(
                            ctx.clone(),
                            "CDC poll error, will retry",
                            audience = LogAudience::Internal,
                            error = e.to_string()
                        );
                    }
                }

                // Flush if needed
                if buffer.should_flush() {
                    if let Some(batch) = buffer.take_batch() {
                        match destination.write_batch(&batch).await {
                            Ok(()) => {
                                checkpoint_position(&db, &config, &batch.confirm_lsn).await;
                                current_position = batch.confirm_lsn.clone();
                            }
                            Err(e) => {
                                log_error!(
                                    ctx.clone(),
                                    "CDC flush failed",
                                    audience = LogAudience::Both,
                                    error = e.to_string(),
                                    batch_size = batch.changes.len()
                                );
                                // Don't checkpoint — changes will be re-fetched on next poll
                            }
                        }
                    }
                }
            }
        }
    }

    // Update status to Paused on clean shutdown
    if let Ok(conn) = db.pg_connection().await {
        let table = match config.entity_kind {
            CdcEntityKind::Snapshot => "snapshots",
            CdcEntityKind::Pipeline => "pipelines",
        };
        let query = format!("UPDATE {table} SET status = 'Paused', updated_at = NOW() WHERE uuid = $1");
        let _ = conn.execute(&query, &[&config.entity_uuid]).await;
    }

    log_info!(
        ctx,
        "CDC worker stopped",
        audience = LogAudience::Both,
        entity_uuid = config.entity_uuid.to_string()
    );
}

/// Apply delete-skip and WHERE filter to a row change.
///
/// Returns `Some(RowChange)` if the change passes, `None` otherwise.
fn filter_change(change: RowChange, config: &CdcWorkerConfig) -> Option<RowChange> {
    // Skip deletes if not configured to propagate them
    if change.kind == ChangeKind::Delete && !config.include_deletes {
        return None;
    }

    // Apply the WHERE filter
    if let Some(ref filter) = config.filter {
        match filter.evaluate(&change.columns) {
            Ok(true) => Some(change),
            Ok(false) => None,
            Err(_) => None, // Filter evaluation error → skip row
        }
    } else {
        Some(change)
    }
}

/// Checkpoint the confirmed position in the entity's table.
async fn checkpoint_position(db: &EdenDb, config: &CdcWorkerConfig, position: &str) {
    let table = match config.entity_kind {
        CdcEntityKind::Snapshot => "snapshots",
        CdcEntityKind::Pipeline => "pipelines",
    };
    if let Ok(conn) = db.pg_connection().await {
        let query = format!("UPDATE {table} SET last_lsn = $1, updated_at = NOW() WHERE uuid = $2");
        let _ = conn.execute(&query, &[&position, &config.entity_uuid]).await;
    }
}

/// Update entity status to Failed.
async fn update_status_failed(db: &EdenDb, config: &CdcWorkerConfig) {
    let table = match config.entity_kind {
        CdcEntityKind::Snapshot => "snapshots",
        CdcEntityKind::Pipeline => "pipelines",
    };
    if let Ok(conn) = db.pg_connection().await {
        let query = format!("UPDATE {table} SET status = 'Failed', updated_at = NOW() WHERE uuid = $1");
        let _ = conn.execute(&query, &[&config.entity_uuid]).await;
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    fn make_config(filter: Option<&str>, include_deletes: bool) -> CdcWorkerConfig {
        CdcWorkerConfig {
            entity_uuid: uuid::Uuid::new_v4(),
            entity_kind: CdcEntityKind::Snapshot,
            tables: vec!["t".into()],
            batch_size: 100,
            flush_interval_ms: 5000,
            include_deletes,
            filter: filter.map(|f| WhereFilter::parse(f).expect("parse filter")),
            last_position: None,
            read_template_uuid: uuid::Uuid::new_v4(),
            write_template_uuid: uuid::Uuid::new_v4(),
        }
    }

    #[test]
    fn test_filter_change_pass() {
        let config = make_config(Some("id > 0"), true);
        let change = RowChange {
            table: "t".into(),
            kind: ChangeKind::Insert,
            columns: {
                let mut m = HashMap::new();
                m.insert("id".into(), json!(5));
                m
            },
            old_columns: None,
        };

        assert!(filter_change(change, &config).is_some());
    }

    #[test]
    fn test_filter_change_reject() {
        let config = make_config(Some("id > 10"), true);
        let change = RowChange {
            table: "t".into(),
            kind: ChangeKind::Insert,
            columns: {
                let mut m = HashMap::new();
                m.insert("id".into(), json!(5));
                m
            },
            old_columns: None,
        };

        assert!(filter_change(change, &config).is_none());
    }

    #[test]
    fn test_filter_change_skip_deletes() {
        let config = make_config(None, false);
        let change = RowChange {
            table: "t".into(),
            kind: ChangeKind::Delete,
            columns: HashMap::new(),
            old_columns: Some({
                let mut m = HashMap::new();
                m.insert("id".into(), json!(1));
                m
            }),
        };

        assert!(filter_change(change, &config).is_none());
    }

    #[test]
    fn test_filter_change_no_filter() {
        let config = make_config(None, true);
        let change = RowChange {
            table: "t".into(),
            kind: ChangeKind::Insert,
            columns: {
                let mut m = HashMap::new();
                m.insert("id".into(), json!(42));
                m
            },
            old_columns: None,
        };

        assert!(filter_change(change, &config).is_some());
    }
}
