//! PostgreSQL CDC source using logical replication.
//!
//! Uses the `pgoutput` plugin to capture WAL changes. Backfill performs
//! direct SQL queries against the source tables. WAL streaming
//! infrastructure (replication slots, publications) is managed here;
//! the template layer composes with this source for template-driven backfill.
use crate::EdenDb;

use super::buffer::{ChangeKind, RowChange};
use super::filter::WhereFilter;
use super::postgres::{PgOutputEvent, RelationMap, ReplicationCommands, decode_pgoutput_message, quote_identifier};
use super::traits::{CdcSource, SourceEvent};
use async_trait::async_trait;
use eden_core::error::EpError;
use eden_logger_internal::{LogAudience, log_info, log_warn};
use serde_json::Value;
use std::sync::Arc;

/// PostgreSQL CDC source using logical replication.
pub struct PgCdcSource {
    db: Arc<EdenDb>,
    slot_name: String,
    publication_name: String,
    relations: RelationMap,
    ctx: eden_logger_internal::LogContext,
}

impl PgCdcSource {
    pub fn new(db: Arc<EdenDb>, slot_name: String, publication_name: String, ctx: eden_logger_internal::LogContext) -> Self {
        Self {
            db,
            slot_name,
            publication_name,
            relations: RelationMap::default(),
            ctx,
        }
    }
}

#[async_trait]
impl CdcSource for PgCdcSource {
    async fn setup(&mut self) -> Result<(), EpError> {
        let conn = self.db.pg_connection().await?;

        // Verify wal_level = logical
        let wal_row = conn.query_one(ReplicationCommands::check_wal_level(), &[]).await.map_err(EpError::database)?;
        let wal_level: String = wal_row.get(0);
        if wal_level != "logical" {
            return Err(EpError::parse(format!("Source database wal_level is '{wal_level}', must be 'logical' for CDC")));
        }

        // Create replication slot if it doesn't exist
        let check_slot_sql = ReplicationCommands::check_slot_exists(&self.slot_name);
        let slot_exists = conn.query_opt(&check_slot_sql, &[]).await.map_err(EpError::database)?;

        if slot_exists.is_none() {
            let create_slot_sql = ReplicationCommands::create_replication_slot(&self.slot_name);
            conn.execute(&create_slot_sql, &[]).await.map_err(EpError::database)?;
        }

        // Publication is created by the run handler before setup() since it
        // needs the table list from CdcConfig. This is intentional — the
        // source only manages the replication slot lifecycle.

        Ok(())
    }

    async fn backfill(&mut self, tables: &[String], filter: Option<&WhereFilter>) -> Result<(Vec<SourceEvent>, String), EpError> {
        let conn = self.db.pg_connection().await?;

        // Capture the current WAL position as the starting position for streaming
        let lsn_row = conn.query_one(ReplicationCommands::current_lsn(), &[]).await.map_err(EpError::database)?;
        let start_position: String = lsn_row.get("lsn");

        let mut events = Vec::new();

        for table in tables {
            let quoted_table = quote_identifier(table);
            let query = match filter {
                Some(f) => format!("SELECT row_to_json(t.*) AS row_data FROM {quoted_table} t WHERE {}", f.sql()),
                None => format!("SELECT row_to_json(t.*) AS row_data FROM {quoted_table} t"),
            };

            log_info!(
                self.ctx.clone(),
                "CDC backfill: querying table",
                audience = LogAudience::Internal,
                table = table.clone(),
                has_filter = filter.is_some()
            );

            let rows = conn.query(&query, &[]).await.map_err(EpError::database)?;

            for row in &rows {
                let json_val: Value = row.get("row_data");
                if let Value::Object(map) = json_val {
                    events.push(SourceEvent {
                        change: RowChange {
                            table: table.clone(),
                            kind: ChangeKind::Insert,
                            columns: map.into_iter().collect(),
                            old_columns: None,
                        },
                        position: start_position.clone(),
                    });
                }
            }

            log_info!(
                self.ctx.clone(),
                "CDC backfill: table complete",
                audience = LogAudience::Both,
                table = table.clone(),
                rows = rows.len()
            );
        }

        Ok((events, start_position))
    }

    async fn poll_changes(&mut self, from_position: &str, batch_size: u32) -> Result<Vec<SourceEvent>, EpError> {
        let conn = self.db.pg_connection().await?;

        let escaped_slot = self.slot_name.replace('\'', "''");
        let escaped_lsn = from_position.replace('\'', "''");
        let escaped_pub = self.publication_name.replace('\'', "''");
        let query = format!(
            "SELECT lsn::text, data FROM pg_logical_slot_get_changes('{escaped_slot}', '{escaped_lsn}', {batch_size}, 'proto_version', '1', 'publication_names', '{escaped_pub}')",
        );

        let rows = conn.query(&query, &[]).await.map_err(EpError::database)?;

        let mut events = Vec::with_capacity(rows.len());
        for row in &rows {
            let lsn: String = row.get(0);
            let data: &[u8] = row.get(1);

            match decode_pgoutput_message(data, &mut self.relations) {
                Ok(Some(PgOutputEvent::Change(change))) => {
                    events.push(SourceEvent { change, position: lsn });
                }
                Ok(Some(_)) => {} // Metadata message (Relation, Begin, Commit), skip
                Ok(None) => {}
                Err(e) => {
                    log_warn!(
                        self.ctx.clone(),
                        "Failed to decode WAL message",
                        audience = LogAudience::Internal,
                        error = e.to_string()
                    );
                }
            }
        }

        Ok(events)
    }

    async fn teardown(&mut self) -> Result<(), EpError> {
        let conn = self.db.pg_connection().await?;

        // Drop replication slot (ignore "does not exist")
        let drop_slot = ReplicationCommands::drop_replication_slot(&self.slot_name);
        if let Err(e) = conn.execute(&drop_slot, &[]).await {
            let msg = e.to_string();
            if !msg.contains("does not exist") {
                return Err(EpError::database(e));
            }
        }

        // Drop publication (ignore "does not exist")
        let drop_pub = ReplicationCommands::drop_publication(&self.publication_name);
        if let Err(e) = conn.execute(&drop_pub, &[]).await {
            let msg = e.to_string();
            if !msg.contains("does not exist") {
                return Err(EpError::database(e));
            }
        }

        Ok(())
    }
}
