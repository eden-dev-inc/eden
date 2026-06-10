//! Template-based CDC source implementation.
//!
//! Composes with `PgCdcSource` for WAL streaming infrastructure (setup,
//! poll_changes, teardown) but performs backfill via a read template
//! rendered through the engine service. This makes the backfill query
//! dynamically configurable — updating the read template changes what
//! data is selected on the next backfill without restarting the pipeline.
use crate::EdenDb;

use super::buffer::{ChangeKind, RowChange};
use super::engine::execute_template_read;
use super::filter::WhereFilter;
#[cfg(not(embedded_db))]
use super::pg_source::PgCdcSource;
use super::postgres::ReplicationCommands;
use super::traits::{CdcSource, SourceEvent};
use async_trait::async_trait;
use database::template::TemplateOutput;
use eden_core::error::EpError;
use eden_core::format::OrganizationUuid;
use eden_core::format::TemplateUuid;
use eden_core::telemetry::TelemetryWrapper;
use eden_logger_internal::{LogAudience, log_error, log_info};
use endpoint_core::ep_core::database::template::TemplateFields;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use ep_runtime::comp::MyEngineService;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

/// CDC source that uses a read template for backfill and delegates
/// WAL streaming to the underlying `PgCdcSource`.
pub struct TemplateCdcSource {
    inner: PgCdcSource,
    read_template_uuid: Uuid,
    template_registry: Arc<TemplateRegistry>,
    engine_service: Arc<MyEngineService>,
    db: Arc<EdenDb>,
    org_uuid: OrganizationUuid,
    telemetry: TelemetryWrapper,
    ctx: eden_logger_internal::LogContext,
}

impl TemplateCdcSource {
    pub fn new(
        inner: PgCdcSource,
        read_template_uuid: Uuid,
        template_registry: Arc<TemplateRegistry>,
        engine_service: Arc<MyEngineService>,
        db: Arc<EdenDb>,
        org_uuid: OrganizationUuid,
        telemetry: TelemetryWrapper,
        ctx: eden_logger_internal::LogContext,
    ) -> Self {
        Self {
            inner,
            read_template_uuid,
            template_registry,
            engine_service,
            db,
            org_uuid,
            telemetry,
            ctx,
        }
    }
}

#[async_trait]
impl CdcSource for TemplateCdcSource {
    async fn setup(&mut self) -> Result<(), EpError> {
        self.inner.setup().await
    }

    async fn backfill(&mut self, tables: &[String], _filter: Option<&WhereFilter>) -> Result<(Vec<SourceEvent>, String), EpError> {
        let conn = self.db.pg_connection().await?;

        // Capture the current WAL position as the starting position for streaming
        let lsn_row = conn.query_one(ReplicationCommands::current_lsn(), &[]).await.map_err(EpError::database)?;
        let start_position: String = lsn_row.get("lsn");

        log_info!(
            self.ctx.clone(),
            "CDC template backfill: rendering read template",
            audience = LogAudience::Both,
            template_uuid = self.read_template_uuid.to_string(),
            tables = format!("{:?}", tables)
        );

        // Build template fields with table context
        let tables_json = Value::Array(tables.iter().map(|t| Value::String(t.clone())).collect());
        let fields = TemplateFields::new(vec![("tables".to_string(), tables_json)]);

        // Render the read template
        let template_uuid = TemplateUuid::from(self.read_template_uuid);
        let mut telemetry = self.telemetry.clone();
        let template_output =
            self.db.render_template(&self.template_registry, &template_uuid, &self.org_uuid, &fields, &mut telemetry).await?;

        // Execute the rendered read template via the engine
        let response = match template_output {
            TemplateOutput::Read(request) => {
                execute_template_read(&self.engine_service, &self.db, request, &self.org_uuid, &mut telemetry).await?
            }
            other => {
                return Err(EpError::parse(format!(
                    "read_template_uuid must reference a Read template, got {:?}",
                    std::mem::discriminant(&other)
                )));
            }
        };

        // Parse the response into SourceEvents
        let events = parse_read_response(response, tables, &start_position, &self.ctx);

        log_info!(
            self.ctx.clone(),
            "CDC template backfill completed",
            audience = LogAudience::Both,
            start_position = start_position.clone(),
            event_count = events.len()
        );

        Ok((events, start_position))
    }

    async fn poll_changes(&mut self, from_position: &str, batch_size: u32) -> Result<Vec<SourceEvent>, EpError> {
        self.inner.poll_changes(from_position, batch_size).await
    }

    async fn teardown(&mut self) -> Result<(), EpError> {
        self.inner.teardown().await
    }
}

/// Parse engine read response into CDC source events.
///
/// The response is expected to be either:
/// - A JSON array of row objects
/// - A JSON object with a data/rows/results key containing an array
fn parse_read_response(response: Value, tables: &[String], position: &str, ctx: &eden_logger_internal::LogContext) -> Vec<SourceEvent> {
    let table_name = tables.first().map(|t| t.as_str()).unwrap_or("unknown");

    let rows = match &response {
        Value::Array(arr) => arr.clone(),
        Value::Object(obj) => {
            // Try common response keys
            obj.get("data")
                .or_else(|| obj.get("rows"))
                .or_else(|| obj.get("results"))
                .and_then(|v| v.as_array())
                .cloned()
                .unwrap_or_else(|| {
                    // Treat the entire object as a single row
                    vec![response.clone()]
                })
        }
        _ => {
            log_error!(
                ctx.clone(),
                "CDC template backfill: unexpected response type",
                audience = LogAudience::Internal,
                response_type = format!("{}", response_type_name(&response))
            );
            vec![]
        }
    };

    rows.into_iter()
        .filter_map(|row| {
            if let Value::Object(map) = row {
                Some(SourceEvent {
                    change: RowChange {
                        table: table_name.to_string(),
                        kind: ChangeKind::Insert,
                        columns: map.into_iter().collect(),
                        old_columns: None,
                    },
                    position: position.to_string(),
                })
            } else {
                None
            }
        })
        .collect()
}

fn response_type_name(v: &Value) -> &'static str {
    match v {
        Value::Null => "null",
        Value::Bool(_) => "bool",
        Value::Number(_) => "number",
        Value::String(_) => "string",
        Value::Array(_) => "array",
        Value::Object(_) => "object",
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    #[test]
    fn test_parse_read_response_array() {
        let ctx = eden_logger_internal::LogContext::default();
        let response = json!([
            {"id": 1, "name": "Alice"},
            {"id": 2, "name": "Bob"}
        ]);

        let events = parse_read_response(response, &["users".to_string()], "0/1234", &ctx);
        assert_eq!(events.len(), 2);
        assert_eq!(events[0].change.table, "users");
        assert_eq!(events[0].change.kind, ChangeKind::Insert);
        assert_eq!(events[0].position, "0/1234");
    }

    #[test]
    fn test_parse_read_response_object_with_data() {
        let ctx = eden_logger_internal::LogContext::default();
        let response = json!({
            "data": [
                {"id": 1},
                {"id": 2}
            ]
        });

        let events = parse_read_response(response, &["orders".to_string()], "0/5678", &ctx);
        assert_eq!(events.len(), 2);
    }

    #[test]
    fn test_parse_read_response_single_object() {
        let ctx = eden_logger_internal::LogContext::default();
        let response = json!({"id": 1, "name": "Alice"});

        let events = parse_read_response(response, &["users".to_string()], "0/1234", &ctx);
        assert_eq!(events.len(), 1);
        assert_eq!(events[0].change.columns.get("name"), Some(&json!("Alice")));
    }

    #[test]
    fn test_parse_read_response_empty_array() {
        let ctx = eden_logger_internal::LogContext::default();
        let response = json!([]);

        let events = parse_read_response(response, &["t".to_string()], "0/0", &ctx);
        assert!(events.is_empty());
    }
}
