//! Template-based CDC destination implementation.
//!
//! Replaces `PgCdcDestination` — instead of generating SQL INSERT/UPDATE/DELETE
//! statements, each row change is rendered through a write template and executed
//! via the engine service. This makes the destination fully database-agnostic:
//! a Postgres source can write to MongoDB, Redis, or any endpoint type.
//!
//! Templates are looked up from the registry at render time, so updating a
//! write template via the API immediately changes how the next batch is written.
use crate::EdenDb;

use super::buffer::{ChangeBatch, ChangeKind};
use super::engine::execute_template_write;
use super::traits::CdcDestination;
use async_trait::async_trait;
use database::template::TemplateOutput;
use eden_core::error::EpError;
use eden_core::format::OrganizationUuid;
use eden_core::format::TemplateUuid;
use eden_core::telemetry::TelemetryWrapper;
use endpoint_core::ep_core::database::template::TemplateFields;
use endpoint_core::ep_core::database::template::registry::TemplateRegistry;
use ep_runtime::comp::MyEngineService;
use serde_json::Value;
use std::sync::Arc;
use uuid::Uuid;

/// CDC destination that writes changes via a rendered write template.
pub struct TemplateCdcDestination {
    write_template_uuid: Uuid,
    template_registry: Arc<TemplateRegistry>,
    engine_service: Arc<MyEngineService>,
    db: Arc<EdenDb>,
    org_uuid: OrganizationUuid,
    telemetry: TelemetryWrapper,
    _ctx: eden_logger_internal::LogContext,
}

impl TemplateCdcDestination {
    pub fn new(
        write_template_uuid: Uuid,
        template_registry: Arc<TemplateRegistry>,
        engine_service: Arc<MyEngineService>,
        db: Arc<EdenDb>,
        org_uuid: OrganizationUuid,
        telemetry: TelemetryWrapper,
        ctx: eden_logger_internal::LogContext,
    ) -> Self {
        Self {
            write_template_uuid,
            template_registry,
            engine_service,
            db,
            org_uuid,
            telemetry,
            _ctx: ctx,
        }
    }
}

#[async_trait]
impl CdcDestination for TemplateCdcDestination {
    async fn write_batch(&self, batch: &ChangeBatch) -> Result<(), EpError> {
        if batch.changes.is_empty() {
            return Ok(());
        }

        let template_uuid = TemplateUuid::from(self.write_template_uuid);

        for change in &batch.changes {
            // Build template fields from the row change
            let mut field_vec: Vec<(String, Value)> = change.columns.iter().map(|(k, v)| (k.clone(), v.clone())).collect();

            // Add metadata fields for template authors
            field_vec.push(("_table".to_string(), Value::String(change.table.clone())));
            field_vec.push(("_kind".to_string(), Value::String(change_kind_str(&change.kind).to_string())));

            if let Some(ref old_cols) = change.old_columns {
                let old_obj: serde_json::Map<String, Value> = old_cols.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
                field_vec.push(("_old_columns".to_string(), Value::Object(old_obj)));
            }

            let fields = TemplateFields::new(field_vec);

            // Render the write template
            let mut telemetry = self.telemetry.clone();
            let template_output =
                self.db.render_template(&self.template_registry, &template_uuid, &self.org_uuid, &fields, &mut telemetry).await?;

            // Execute the rendered write template via the engine
            match template_output {
                TemplateOutput::Write(request) => {
                    execute_template_write(&self.engine_service, &self.db, request, &self.org_uuid, &mut telemetry).await?;
                }
                other => {
                    return Err(EpError::parse(format!(
                        "write_template_uuid must reference a Write template, got {:?}",
                        std::mem::discriminant(&other)
                    )));
                }
            }
        }

        Ok(())
    }
}

fn change_kind_str(kind: &ChangeKind) -> &'static str {
    match kind {
        ChangeKind::Insert => "insert",
        ChangeKind::Update => "update",
        ChangeKind::Delete => "delete",
    }
}

#[cfg(test)]
mod tests {
    use super::super::buffer::RowChange;
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_change_kind_str() {
        assert_eq!(change_kind_str(&ChangeKind::Insert), "insert");
        assert_eq!(change_kind_str(&ChangeKind::Update), "update");
        assert_eq!(change_kind_str(&ChangeKind::Delete), "delete");
    }

    #[test]
    fn test_row_change_to_template_fields() {
        let change = RowChange {
            table: "public.orders".to_string(),
            kind: ChangeKind::Insert,
            columns: {
                let mut m = HashMap::new();
                m.insert("id".to_string(), json!(1));
                m.insert("name".to_string(), json!("Alice"));
                m
            },
            old_columns: None,
        };

        let mut field_vec: Vec<(String, Value)> = change.columns.iter().map(|(k, v)| (k.clone(), v.clone())).collect();
        field_vec.push(("_table".to_string(), Value::String(change.table.clone())));
        field_vec.push(("_kind".to_string(), Value::String("insert".to_string())));

        let fields = TemplateFields::new(field_vec);

        // Verify we can access the fields
        assert!(fields.contains_key("id"));
        assert!(fields.contains_key("name"));
        assert!(fields.contains_key("_table"));
        assert!(fields.contains_key("_kind"));
        assert_eq!(fields.get("_table"), Some(&Value::String("public.orders".to_string())));
    }
}
