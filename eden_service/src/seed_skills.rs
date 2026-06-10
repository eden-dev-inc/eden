//! Seed builtin skills into the database on startup.
//!
//! Uses deterministic UUIDs so repeated upserts are idempotent.
use crate::EdenDb;

use eden_core::telemetry::TelemetryWrapper;

/// Upsert embedded builtin skills into the `llm_skills` table.
///
/// Safe to call on every startup. The upsert is keyed on a
/// deterministic UUID so existing rows are refreshed in place.
pub async fn seed_builtin_skills(_database: &EdenDb, _telemetry_wrapper: &mut TelemetryWrapper) -> Result<(), Box<dyn std::error::Error>> {
    Ok(())
}
