//! # Analytics dashboard preferences (per-user, per-org)
//!
//! Durable storage for a single opaque JSON blob of a user's analytics-dashboard
//! UI state (saved y-axis ranges, named color ranges, etc.). The dashboard owns
//! the schema of the blob; the backend treats `prefs` as an opaque string and
//! only scopes it by `(user_uuid, organization_uuid)`.
//!
//! Deliberately lean: no cache layer and no `Table`/`FromRow` machinery. The two
//! operations run portable SQL directly through [`DatabaseManager::pg_connection`],
//! which works in both the standard Postgres and `embedded_db` (Turso) backends —
//! the Turso layer rewrites `$N` placeholders to `?N` at runtime, and the SQL
//! uses only the portable subset (`TEXT`/`uuid` column types, `ON CONFLICT … DO
//! UPDATE … excluded`). The table itself is created at startup by
//! `initialize_database` (see `methods::create::CREATE_ANALYTICS_DASHBOARD_PREFS`).

use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use eden_core::error::{EpError, ResultEP};
use std::future::Future;
use uuid::Uuid;

/// Read/upsert a user's analytics dashboard preference blob.
pub trait AnalyticsPrefsStore {
    /// Fetch the stored preference blob for `(user_uuid, org_uuid)`, or `None`
    /// if the user has never saved any.
    fn get_analytics_prefs(&self, user_uuid: Uuid, org_uuid: Uuid) -> impl Future<Output = ResultEP<Option<String>>> + Send;

    /// Insert or replace the user's preference blob. `updated_at` is an RFC3339
    /// timestamp supplied by the caller (stored as text for backend portability).
    fn upsert_analytics_prefs(
        &self,
        user_uuid: Uuid,
        org_uuid: Uuid,
        prefs: &str,
        updated_at: &str,
    ) -> impl Future<Output = ResultEP<()>> + Send;
}

/// `SELECT` the opaque prefs blob scoped to one user within one organization.
const SELECT_ANALYTICS_PREFS: &str = "SELECT prefs FROM analytics_dashboard_prefs WHERE user_uuid = $1 AND organization_uuid = $2";

/// Upsert: insert the blob, or replace it on the `(user_uuid, organization_uuid)`
/// primary key. Uses `excluded` (supported by both Postgres and SQLite/Turso) to
/// avoid re-binding parameters.
const UPSERT_ANALYTICS_PREFS: &str = "INSERT INTO analytics_dashboard_prefs (user_uuid, organization_uuid, prefs, updated_at) \
     VALUES ($1, $2, $3, $4) \
     ON CONFLICT (user_uuid, organization_uuid) \
     DO UPDATE SET prefs = excluded.prefs, updated_at = excluded.updated_at";

impl<R, P, C> AnalyticsPrefsStore for DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    async fn get_analytics_prefs(&self, user_uuid: Uuid, org_uuid: Uuid) -> ResultEP<Option<String>> {
        let conn = self.pg_connection().await?;
        let row = conn
            .query_opt(SELECT_ANALYTICS_PREFS, &[&user_uuid, &org_uuid])
            .await
            .map_err(|e| EpError::database(format!("Failed to load analytics dashboard prefs: {e}")))?;
        Ok(row.map(|row| row.get("prefs")))
    }

    async fn upsert_analytics_prefs(&self, user_uuid: Uuid, org_uuid: Uuid, prefs: &str, updated_at: &str) -> ResultEP<()> {
        let conn = self.pg_connection().await?;
        conn.execute(UPSERT_ANALYTICS_PREFS, &[&user_uuid, &org_uuid, &prefs, &updated_at])
            .await
            .map_err(|e| EpError::database(format!("Failed to save analytics dashboard prefs: {e}")))?;
        Ok(())
    }
}
