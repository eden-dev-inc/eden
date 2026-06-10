use crate::db::lib::{DatabaseManager, EdenClickhouseConnection, EdenPostgresConnection, EdenRedisConnection};
use crate::sql_file;
use chrono::{DateTime, Utc};
use eden_core::error::{EpError, ResultEP};
use eden_core::telemetry::FastSpanStatus;
use eden_core::telemetry::TelemetryWrapper;
#[cfg(embedded_db)]
use ep_core::database::schema::Row;
use function_name::named;
use std::borrow::Cow;
#[cfg(not(embedded_db))]
use tokio_postgres::Row;
use uuid::Uuid;

/// Notification kind for categorizing user alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationKind {
    /// System-wide updates (e.g., new features, maintenance)
    SystemUpdate,
    /// New service or endpoint available
    NewService,
    /// Optimization or usage recommendations
    Recommendation,
    /// Security-related alerts
    Security,
    /// Billing or subscription alerts
    Billing,
    /// Scheduled maintenance notifications
    Maintenance,
    /// New feature announcements
    Feature,
}

impl NotificationKind {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::SystemUpdate => "system_update",
            Self::NewService => "new_service",
            Self::Recommendation => "recommendation",
            Self::Security => "security",
            Self::Billing => "billing",
            Self::Maintenance => "maintenance",
            Self::Feature => "feature",
        }
    }
}

/// Notification category for grouping alerts.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationCategory {
    General,
    Endpoints,
    Analytics,
    Security,
    Billing,
    Features,
}

impl NotificationCategory {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::General => "general",
            Self::Endpoints => "endpoints",
            Self::Analytics => "analytics",
            Self::Security => "security",
            Self::Billing => "billing",
            Self::Features => "features",
        }
    }
}

/// Notification severity level.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NotificationSeverity {
    Info,
    Warning,
    Critical,
}

impl NotificationSeverity {
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::Info => "info",
            Self::Warning => "warning",
            Self::Critical => "critical",
        }
    }
}

/// A stored user notification.
#[derive(Debug, Clone)]
pub struct StoredUserNotification {
    pub id: Uuid,
    pub user_uuid: Uuid,
    pub organization_uuid: Uuid,
    pub kind: String,
    pub category: String,
    pub severity: String,
    pub title: String,
    pub body: String,
    pub action_url: Option<String>,
    pub action_label: Option<String>,
    pub read: bool,
    pub created_at: DateTime<Utc>,
}

fn row_to_user_notification(row: &Row) -> StoredUserNotification {
    StoredUserNotification {
        id: row.get("id"),
        user_uuid: row.get("user_uuid"),
        organization_uuid: row.get("organization_uuid"),
        kind: row.get("kind"),
        category: row.get("category"),
        severity: row.get("severity"),
        title: row.get("title"),
        body: row.get("body"),
        action_url: row.get("action_url"),
        action_label: row.get("action_label"),
        read: row.get("read"),
        created_at: row.get("created_at"),
    }
}

impl<R, P, C> DatabaseManager<R, P, C>
where
    R: EdenRedisConnection + Sync,
    P: EdenPostgresConnection + Sync,
    C: EdenClickhouseConnection + Sync,
{
    /// Insert a new user notification.
    #[named]
    #[allow(clippy::too_many_arguments)]
    pub async fn insert_user_notification(
        &self,
        id: Uuid,
        user_uuid: Uuid,
        organization_uuid: Uuid,
        kind: &str,
        category: &str,
        severity: &str,
        title: &str,
        body: &str,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(
            sql_file!("insert", "user_notification"),
            &[
                &id,
                &user_uuid,
                &organization_uuid,
                &kind,
                &category,
                &severity,
                &title,
                &body,
                &action_url,
                &action_label,
            ],
        )
        .await
        .map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    /// Load user notifications for a specific user.
    #[named]
    pub async fn load_user_notifications(
        &self,
        user_uuid: Uuid,
        limit: i64,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<Vec<StoredUserNotification>> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        let rows = conn.query(sql_file!("select", "user_notifications_for_user"), &[&user_uuid, &limit]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(rows.iter().map(row_to_user_notification).collect())
    }

    /// Mark a user notification as read.
    #[named]
    pub async fn mark_user_notification_read(
        &self,
        notification_id: Uuid,
        user_uuid: Uuid,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("update", "user_notification_read"), &[&notification_id, &user_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }

    /// Mark all user notifications as read for a specific user.
    #[named]
    pub async fn mark_all_user_notifications_read(&self, user_uuid: Uuid, telemetry_wrapper: &mut TelemetryWrapper) -> ResultEP<()> {
        let mut span = telemetry_wrapper.client_tracer(function_name!().to_string());
        let conn = self.pg_connection().await.inspect_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
        })?;

        conn.execute(sql_file!("update", "user_notifications_read_all"), &[&user_uuid]).await.map_err(|e| {
            span.set_status(FastSpanStatus::Error { message: Cow::Owned(e.to_string()) });
            EpError::database(e)
        })?;

        Ok(())
    }
}
