//! API endpoints for user notifications (alerts, updates, recommendations).
//!
//! These are general-purpose notifications separate from LLM agent notifications.
//! They include system updates, new services, recommendations, security alerts, etc.

use crate::EdenDb;

use actix_web::{HttpResponse, Responder, web};
use eden_core::auth::ParsedJwt;
use eden_core::format::EdenUuid;
use serde::{Deserialize, Serialize};
use telemetry_extensions_macro::with_telemetry;
use uuid::Uuid;

use crate::error_handling;

/// Query parameters for listing notifications.
#[derive(Debug, Deserialize)]
pub struct NotificationsQuery {
    #[serde(default = "default_limit")]
    pub limit: i64,
}

fn default_limit() -> i64 {
    50
}

/// Response for a single user notification.
#[derive(Debug, Serialize)]
pub struct UserNotificationResponse {
    pub id: Uuid,
    pub kind: String,
    pub category: String,
    pub severity: String,
    pub title: String,
    pub body: String,
    pub action_url: Option<String>,
    pub action_label: Option<String>,
    pub read: bool,
    pub created_at: String,
}

/// List user notifications for the current user.
///
/// Returns notifications sorted by created_at descending (newest first).
#[with_telemetry]
pub async fn list_user_notifications(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    query: web::Query<NotificationsQuery>,
) -> Result<impl Responder, actix_web::Error> {
    let notifications = database
        .load_user_notifications(auth.user_uuid().uuid(), query.limit.min(100), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let response: Vec<UserNotificationResponse> = notifications
        .iter()
        .map(|n| UserNotificationResponse {
            id: n.id,
            kind: n.kind.clone(),
            category: n.category.clone(),
            severity: n.severity.clone(),
            title: n.title.clone(),
            body: n.body.clone(),
            action_url: n.action_url.clone(),
            action_label: n.action_label.clone(),
            read: n.read,
            created_at: n.created_at.to_rfc3339(),
        })
        .collect();

    Ok(HttpResponse::Ok().json(response))
}

/// Mark a user notification as read.
#[with_telemetry]
pub async fn mark_user_notification_read(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    path: web::Path<Uuid>,
) -> Result<impl Responder, actix_web::Error> {
    database
        .mark_user_notification_read(path.into_inner(), auth.user_uuid().uuid(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    Ok(HttpResponse::Ok().finish())
}

/// Mark all user notifications as read.
#[with_telemetry]
pub async fn mark_all_user_notifications_read(
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    database
        .mark_all_user_notifications_read(auth.user_uuid().uuid(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    Ok(HttpResponse::Ok().finish())
}

// =============================================================================
// Notification Service - for creating notifications from other parts of the app
// =============================================================================

use database::db::methods::user_notifications::{NotificationCategory, NotificationKind, NotificationSeverity};
use eden_core::telemetry::TelemetryWrapper;

/// Service for creating user notifications from various parts of the application.
pub struct NotificationService;

impl NotificationService {
    /// Create a system update notification for all users in an organization.
    pub async fn notify_system_update(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        title: &str,
        body: &str,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            NotificationKind::SystemUpdate.as_str(),
            NotificationCategory::General.as_str(),
            NotificationSeverity::Info.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }

    /// Create a new service notification.
    pub async fn notify_new_service(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        title: &str,
        body: &str,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            NotificationKind::NewService.as_str(),
            NotificationCategory::Endpoints.as_str(),
            NotificationSeverity::Info.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }

    /// Create a recommendation notification.
    pub async fn notify_recommendation(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        title: &str,
        body: &str,
        category: NotificationCategory,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            NotificationKind::Recommendation.as_str(),
            category.as_str(),
            NotificationSeverity::Info.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }

    /// Create a security alert notification.
    pub async fn notify_security_alert(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        title: &str,
        body: &str,
        severity: NotificationSeverity,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            NotificationKind::Security.as_str(),
            NotificationCategory::Security.as_str(),
            severity.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }

    /// Create a billing notification.
    pub async fn notify_billing(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        title: &str,
        body: &str,
        severity: NotificationSeverity,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            NotificationKind::Billing.as_str(),
            NotificationCategory::Billing.as_str(),
            severity.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }

    /// Create a maintenance notification.
    pub async fn notify_maintenance(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        title: &str,
        body: &str,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            NotificationKind::Maintenance.as_str(),
            NotificationCategory::General.as_str(),
            NotificationSeverity::Warning.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }

    /// Create a new feature notification.
    pub async fn notify_new_feature(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        title: &str,
        body: &str,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            NotificationKind::Feature.as_str(),
            NotificationCategory::Features.as_str(),
            NotificationSeverity::Info.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }

    /// Create a custom notification with full control over all fields.
    #[allow(clippy::too_many_arguments)]
    pub async fn notify_custom(
        db: &EdenDb,
        organization_uuid: Uuid,
        user_uuid: Uuid,
        kind: NotificationKind,
        category: NotificationCategory,
        severity: NotificationSeverity,
        title: &str,
        body: &str,
        action_url: Option<&str>,
        action_label: Option<&str>,
        telemetry_wrapper: &mut TelemetryWrapper,
    ) -> Result<(), eden_core::error::EpError> {
        db.insert_user_notification(
            Uuid::new_v4(),
            user_uuid,
            organization_uuid,
            kind.as_str(),
            category.as_str(),
            severity.as_str(),
            title,
            body,
            action_url,
            action_label,
            telemetry_wrapper,
        )
        .await
    }
}
