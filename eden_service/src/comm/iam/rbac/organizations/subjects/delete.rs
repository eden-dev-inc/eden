use crate::EdenDb;
use crate::comm::iam::rbac::resolve_subject_for_org;
use crate::comm::notifications::NotificationService;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::methods::user_notifications::NotificationSeverity;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::IdKind;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::response::EdenResponse;
use eden_core::telemetry::labels::TelemetryLabels;
use eden_core::telemetry::{AllMetrics, MetadataMapWrapper, TelemetryDurations};
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | subject_bits` on Organization
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["RBAC"],
    path="/iam/control/organizations/subjects/{subject}",
    operation_id = "delete_rbac_organization_subject",
    responses((status = OK, body = ControlPerms))
)]
// TODO: Refactor parameters into a request/context struct to reduce argument count.
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    input: web::Path<String>,
    database: web::Data<EdenDb>,
    // telemetry data
    metrics: web::Data<AllMetrics>,
    metadata: MetadataMapWrapper,
    labels: TelemetryLabels,
    durations: TelemetryDurations,
) -> Result<impl Responder, actix_web::Error> {
    let subject = input.into_inner();

    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());
    let org_uuid = org_cache.uuid();

    let resolved_subject = resolve_subject_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let perms = database
        .control_plane_get(org_uuid, IdKind::Organization, org_uuid, resolved_subject.kind, resolved_subject.uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let required_perms = ControlPerms::GRANT | perms;
    verify_control_perms(&database, &auth, None, required_perms, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let version_ms = chrono::Utc::now().timestamp_millis();

    database
        .control_plane_revoke(
            org_uuid,
            IdKind::Organization,
            org_uuid,
            resolved_subject.kind,
            resolved_subject.uuid,
            version_ms,
            0i64,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Notify the user that their access has been revoked
    let _ = NotificationService::notify_security_alert(
        &database,
        org_uuid,
        resolved_subject.uuid,
        "Access revoked",
        "Your access to this organization has been revoked by an administrator.",
        NotificationSeverity::Warning,
        None,
        None,
        telemetry_wrapper,
    )
    .await;

    EdenResponse::response(Response::new(perms)).into()
}

#[derive(Debug, PartialEq, Serialize, Deserialize, ToSchema)]
pub struct Response(ControlPerms);

impl Response {
    fn new(perms: ControlPerms) -> Self {
        Self(perms)
    }
}
