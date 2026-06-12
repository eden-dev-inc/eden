use crate::EdenDb;
use crate::comm::iam::SubjectInput;
use crate::comm::iam::rbac::resolve_user_cache_uuid_for_org;
use crate::comm::notifications::NotificationService;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::IdKind;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPlaneRbacData;
use eden_core::response::EdenResponse;
use serde::Deserialize;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// **Permissions**: `ControlPerms::GRANT | granted_bits` on Organization
#[allow(clippy::too_many_arguments)]
#[with_telemetry]
pub async fn post(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    database: web::Data<EdenDb>,
    body: web::Json<SubjectInput>,
) -> Result<impl Responder, actix_web::Error> {
    let subject_input = body.into_inner();

    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());
    let org_uuid = org_cache.uuid();

    // only admins can view RBAC info
    verify_control_perms(&database, &auth, None, subject_input.required_grant_perms(), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let version_ms = chrono::Utc::now().timestamp_millis();

    for (subject, relation) in subject_input.to_vec() {
        let user_cache = resolve_user_cache_uuid_for_org(&database, &org_cache, auth.org_uuid(), &subject, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;

        let subject_uuid = user_cache.uuid();
        let data = ControlPlaneRbacData {
            org_uuid,
            entity_kind: IdKind::Organization.as_str().to_owned(),
            entity_uuid: org_uuid,
            subject_kind: IdKind::User.as_str().to_owned(),
            subject_uuid: user_cache.uuid(),
            perms: relation,
        };

        database.control_plane_grant(&data, version_ms, 0i64).await.map_err(|e| error_handling(e, &mut span))?;

        // Notify the user about their new access
        let _ = NotificationService::notify_system_update(
            &database,
            org_uuid,
            subject_uuid,
            "Access granted",
            &format!("You have been granted {} access to this organization.", relation),
            None,
            None,
            telemetry_wrapper,
        )
        .await;
    }

    EdenResponse::<String>::ok("success").into()
}

#[derive(Debug, Serialize, Deserialize, ToSchema)]
pub struct Response(String);
