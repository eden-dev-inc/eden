//! Return the authenticated caller's resolved control-plane and data-plane access
//! for an endpoint.

use crate::EdenDb;
use crate::comm::iam::data::{AccessResponse, ControlPlaneAccess, DataPlaneAccess, DataPlaneMode};
use crate::error_handling;
use actix_web::{HttpRequest, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::els::{ElsCommands, UserPolicyAssignmentRedacted};
use database::db::rbac::{ControlPlaneRbac, DataPlaneRbac};
use eden_core::auth::ParsedJwt;
use eden_core::error::EpError;
use eden_core::format::cache_id::EndpointCacheId;
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::{CacheObjectType, EdenUuid, EndpointId, EndpointUuid, IdKind};
use eden_core::response::EdenResponse;
use endpoint_schema::endpoint::EndpointSchema;
use telemetry_extensions_macro::with_telemetry;

fn has_visible_access(
    organization_perms: eden_core::format::rbac::ControlPerms,
    endpoint_perms: eden_core::format::rbac::ControlPerms,
    shared_perms: eden_core::format::rbac::DataPerms,
    has_els_assignment: bool,
) -> bool {
    !organization_perms.is_empty() || !endpoint_perms.is_empty() || !shared_perms.is_empty() || has_els_assignment
}

/// Get the caller's resolved control-plane and data-plane access for an endpoint.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/access/endpoints/{endpoint}",
    operation_id = "get_my_endpoint_access",
    responses((status = OK, body = AccessResponse))
)]
pub async fn get(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    endpoint: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let endpoint = endpoint.into_inner();
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned());
    let org_uuid = org_cache.uuid();

    let endpoint_schema =
        <EdenDb as CacheFunctions<EndpointSchema, EndpointCacheUuid, EndpointUuid, EndpointCacheId, EndpointId>>::get_from_cache(
            &database,
            &CacheObjectType::from((Some(org_cache.clone()), endpoint)),
            telemetry_wrapper,
        )
        .await
        .map_err(|e| error_handling(e, &mut span))?;
    let endpoint_cache = EndpointCacheUuid::new(Some(org_cache.clone()), endpoint_schema.endpoint_uuid());

    let (subject_kind, subject_uuid) = if auth.is_robot() {
        let robot_uuid = auth
            .robot_uuid()
            .ok_or_else(|| error_handling(eden_core::error::EpError::rbac("robot auth missing robot UUID"), &mut span))?;
        (IdKind::Robot, robot_uuid.uuid())
    } else {
        (IdKind::User, auth.user_uuid().uuid())
    };

    let organization_perms = database
        .control_plane_get(org_uuid, IdKind::Organization, org_uuid, subject_kind, subject_uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let endpoint_perms = database
        .control_plane_get(org_uuid, IdKind::Endpoint, endpoint_schema.endpoint_uuid().uuid(), subject_kind, subject_uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let shared_perms = database
        .data_plane_get(org_uuid, endpoint_schema.endpoint_uuid().uuid(), subject_kind, subject_uuid)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let els_assignment = if auth.is_robot() {
        None
    } else {
        database
            .els_get_user_policy(&endpoint_cache, auth.user_uuid())
            .await
            .map_err(|e| error_handling(e, &mut span))?
            .map(UserPolicyAssignmentRedacted::from)
    };

    let mode = if els_assignment.is_some() {
        DataPlaneMode::Els
    } else if shared_perms.is_empty() {
        DataPlaneMode::None
    } else {
        DataPlaneMode::SharedRbac
    };

    if !has_visible_access(organization_perms, endpoint_perms, shared_perms, els_assignment.is_some()) {
        return Err(error_handling(
            EpError::rbac("caller has no visible control-plane or data-plane access to this endpoint"),
            &mut span,
        ));
    }

    EdenResponse::response(AccessResponse {
        control_plane: ControlPlaneAccess { organization_perms, endpoint_perms },
        data_plane: DataPlaneAccess { mode, shared_perms, els_assignment },
    })
    .into()
}

#[cfg(test)]
mod tests {
    use super::has_visible_access;
    use eden_core::format::rbac::{ControlPerms, DataPerms};

    #[test]
    fn visible_access_allows_control_plane_visibility() {
        assert!(has_visible_access(ControlPerms::READ, ControlPerms::empty(), DataPerms::empty(), false));
    }

    #[test]
    fn visible_access_allows_data_plane_visibility() {
        assert!(has_visible_access(ControlPerms::empty(), ControlPerms::empty(), DataPerms::READ, false));
    }

    #[test]
    fn visible_access_allows_els_visibility() {
        assert!(has_visible_access(ControlPerms::empty(), ControlPerms::empty(), DataPerms::empty(), true));
    }

    #[test]
    fn visible_access_rejects_subjects_without_any_access() {
        assert!(!has_visible_access(ControlPerms::empty(), ControlPerms::empty(), DataPerms::empty(), false,));
    }
}
