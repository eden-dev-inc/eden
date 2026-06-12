use crate::EdenDb;
use database::db::els::ElsCommands;
use database::db::rbac::{ControlPlaneRbac, DataPlaneRbac};
use eden_core::auth::ParsedJwt;
use eden_core::error::{EpError, RbacError, ResultEP};
use eden_core::format::cache_uuid::{CacheUuid, EndpointCacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::{ControlPerms, DataPerms};
use eden_core::format::{EdenUuid, EndpointUuid, IdKind};
use eden_core::telemetry::TelemetryWrapper;
use function_name::named;
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuthMode {
    Rbac,
    Els,
}

fn subject_identity(auth: &ParsedJwt) -> ResultEP<(IdKind, Uuid)> {
    if auth.is_robot() {
        let robot_uuid = auth.robot_uuid().ok_or(EpError::Rbac(RbacError::Unauthorized))?;
        Ok((IdKind::Robot, robot_uuid.uuid()))
    } else {
        Ok((IdKind::User, auth.user_uuid().uuid()))
    }
}

// ---------------------------------------------------------------------------
// Control plane verification
// ---------------------------------------------------------------------------

#[named]
pub async fn verify_control_perms_for_entity(
    database: &EdenDb,
    auth: &ParsedJwt,
    entity_kind: IdKind,
    entity_uuid: Uuid,
    required: ControlPerms,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

    let org_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned()).uuid();
    let (subject_kind, subject_uuid) = subject_identity(auth)?;

    let org_ok = database.control_plane_verify(org_uuid, IdKind::Organization, org_uuid, subject_kind, subject_uuid, required).await?;
    if org_ok {
        return Ok(());
    }

    let entity_ok = database.control_plane_verify(org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid, required).await?;
    if entity_ok {
        return Ok(());
    }

    Err(EpError::Rbac(RbacError::Unauthorized))
}

#[named]
pub async fn verify_control_perms(
    database: &EdenDb,
    auth: &ParsedJwt,
    endpoint_entity: Option<EndpointUuid>,
    required: ControlPerms,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

    if let Some(ep) = endpoint_entity {
        return verify_control_perms_for_entity(database, auth, IdKind::Endpoint, ep.uuid(), required, telemetry_wrapper).await;
    }

    let org_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned()).uuid();
    let (subject_kind, subject_uuid) = subject_identity(auth)?;
    let org_ok = database.control_plane_verify(org_uuid, IdKind::Organization, org_uuid, subject_kind, subject_uuid, required).await?;
    if org_ok {
        Ok(())
    } else {
        Err(EpError::Rbac(RbacError::Unauthorized))
    }
}

// ---------------------------------------------------------------------------
// Data plane verification (endpoint read/write/execute operations)
// ---------------------------------------------------------------------------

/// Verify the caller has the required data-plane permissions (`r/w/x`) on an
/// endpoint and resolve the auth mode (shared RBAC vs personal ELS).
///
/// - **Robots** always use RBAC mode — checked via data-plane `r/w/x` bits.
/// - **Users with an ELS assignment** use personal mode — Eden skips `r/w/x`
///   gating; the target system enforces access via the injected credentials.
/// - **Users without ELS** use shared mode — checked via data-plane `r/w/x` bits.
#[named]
pub async fn verify_endpoint_access(
    database: &EdenDb,
    auth: &ParsedJwt,
    endpoint_cache_uuid: &EndpointCacheUuid,
    endpoint_entity: EndpointUuid,
    required: DataPerms,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<AuthMode> {
    let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

    if auth.is_robot() {
        verify_data_perms(database, auth, endpoint_entity, required, telemetry_wrapper).await?;
        return Ok(AuthMode::Rbac);
    }

    if database.els_has_assignment(endpoint_cache_uuid, auth.user_uuid()).await? {
        return Ok(AuthMode::Els);
    }

    verify_data_perms(database, auth, endpoint_entity, required, telemetry_wrapper).await?;
    Ok(AuthMode::Rbac)
}

/// Check that the caller holds the required data-plane bits (`r/w/x`) for an
/// endpoint. Uses the Redis-cached fast path.
#[named]
async fn verify_data_perms(
    database: &EdenDb,
    auth: &ParsedJwt,
    endpoint_entity: EndpointUuid,
    required: DataPerms,
    telemetry_wrapper: &mut TelemetryWrapper,
) -> ResultEP<()> {
    let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

    let org_uuid = OrganizationCacheUuid::new(None, auth.org_uuid().to_owned()).uuid();
    let (subject_kind, subject_uuid) = subject_identity(auth)?;

    let ok = database.data_plane_verify(org_uuid, endpoint_entity.uuid(), subject_kind, subject_uuid, required).await?;

    if ok { Ok(()) } else { Err(EpError::Rbac(RbacError::Unauthorized)) }
}
