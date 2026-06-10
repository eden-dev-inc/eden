use crate::EdenDb;
use crate::comm::organization::get::get_organization;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{Responder, web};
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, RobotId, RobotUuid};
use eden_core::response::EdenResponse;
use serde::Serialize;
use telemetry_extensions_macro::with_telemetry;
use utoipa::ToSchema;

/// List all Robots (Machine Accounts) in the organization
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    get,
    tags = ["IAM"],
    path="/iam/agents",
    operation_id = "list_agents",
    responses((status = OK, body = Vec<Response>))
)]
#[allow(clippy::too_many_arguments)]
pub async fn list(auth: web::ReqData<ParsedJwt>, database: web::Data<EdenDb>) -> Result<impl Responder, actix_web::Error> {
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let org_uuid = auth.org_uuid();
    let org_key = OrganizationCacheUuid::new(None, org_uuid.clone());

    let org_schema = get_organization(&database, &CacheObjectType::new(Some(org_key), None), telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let robots: Vec<Response> =
        org_schema.robot_pairs().iter().map(|(id, uuid)| Response { uuid: uuid.clone(), username: id.clone() }).collect();

    EdenResponse::response(robots).into()
}

#[derive(Debug, PartialEq, Serialize, ToSchema)]
pub struct Response {
    pub uuid: RobotUuid,
    pub username: RobotId,
}
