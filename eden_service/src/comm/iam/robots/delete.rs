use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use database::db::cache::CacheFunctions;
use database::db::lib::{ClickhouseConn, DatabaseManager, PgConn, RedisConn};
use database::db::methods::delete::DeleteMethod;
use database::db::methods::delete::robot::DeleteRobot;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::error::{DatabaseError, EpError};
use eden_core::format::cache_id::{CacheId, RobotCacheId};
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, RobotCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, IdKind, RobotId, RobotUuid};
use eden_logger_internal::LogContextEdenExt;
use eden_logger_internal::{LogAudience, ctx_with_trace, log_error, log_info, log_warn};
use endpoint_core::ep_core::database::schema::robot::RobotSchema;
use function_name::named;
use telemetry_extensions_macro::with_telemetry;

/// Delete a Robot (Machine Account)
///
/// 1. Validate authentication & authorization (Admin required)
/// 2. Check resource exists (return 204 if already deleted)
/// 3. Delete RBAC permissions
/// 4. Delete persisted robot row from PostgreSQL
/// 5. Attempt synchronous cache invalidation; if it fails, enqueue async retry with backoff
/// 6. Return 204 No Content
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["IAM"],
    path="/iam/agents/{agent}",
    operation_id = "delete_agent",
    responses((status = 204))
)]
#[named]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    robot: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());
    let log_ctx = ctx_with_trace!()
        .with_feature("iam")
        .with_organization_uuid(auth.org_uuid().to_string())
        .with_additional("robot", robot.to_string());

    // Only admins can delete robots
    verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Resolve robot from cache
    let robot_cache = match <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), robot.clone())),
        telemetry_wrapper,
    )
    .await
    {
        Ok(cache) => cache,
        Err(EpError::Database(DatabaseError::RobotNotFound)) => {
            log_info!(
                log_ctx.clone(),
                "Robot not found during delete; treating as already deleted",
                audience = LogAudience::Internal
            );
            return Ok(HttpResponse::NoContent().finish());
        }
        Err(e) => return Err(error_handling(e, &mut span)),
    };

    // Delete RBAC permissions
    let version_ms = chrono::Utc::now().timestamp_millis();
    database
        .control_plane_remove_subject(org_cache.uuid(), IdKind::Robot, robot_cache.uuid(), version_ms, 0i64)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Delete persisted robot data from PostgreSQL before returning success.
    let robot_cache_object = CacheObjectType::new(
        Some(robot_cache.clone()),
        Some(RobotCacheId::new(Some(org_cache.clone()), RobotId::from(robot.to_string()))),
    );
    let delete_robot = <DeleteRobot as DeleteMethod<
        RobotSchema,
        RobotCacheUuid,
        RobotUuid,
        RobotCacheId,
        RobotId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::new(robot_cache_object.clone());

    <DeleteRobot as DeleteMethod<
        RobotSchema,
        RobotCacheUuid,
        RobotUuid,
        RobotCacheId,
        RobotId,
        RedisConn,
        PgConn,
        ClickhouseConn,
    >>::delete_database(&delete_robot, &database, telemetry_wrapper)
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // Try cache invalidation synchronously before returning 204. If this fails, retry in the background.
    if let Err(e) = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::invalidate(
        &database,
        &robot_cache_object,
        telemetry_wrapper,
    )
    .await
    {
        log_warn!(
            log_ctx.clone(),
            "Synchronous robot cache invalidation failed; scheduling background retries",
            audience = LogAudience::Internal,
            error = format!("{:?}", e)
        );

        let robot_cache_object_clone = robot_cache_object.clone();
        let database_clone = database.clone();
        let robot_clone = robot.to_string();
        let mut tw = telemetry_wrapper.clone();
        tokio::spawn(async move {
            spawn_cache_invalidation_task(&database_clone, &robot_cache_object_clone, &robot_clone, &mut tw).await;
        });
    } else {
        log_info!(log_ctx, "Successfully invalidated robot cache synchronously", audience = LogAudience::Internal);
    }

    Ok(HttpResponse::NoContent().finish())
}

/// Retries robot cache invalidation with exponential backoff.
///
/// Spawned via `tokio::spawn` from [`delete`] **after** the 204 response has
/// been returned, so the caller never blocks on retries. This means there is a
/// window where the DB row is gone but the cache entry still exists (eventual
/// consistency).
///
/// Backoff: up to 3 attempts at 100 ms, 200 ms, 400 ms. On exhaustion the
/// stale cache entry will be evicted by its normal TTL.
#[named]
async fn spawn_cache_invalidation_task(
    database: &web::Data<EdenDb>,
    robot_cache_object: &CacheObjectType<RobotCacheUuid, RobotCacheId>,
    robot: &str,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) {
    const MAX_RETRIES: u32 = 3;
    const INITIAL_BACKOFF_MS: u64 = 100;

    for attempt in 1..=MAX_RETRIES {
        let backoff_ms = INITIAL_BACKOFF_MS * 2_u64.pow(attempt - 1);

        if attempt > 1 {
            tokio::time::sleep(tokio::time::Duration::from_millis(backoff_ms)).await;
        }

        let result = <EdenDb as CacheFunctions<RobotSchema, RobotCacheUuid, RobotUuid, RobotCacheId, RobotId>>::invalidate(
            database,
            robot_cache_object,
            telemetry_wrapper,
        )
        .await;

        if let Err(e) = result {
            log_warn!(
                ctx_with_trace!().with_feature("iam").with_additional("robot", robot.to_string()),
                "Cache invalidation retry failed for robot delete",
                audience = LogAudience::Internal,
                attempt = attempt,
                max_retries = MAX_RETRIES,
                error = format!("{:?}", e)
            );

            if attempt == MAX_RETRIES {
                log_error!(
                    ctx_with_trace!().with_feature("iam").with_additional("robot", robot.to_string()),
                    "Cache invalidation exhausted retries for robot delete",
                    audience = LogAudience::Internal,
                    max_retries = MAX_RETRIES
                );
            }

            continue;
        }

        log_info!(
            ctx_with_trace!().with_feature("iam").with_additional("robot", robot.to_string()),
            "Successfully invalidated cache for robot delete",
            audience = LogAudience::Internal,
            attempt = attempt,
            max_retries = MAX_RETRIES
        );
        return;
    }
}
