use crate::EdenDb;
use crate::comm::rbac::verify_control_perms;
use crate::error_handling;
use actix_web::{HttpRequest, HttpResponse, Responder, web};
use backon::{BackoffBuilder, ExponentialBuilder};
use database::db::cache::CacheFunctions;
use database::db::rbac::ControlPlaneRbac;
use eden_core::auth::ParsedJwt;
use eden_core::format::cache_id::UserCacheId;
use eden_core::format::cache_uuid::{CacheUuid, OrganizationCacheUuid, UserCacheUuid};
use eden_core::format::rbac::ControlPerms;
use eden_core::format::{CacheObjectType, IdKind, UserId, UserUuid};
use endpoint_core::ep_core::database::schema::user::UserSchema;
use std::time::Duration;
use telemetry_extensions_macro::with_telemetry;

fn requires_superadmin(perms: ControlPerms) -> bool {
    perms.intersects(ControlPerms::CONFIGURE | ControlPerms::PROMOTE | ControlPerms::GRANT | ControlPerms::AUDIT | ControlPerms::DESTROY)
}

/// Delete an IAM User
///
/// 1. Validate authentication & authorization
/// 2. Check resource exists (return 204 if already deleted)
/// 3. Delete RBAC permissions from database
/// 4. Invalidate cache (with retry logic)
/// 5. Return 204 No Content
///
/// **Permissions**: See exact permission-bit checks in the handler body.
#[with_telemetry]
#[utoipa::path(
    delete,
    tags = ["IAM"],
    path="/iam/humans/{human}",
    operation_id = "delete_human",
    responses((status = 204))
)]
#[allow(clippy::too_many_arguments)]
pub async fn delete(
    _req: HttpRequest,
    auth: web::ReqData<ParsedJwt>,
    user: web::Path<String>,
    database: web::Data<EdenDb>,
) -> Result<impl Responder, actix_web::Error> {
    let org_cache = OrganizationCacheUuid::new(None, auth.org_uuid().clone());

    // Step 1: Validate authentication & authorization
    let requester_cache = UserCacheUuid::new(Some(org_cache.clone()), auth.user_uuid().clone());

    // Resolve user to delete from cache
    let user_cache = match <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_cache_uuid(
        &database,
        &CacheObjectType::from((Some(org_cache.clone()), user.clone())),
        telemetry_wrapper,
    )
    .await
    {
        Ok(cache) => cache,
        Err(_) => {
            log::info!("User {} not found in cache - treating as already deleted", user);
            return Ok(HttpResponse::NoContent().finish());
        }
    };

    let is_self_delete = user_cache == requester_cache;

    // For deleting OTHER users, require Admin access
    if !is_self_delete {
        verify_control_perms(&database, &auth, None, ControlPerms::CONFIGURE, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;
    }

    // Step 2: Check resource exists
    let user_cache_object = CacheObjectType::new(Some(user_cache.clone()), None);
    let _user_to_delete: UserSchema = <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::get_from_cache(
        &database,
        &user_cache_object,
        telemetry_wrapper,
    )
    .await
    .map_err(|e| error_handling(e, &mut span))?;

    // Get the user's current organization permission bits for authorization.
    let user_entries = database
        .control_plane_list_by_subject(org_cache.uuid(), IdKind::User, user_cache.uuid())
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    let user_perms = user_entries
        .iter()
        .find(|entry| entry.entity_kind == IdKind::Organization.as_str() && entry.entity_uuid == org_cache.uuid())
        .map(|entry| entry.perms)
        .unwrap_or(ControlPerms::empty());

    if !is_self_delete && !user_perms.is_empty() {
        verify_control_perms(&database, &auth, None, ControlPerms::GRANT | user_perms, telemetry_wrapper)
            .await
            .map_err(|e| error_handling(e, &mut span))?;
        if requires_superadmin(user_perms) {
            verify_control_perms(&database, &auth, None, ControlPerms::DESTROY, telemetry_wrapper)
                .await
                .map_err(|e| error_handling(e, &mut span))?;
        }
    }

    // Step 3: Delete RBAC permissions from database
    let version_ms = chrono::Utc::now().timestamp_millis();
    database
        .control_plane_remove_subject(org_cache.uuid(), IdKind::User, user_cache.uuid(), version_ms, 0i64)
        .await
        .map_err(|e| error_handling(e, &mut span))?;

    // Step 4: Spawn background task for cache invalidation with retry logic
    // This allows the endpoint to return immediately (204) while cache is invalidated asynchronously
    let user_cache_clone = user_cache.clone();
    let _org_cache_clone = org_cache.clone();
    let database_clone = database.clone();
    let user_clone = user.to_string();
    let mut tw = telemetry_wrapper.clone();
    tokio::spawn(async move {
        spawn_cache_invalidation_task(&database_clone, &user_cache_clone, &user_clone, &mut tw).await;
    });

    // Database deletion is committed, cache invalidation happens in background
    Ok(HttpResponse::NoContent().finish())
}
const CACHE_INVALIDATION_MAX_RETRIES: u32 = 3;
const CACHE_INVALIDATION_INITIAL_BACKOFF_MS: u64 = 100;

fn cache_invalidation_backoff_sequence() -> impl Iterator<Item = Duration> {
    ExponentialBuilder::default()
        .with_min_delay(Duration::from_millis(CACHE_INVALIDATION_INITIAL_BACKOFF_MS))
        .with_factor(2.0)
        .without_max_delay()
        .with_max_times(CACHE_INVALIDATION_MAX_RETRIES as usize)
        .build()
        .skip(1)
        .take(CACHE_INVALIDATION_MAX_RETRIES.saturating_sub(1) as usize)
}

/// Spawns a background task to invalidate cache with exponential backoff retry
/// Uses exponential backoff at: 100ms, 200ms, 400ms
async fn spawn_cache_invalidation_task(
    database: &web::Data<EdenDb>,
    user_cache: &UserCacheUuid,
    user: &str,
    telemetry_wrapper: &mut eden_core::telemetry::TelemetryWrapper,
) {
    log::debug!("Cache invalidation task for user: {}", user);
    let mut backoff_sequence = cache_invalidation_backoff_sequence();

    for attempt in 1..=CACHE_INVALIDATION_MAX_RETRIES {
        if attempt > 1
            && let Some(delay) = backoff_sequence.next()
        {
            log::debug!(
                "Cache invalidation background retry attempt {}/{} for user {} - waiting {}ms...",
                attempt,
                CACHE_INVALIDATION_MAX_RETRIES,
                user,
                delay.as_millis() as u64
            );
            tokio::time::sleep(delay).await;
        }

        let user_cache_object = CacheObjectType::new(Some(user_cache.clone()), None);

        match <EdenDb as CacheFunctions<UserSchema, UserCacheUuid, UserUuid, UserCacheId, UserId>>::invalidate(
            database,
            &user_cache_object,
            telemetry_wrapper,
        )
        .await
        {
            Ok(_) => {
                log::info!(
                    "Successfully invalidated cache for user {} (attempt {}/{})",
                    user,
                    attempt,
                    CACHE_INVALIDATION_MAX_RETRIES
                );
                return;
            }
            Err(e) => {
                log::warn!(
                    "Cache invalidation attempt {}/{} failed for user {} - cache key: {:?}, error: {:?}",
                    attempt,
                    CACHE_INVALIDATION_MAX_RETRIES,
                    user,
                    user_cache,
                    e
                );

                if attempt == CACHE_INVALIDATION_MAX_RETRIES {
                    log::error!(
                        "Cache invalidation exhausted all {} retries for user {} - eventual consistency will handle this",
                        CACHE_INVALIDATION_MAX_RETRIES,
                        user
                    );
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::future::Future;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    fn assert_duration_close(actual: Duration, expected: Duration) {
        assert!(actual.abs_diff(expected) <= Duration::from_micros(1), "expected {expected:?}, got {actual:?}");
    }

    async fn run_cache_invalidation_with_retry<F, Fut, E, I>(
        user: &str,
        max_retries: u32,
        mut backoff_sequence: I,
        mut op: F,
    ) -> Result<(), E>
    where
        F: FnMut(u32) -> Fut,
        Fut: Future<Output = Result<(), E>>,
        I: Iterator<Item = Duration>,
    {
        if max_retries == 0 {
            return Ok(());
        }

        for attempt in 1..=max_retries {
            if attempt > 1
                && let Some(delay) = backoff_sequence.next()
            {
                log::debug!(
                    "Cache invalidation background retry attempt {}/{} for user {} - waiting {}ms...",
                    attempt,
                    max_retries,
                    user,
                    delay.as_millis() as u64
                );
                tokio::time::sleep(delay).await;
            }

            match op(attempt).await {
                Ok(()) => return Ok(()),
                Err(err) if attempt == max_retries => return Err(err),
                Err(_) => {}
            }
        }

        Ok(())
    }

    #[test]
    fn cache_invalidation_backoff_sequence_matches_existing_progression() {
        let delays: Vec<_> = cache_invalidation_backoff_sequence().collect();

        assert_eq!(delays.len(), CACHE_INVALIDATION_MAX_RETRIES.saturating_sub(1) as usize);
        if let Some(first) = delays.first() {
            assert_duration_close(*first, Duration::from_millis(CACHE_INVALIDATION_INITIAL_BACKOFF_MS * 2));
        }
        for (idx, delay) in delays.iter().enumerate().skip(1) {
            let expected = Duration::from_millis(CACHE_INVALIDATION_INITIAL_BACKOFF_MS * 2).saturating_mul(2_u32.pow(idx as u32));
            assert_duration_close(*delay, expected);
        }
    }

    #[tokio::test]
    async fn run_cache_invalidation_with_retry_first_attempt_is_immediate() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = run_cache_invalidation_with_retry(
            "user-a",
            CACHE_INVALIDATION_MAX_RETRIES,
            std::iter::repeat_n(Duration::ZERO, CACHE_INVALIDATION_MAX_RETRIES as usize),
            {
                let attempts = attempts.clone();
                move |_| {
                    let attempts = attempts.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Ok::<(), &'static str>(())
                    }
                }
            },
        )
        .await;

        assert_eq!(result, Ok(()));
        assert_eq!(attempts.load(Ordering::SeqCst), 1);
    }

    #[tokio::test]
    async fn run_cache_invalidation_with_retry_retries_until_max_attempts() {
        let attempts = Arc::new(AtomicUsize::new(0));
        let result = run_cache_invalidation_with_retry(
            "user-b",
            CACHE_INVALIDATION_MAX_RETRIES,
            std::iter::repeat_n(Duration::ZERO, CACHE_INVALIDATION_MAX_RETRIES as usize),
            {
                let attempts = attempts.clone();
                move |_| {
                    let attempts = attempts.clone();
                    async move {
                        attempts.fetch_add(1, Ordering::SeqCst);
                        Err::<(), &'static str>("fail")
                    }
                }
            },
        )
        .await;

        assert_eq!(result, Err("fail"));
        assert_eq!(attempts.load(Ordering::SeqCst), CACHE_INVALIDATION_MAX_RETRIES as usize);
    }
}
