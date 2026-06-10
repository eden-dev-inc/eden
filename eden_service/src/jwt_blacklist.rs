//! JWT blacklist for revoked sessions.
//!
//! When a user's sessions are revoked, their active JWTs are blacklisted
//! in durable storage until they expire naturally. Each process keeps a small
//! in-memory hot path, but cache misses check the database so revocations
//! survive restarts and are visible to other service replicas.

use dashmap::DashSet;
#[cfg(not(embedded_db))]
use database::lib::ShardCache;
use eden_logger_internal::LogAudience;
use function_name::named;
use once_cell::sync::Lazy;
use std::sync::Arc;

const EDEN_JWT_EXPIRY: u64 = 900;
const EDEN_JWT_EXPIRY_S: &str = "EDEN_JWT_EXPIRY_S";

/// Blacklist key prefix for user-level blacklisting.
#[cfg(not(embedded_db))]
const BLACKLIST_KEY_PREFIX: &str = "jwt:blacklist:";

/// Blacklist key prefix for session-level (jti) blacklisting.
#[cfg(not(embedded_db))]
const JTI_BLACKLIST_KEY_PREFIX: &str = "jwt:jti:";

/// In-memory blacklist for fast lookups.
///
/// Key format: "org_uuid:user_uuid" for user-level, "jti:<jti>" for session-level
static IN_MEMORY_BLACKLIST: Lazy<Arc<DashSet<String>>> = Lazy::new(|| Arc::new(DashSet::new()));

/// Get the JWT expiry duration in seconds from environment or default.
fn get_jwt_expiry_secs() -> u64 {
    std::env::var(EDEN_JWT_EXPIRY_S).ok().and_then(|v| v.parse().ok()).unwrap_or(EDEN_JWT_EXPIRY)
}

/// Blacklist all JWTs for a user.
///
/// This should be called when sessions are revoked. All tokens for the user
/// will be rejected until the blacklist entry expires (matching JWT expiry).
///
/// # Arguments
/// * `org_uuid` - Organization UUID
/// * `user_uuid` - User UUID
#[cfg(not(embedded_db))]
#[named]
pub async fn blacklist_user<R, P, C>(db: &database::lib::DatabaseManager<R, P, C>, org_uuid: &str, user_uuid: &str)
where
    R: database::lib::EdenRedisConnection + Sync,
    P: database::lib::EdenPostgresConnection + Sync,
    C: database::lib::EdenClickhouseConnection + Sync,
{
    let key = format!("{}{}:{}", BLACKLIST_KEY_PREFIX, org_uuid, user_uuid);
    let memory_key = format!("{}:{}", org_uuid, user_uuid);
    let expiry_secs = get_jwt_expiry_secs();

    // Add to in-memory blacklist for fast lookups
    IN_MEMORY_BLACKLIST.insert(memory_key.clone());

    if let Err(e) = db.persist_jwt_blacklist_entry(&key, expiry_secs).await {
        eden_logger_internal::log_error!(
            eden_logger_internal::ctx_with_trace!(),
            "Failed to persist JWT blacklist to durable storage",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
    }

    // Store in this process' internal cache with TTL matching JWT expiry.
    if let Err(e) = db.internal_cache().kv_set_ex(key, "1".to_string(), expiry_secs).await {
        eden_logger_internal::log_error!(
            eden_logger_internal::ctx_with_trace!(),
            "Failed to cache JWT blacklist entry",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
    } else {
        eden_logger_internal::log_info!(
            eden_logger_internal::ctx_with_trace!(),
            "Blacklisted JWTs for user",
            audience = LogAudience::Internal,
            org_uuid = org_uuid,
            user_uuid = user_uuid,
            expiry_secs = expiry_secs
        );
    }

    // Schedule removal from in-memory cache after expiry
    let memory_key_clone = memory_key;
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(expiry_secs)).await;
        IN_MEMORY_BLACKLIST.remove(&memory_key_clone);
    });
}

/// Stub for embedded_db mode - uses only in-memory blacklist.
#[cfg(embedded_db)]
#[named]
pub async fn blacklist_user<R, P, C>(_db: &database::lib::DatabaseManager<R, P, C>, org_uuid: &str, user_uuid: &str)
where
    R: Send + Sync + 'static,
    P: Sync,
    C: Send + Sync + 'static,
{
    let memory_key = format!("{}:{}", org_uuid, user_uuid);
    let expiry_secs = get_jwt_expiry_secs();

    IN_MEMORY_BLACKLIST.insert(memory_key.clone());

    eden_logger_internal::log_info!(
        eden_logger_internal::ctx_with_trace!(),
        "Blacklisted JWTs for user (in-memory)",
        audience = LogAudience::Internal,
        org_uuid = org_uuid,
        user_uuid = user_uuid,
        expiry_secs = expiry_secs
    );

    // Schedule removal after expiry
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(expiry_secs)).await;
        IN_MEMORY_BLACKLIST.remove(&memory_key);
    });
}

/// Check if a user's JWTs are blacklisted.
///
/// This performs a fast in-memory check first, then falls back to the internal cache
/// and finally durable storage. Durable read failures fail closed.
///
/// # Arguments
/// * `org_uuid` - Organization UUID
/// * `user_uuid` - User UUID
///
/// # Returns
/// `true` if the user's tokens are blacklisted and should be rejected.
#[cfg(not(embedded_db))]
#[named]
pub async fn is_blacklisted<R, P, C>(db: &database::lib::DatabaseManager<R, P, C>, org_uuid: &str, user_uuid: &str) -> bool
where
    R: database::lib::EdenRedisConnection + Sync,
    P: database::lib::EdenPostgresConnection + Sync,
    C: database::lib::EdenClickhouseConnection + Sync,
{
    let memory_key = format!("{}:{}", org_uuid, user_uuid);

    // Fast path: check in-memory cache first
    if IN_MEMORY_BLACKLIST.contains(&memory_key) {
        return true;
    }

    let key = format!("{}{}:{}", BLACKLIST_KEY_PREFIX, org_uuid, user_uuid);
    match db.internal_cache().kv_get(&key).await {
        Ok(Some(_)) => {
            IN_MEMORY_BLACKLIST.insert(memory_key);
            return true;
        }
        Ok(None) => {}
        Err(e) => {
            eden_logger_internal::log_error!(
                eden_logger_internal::ctx_with_trace!(),
                "Failed to check JWT blacklist in process cache",
                audience = LogAudience::Internal,
                error = e.to_string()
            );
        }
    }

    match db.jwt_blacklist_entry_exists(&key).await {
        Ok(true) => {
            IN_MEMORY_BLACKLIST.insert(memory_key);
            true
        }
        Ok(false) => false,
        Err(e) => {
            eden_logger_internal::log_error!(
                eden_logger_internal::ctx_with_trace!(),
                "Failed to check JWT blacklist in durable storage",
                audience = LogAudience::Internal,
                error = e.to_string()
            );
            true
        }
    }
}

/// Stub for embedded_db mode - uses only in-memory blacklist.
#[cfg(embedded_db)]
pub async fn is_blacklisted<R, P, C>(_db: &database::lib::DatabaseManager<R, P, C>, org_uuid: &str, user_uuid: &str) -> bool
where
    R: Send + Sync + 'static,
    P: Sync,
    C: Send + Sync + 'static,
{
    let memory_key = format!("{}:{}", org_uuid, user_uuid);
    IN_MEMORY_BLACKLIST.contains(&memory_key)
}

/// Blacklist a specific JWT by its jti (JWT ID).
///
/// This is used for per-session revocation (e.g., "revoke other sessions").
///
/// # Arguments
/// * `jti` - The JWT ID to blacklist
#[cfg(not(embedded_db))]
#[named]
pub async fn blacklist_jti<R, P, C>(db: &database::lib::DatabaseManager<R, P, C>, jti: &str)
where
    R: database::lib::EdenRedisConnection + Sync,
    P: database::lib::EdenPostgresConnection + Sync,
    C: database::lib::EdenClickhouseConnection + Sync,
{
    let key = format!("{}{}", JTI_BLACKLIST_KEY_PREFIX, jti);
    let memory_key = format!("jti:{}", jti);
    let expiry_secs = get_jwt_expiry_secs();

    // Add to in-memory blacklist for fast lookups
    IN_MEMORY_BLACKLIST.insert(memory_key.clone());

    if let Err(e) = db.persist_jwt_blacklist_entry(&key, expiry_secs).await {
        eden_logger_internal::log_error!(
            eden_logger_internal::ctx_with_trace!(),
            "Failed to persist jti blacklist to durable storage",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
    }

    // Persist to the internal cache with TTL matching JWT expiry.
    if let Err(e) = db.internal_cache().kv_set_ex(key, "1".to_string(), expiry_secs).await {
        eden_logger_internal::log_error!(
            eden_logger_internal::ctx_with_trace!(),
            "Failed to cache jti blacklist entry",
            audience = LogAudience::Internal,
            error = e.to_string()
        );
    } else {
        eden_logger_internal::log_info!(
            eden_logger_internal::ctx_with_trace!(),
            "Blacklisted JWT by jti",
            audience = LogAudience::Internal,
            jti = jti,
            expiry_secs = expiry_secs
        );
    }

    // Schedule removal from in-memory cache after expiry
    let memory_key_clone = memory_key;
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(expiry_secs)).await;
        IN_MEMORY_BLACKLIST.remove(&memory_key_clone);
    });
}

/// Stub for embedded_db mode - uses only in-memory blacklist.
#[cfg(embedded_db)]
#[named]
pub async fn blacklist_jti<R, P, C>(_db: &database::lib::DatabaseManager<R, P, C>, jti: &str)
where
    R: Send + Sync + 'static,
    P: Sync,
    C: Send + Sync + 'static,
{
    let memory_key = format!("jti:{}", jti);
    let expiry_secs = get_jwt_expiry_secs();

    IN_MEMORY_BLACKLIST.insert(memory_key.clone());

    eden_logger_internal::log_info!(
        eden_logger_internal::ctx_with_trace!(),
        "Blacklisted JWT by jti (in-memory)",
        audience = LogAudience::Internal,
        jti = jti,
        expiry_secs = expiry_secs
    );

    // Schedule removal after expiry
    tokio::spawn(async move {
        tokio::time::sleep(tokio::time::Duration::from_secs(expiry_secs)).await;
        IN_MEMORY_BLACKLIST.remove(&memory_key);
    });
}

/// Check if a specific JWT is blacklisted by its jti.
///
/// # Arguments
/// * `jti` - The JWT ID to check
///
/// # Returns
/// `true` if the token is blacklisted and should be rejected.
#[cfg(not(embedded_db))]
#[named]
pub async fn is_jti_blacklisted<R, P, C>(db: &database::lib::DatabaseManager<R, P, C>, jti: &str) -> bool
where
    R: database::lib::EdenRedisConnection + Sync,
    P: database::lib::EdenPostgresConnection + Sync,
    C: database::lib::EdenClickhouseConnection + Sync,
{
    let memory_key = format!("jti:{}", jti);

    // Fast path: check in-memory cache first
    if IN_MEMORY_BLACKLIST.contains(&memory_key) {
        return true;
    }

    let key = format!("{}{}", JTI_BLACKLIST_KEY_PREFIX, jti);
    match db.internal_cache().kv_get(&key).await {
        Ok(Some(_)) => {
            IN_MEMORY_BLACKLIST.insert(memory_key);
            return true;
        }
        Ok(None) => {}
        Err(e) => {
            eden_logger_internal::log_error!(
                eden_logger_internal::ctx_with_trace!(),
                "Failed to check jti blacklist in process cache",
                audience = LogAudience::Internal,
                error = e.to_string()
            );
        }
    }

    match db.jwt_blacklist_entry_exists(&key).await {
        Ok(true) => {
            IN_MEMORY_BLACKLIST.insert(memory_key);
            true
        }
        Ok(false) => false,
        Err(e) => {
            eden_logger_internal::log_error!(
                eden_logger_internal::ctx_with_trace!(),
                "Failed to check jti blacklist in durable storage",
                audience = LogAudience::Internal,
                error = e.to_string()
            );
            true
        }
    }
}

/// Stub for embedded_db mode - uses only in-memory blacklist.
#[cfg(embedded_db)]
pub async fn is_jti_blacklisted<R, P, C>(_db: &database::lib::DatabaseManager<R, P, C>, jti: &str) -> bool
where
    R: Send + Sync + 'static,
    P: Sync,
    C: Send + Sync + 'static,
{
    let memory_key = format!("jti:{}", jti);
    IN_MEMORY_BLACKLIST.contains(&memory_key)
}

/// Clear the in-memory blacklist (for testing).
#[cfg(test)]
pub fn clear_memory_blacklist() {
    IN_MEMORY_BLACKLIST.clear();
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_in_memory_blacklist() {
        let key = "test-org:test-user".to_string();

        // Initially not blacklisted
        assert!(!IN_MEMORY_BLACKLIST.contains(&key));

        // Add to blacklist
        IN_MEMORY_BLACKLIST.insert(key.clone());
        assert!(IN_MEMORY_BLACKLIST.contains(&key));

        // Remove from blacklist
        IN_MEMORY_BLACKLIST.remove(&key);
        assert!(!IN_MEMORY_BLACKLIST.contains(&key));
    }

    #[test]
    fn test_jwt_expiry_default() {
        // Without env var set, should return default
        assert_eq!(get_jwt_expiry_secs(), EDEN_JWT_EXPIRY);
    }
}
