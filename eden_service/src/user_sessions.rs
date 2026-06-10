//! User session and API usage tracking for analytics.
//!
//! This module provides in-memory stores for tracking:
//! - User login sessions (session history)
//! - API request usage per user (API usage history)
//!
//! Data is periodically flushed to ClickHouse for long-term storage and querying.

use analytics_schema::events::{ApiUsageHistoryRow, AuthMethod, SessionHistoryRow, SessionStatus};
use chrono::{DateTime, Utc};
use dashmap::DashMap;
use once_cell::sync::Lazy;
use std::sync::atomic::{AtomicU64, Ordering};
use uuid::Uuid;

/// Maximum number of API usage entries to buffer per user before flushing.
const MAX_API_USAGE_PER_USER: usize = 1000;

/// Maximum number of sessions to track in memory.
const MAX_ACTIVE_SESSIONS: usize = 10000;

/// Session idle timeout in seconds (1 hour). Sessions inactive longer are expired.
const SESSION_IDLE_TIMEOUT_SECS: i64 = 3600;

/// In-memory session entry before flushing to ClickHouse.
#[derive(Debug)]
pub struct SessionEntry {
    pub session_uuid: String,
    pub started_at: DateTime<Utc>,
    pub last_active_at: DateTime<Utc>,
    pub organization_uuid: String,
    pub user_uuid: String,
    pub user_id: String,
    pub device: String,
    pub user_agent: String,
    pub ip_address: String,
    pub auth_method: AuthMethod,
    pub status: SessionStatus,
    pub request_count: AtomicU64,
    pub error_count: AtomicU64,
    /// JWT ID (jti) associated with this session for per-session revocation.
    pub jti: Option<String>,
}

impl Clone for SessionEntry {
    fn clone(&self) -> Self {
        Self {
            session_uuid: self.session_uuid.clone(),
            started_at: self.started_at,
            last_active_at: self.last_active_at,
            organization_uuid: self.organization_uuid.clone(),
            user_uuid: self.user_uuid.clone(),
            user_id: self.user_id.clone(),
            device: self.device.clone(),
            user_agent: self.user_agent.clone(),
            ip_address: self.ip_address.clone(),
            auth_method: self.auth_method,
            status: self.status,
            request_count: AtomicU64::new(self.request_count.load(Ordering::Relaxed)),
            error_count: AtomicU64::new(self.error_count.load(Ordering::Relaxed)),
            jti: self.jti.clone(),
        }
    }
}

impl SessionEntry {
    /// Create a new session entry.
    pub fn new(
        organization_uuid: String,
        user_uuid: String,
        user_id: String,
        device: String,
        user_agent: String,
        ip_address: String,
        auth_method: AuthMethod,
        jti: Option<String>,
    ) -> Self {
        Self {
            session_uuid: Uuid::new_v4().to_string(),
            started_at: Utc::now(),
            last_active_at: Utc::now(),
            organization_uuid,
            user_uuid,
            user_id,
            device,
            user_agent,
            ip_address,
            auth_method,
            status: SessionStatus::Active,
            request_count: AtomicU64::new(1),
            error_count: AtomicU64::new(0),
            jti,
        }
    }

    /// Convert to a ClickHouse row.
    pub fn to_row(&self) -> SessionHistoryRow {
        SessionHistoryRow {
            session_uuid: self.session_uuid.clone(),
            started_at: self.started_at,
            ended_at: match self.status {
                SessionStatus::Active => None,
                _ => Some(self.last_active_at),
            },
            last_active_at: self.last_active_at,
            organization_uuid: self.organization_uuid.clone(),
            user_uuid: self.user_uuid.clone(),
            user_id: self.user_id.clone(),
            device: self.device.clone(),
            user_agent: self.user_agent.clone(),
            ip_address: self.ip_address.clone(),
            auth_method: self.auth_method.as_str().to_string(),
            status: self.status.as_str().to_string(),
            request_count: self.request_count.load(Ordering::Relaxed),
            error_count: self.error_count.load(Ordering::Relaxed),
        }
    }
}

/// In-memory API usage entry before flushing to ClickHouse.
#[derive(Debug, Clone)]
pub struct ApiUsageEntry {
    pub request_time: DateTime<Utc>,
    pub organization_uuid: String,
    pub user_uuid: String,
    pub user_id: String,
    pub session_uuid: Option<String>,
    pub request_id: String,
    pub http_method: String,
    pub http_path: String,
    pub http_status: u16,
    pub endpoint_uuid: Option<String>,
    pub endpoint_id: Option<String>,
    pub latency_us: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    pub client_ip: String,
    pub user_agent: String,
    pub error_code: Option<String>,
    pub error_message: Option<String>,
}

impl ApiUsageEntry {
    /// Convert to a ClickHouse row.
    pub fn to_row(&self) -> ApiUsageHistoryRow {
        ApiUsageHistoryRow {
            request_time: self.request_time,
            organization_uuid: self.organization_uuid.clone(),
            user_uuid: self.user_uuid.clone(),
            user_id: self.user_id.clone(),
            session_uuid: self.session_uuid.clone(),
            request_id: self.request_id.clone(),
            http_method: self.http_method.clone(),
            http_path: self.http_path.clone(),
            http_status: self.http_status,
            endpoint_uuid: self.endpoint_uuid.clone(),
            endpoint_id: self.endpoint_id.clone(),
            latency_us: self.latency_us,
            request_bytes: self.request_bytes,
            response_bytes: self.response_bytes,
            client_ip: self.client_ip.clone(),
            user_agent: self.user_agent.clone(),
            error_code: self.error_code.clone(),
            error_message: self.error_message.clone(),
        }
    }
}

/// Key for looking up sessions by IP/UA (legacy fallback when no JTI).
#[derive(Debug, Clone, Hash, PartialEq, Eq)]
pub struct IpUaKey {
    pub organization_uuid: String,
    pub user_uuid: String,
    pub ip_address: String,
    pub user_agent: String,
}

/// Store for tracking active user sessions.
///
/// Storage model:
/// - Primary storage: `sessions` keyed by session_uuid (allows multiple concurrent sessions)
/// - JTI index: `sessions_by_jti` maps JTI -> session_uuid (fast token lookup)
/// - IP/UA index: `sessions_by_ip_ua` maps IP/UA -> session_uuid (legacy fallback)
///
/// When a request comes in:
/// 1. If JTI is provided, look up by JTI first (authoritative for token-based sessions)
/// 2. If no JTI, fall back to IP/UA lookup (legacy behavior)
/// 3. Each unique JTI gets its own session (multiple tokens = multiple sessions)
pub struct SessionStore {
    /// Primary session storage keyed by session_uuid.
    sessions: DashMap<String, SessionEntry>,
    /// Index: JTI -> session_uuid (for fast token-based lookup).
    sessions_by_jti: DashMap<String, String>,
    /// Index: IP/UA key -> session_uuid (legacy fallback when no JTI).
    sessions_by_ip_ua: DashMap<IpUaKey, String>,
    /// Sessions that have ended and need to be flushed.
    ended_sessions: DashMap<String, SessionEntry>,
    /// New sessions pending initial flush to ClickHouse.
    new_sessions: DashMap<String, SessionEntry>,
    /// Sessions with updated activity that need periodic flush to ClickHouse.
    dirty_sessions: DashMap<String, ()>,
}

impl SessionStore {
    fn new() -> Self {
        Self {
            sessions: DashMap::new(),
            sessions_by_jti: DashMap::new(),
            sessions_by_ip_ua: DashMap::new(),
            ended_sessions: DashMap::new(),
            new_sessions: DashMap::new(),
            dirty_sessions: DashMap::new(),
        }
    }

    /// Record a new session or update an existing one.
    /// Returns the session UUID.
    pub fn record_session(
        &self,
        organization_uuid: &str,
        user_uuid: &str,
        user_id: &str,
        ip_address: &str,
        user_agent: &str,
        auth_method: AuthMethod,
    ) -> String {
        self.record_session_with_jti(organization_uuid, user_uuid, user_id, ip_address, user_agent, auth_method, None)
    }

    /// Record a new session or update an existing one, with JWT ID tracking.
    /// Returns the session UUID.
    ///
    /// Session uniqueness is determined by JTI when provided (each token gets its own session).
    /// When JTI is not provided, falls back to IP/UA based session matching for backwards compat.
    pub fn record_session_with_jti(
        &self,
        organization_uuid: &str,
        user_uuid: &str,
        user_id: &str,
        ip_address: &str,
        user_agent: &str,
        auth_method: AuthMethod,
        jti: Option<&str>,
    ) -> String {
        // If we have a JTI, use it as the authoritative lookup key
        if let Some(jti_str) = jti {
            // Look up by JTI first - this is the authoritative lookup for token-based sessions
            if let Some(session_uuid_ref) = self.sessions_by_jti.get(jti_str) {
                let session_uuid = session_uuid_ref.value().clone();
                if let Some(mut entry) = self.sessions.get_mut(&session_uuid) {
                    entry.last_active_at = Utc::now();
                    entry.request_count.fetch_add(1, Ordering::Relaxed);
                    // Mark as dirty for periodic flush
                    self.dirty_sessions.insert(session_uuid.clone(), ());
                    return session_uuid;
                }
            }

            // New JTI means new session (even if same IP/UA)
            // Check capacity before creating new session
            if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
                self.evict_oldest_sessions(MAX_ACTIVE_SESSIONS / 10);
            }

            // Create new session for this JTI
            let device = parse_device_from_user_agent(user_agent);
            let entry = SessionEntry::new(
                organization_uuid.to_string(),
                user_uuid.to_string(),
                user_id.to_string(),
                device,
                user_agent.to_string(),
                ip_address.to_string(),
                auth_method,
                Some(jti_str.to_string()),
            );
            let session_uuid = entry.session_uuid.clone();

            // Store session by session_uuid (primary key)
            self.sessions.insert(session_uuid.clone(), entry.clone());
            // Index by JTI for fast lookup
            self.sessions_by_jti.insert(jti_str.to_string(), session_uuid.clone());
            // Queue for initial flush to ClickHouse
            self.new_sessions.insert(session_uuid.clone(), entry);

            return session_uuid;
        }

        // Fallback: no JTI provided, use IP/UA based matching (legacy behavior)
        let ip_ua_key = IpUaKey {
            organization_uuid: organization_uuid.to_string(),
            user_uuid: user_uuid.to_string(),
            ip_address: ip_address.to_string(),
            user_agent: user_agent.to_string(),
        };

        // Check if we have an existing session for this IP/UA
        if let Some(session_uuid_ref) = self.sessions_by_ip_ua.get(&ip_ua_key) {
            let session_uuid = session_uuid_ref.value().clone();
            if let Some(mut entry) = self.sessions.get_mut(&session_uuid) {
                entry.last_active_at = Utc::now();
                entry.request_count.fetch_add(1, Ordering::Relaxed);
                // Mark as dirty for periodic flush
                self.dirty_sessions.insert(session_uuid.clone(), ());
                return session_uuid;
            }
        }

        // Check capacity before creating new session
        if self.sessions.len() >= MAX_ACTIVE_SESSIONS {
            self.evict_oldest_sessions(MAX_ACTIVE_SESSIONS / 10);
        }

        // Create new session
        let device = parse_device_from_user_agent(user_agent);
        let entry = SessionEntry::new(
            organization_uuid.to_string(),
            user_uuid.to_string(),
            user_id.to_string(),
            device,
            user_agent.to_string(),
            ip_address.to_string(),
            auth_method,
            None,
        );
        let session_uuid = entry.session_uuid.clone();

        // Store session by session_uuid (primary key)
        self.sessions.insert(session_uuid.clone(), entry.clone());
        // Index by IP/UA for legacy lookup
        self.sessions_by_ip_ua.insert(ip_ua_key, session_uuid.clone());
        // Queue for initial flush to ClickHouse
        self.new_sessions.insert(session_uuid.clone(), entry);

        session_uuid
    }

    /// Record an error for a session by JTI or IP/UA.
    pub fn record_error(&self, organization_uuid: &str, user_uuid: &str, ip_address: &str, user_agent: &str) {
        // Try to find a session for this IP/UA
        let ip_ua_key = IpUaKey {
            organization_uuid: organization_uuid.to_string(),
            user_uuid: user_uuid.to_string(),
            ip_address: ip_address.to_string(),
            user_agent: user_agent.to_string(),
        };

        if let Some(session_uuid_ref) = self.sessions_by_ip_ua.get(&ip_ua_key) {
            if let Some(entry) = self.sessions.get(session_uuid_ref.value()) {
                entry.error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Record an error for a session by JTI.
    pub fn record_error_by_jti(&self, jti: &str) {
        if let Some(session_uuid_ref) = self.sessions_by_jti.get(jti) {
            if let Some(entry) = self.sessions.get(session_uuid_ref.value()) {
                entry.error_count.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// End a session by JTI (mark as logged out).
    pub fn end_session_by_jti(&self, jti: &str) {
        if let Some((_, session_uuid)) = self.sessions_by_jti.remove(jti) {
            if let Some((_, mut entry)) = self.sessions.remove(&session_uuid) {
                // Clean up IP/UA index if this session was also indexed there
                let ip_ua_key = IpUaKey {
                    organization_uuid: entry.organization_uuid.clone(),
                    user_uuid: entry.user_uuid.clone(),
                    ip_address: entry.ip_address.clone(),
                    user_agent: entry.user_agent.clone(),
                };
                // Only remove if it points to this session
                if let Some(ref_uuid) = self.sessions_by_ip_ua.get(&ip_ua_key) {
                    if ref_uuid.value() == &session_uuid {
                        self.sessions_by_ip_ua.remove(&ip_ua_key);
                    }
                }
                entry.status = SessionStatus::LoggedOut;
                entry.last_active_at = Utc::now();
                self.ended_sessions.insert(entry.session_uuid.clone(), entry);
            }
        }
    }

    /// End a session by IP/UA (mark as logged out) - legacy method.
    pub fn end_session(&self, organization_uuid: &str, user_uuid: &str, ip_address: &str, user_agent: &str) {
        let ip_ua_key = IpUaKey {
            organization_uuid: organization_uuid.to_string(),
            user_uuid: user_uuid.to_string(),
            ip_address: ip_address.to_string(),
            user_agent: user_agent.to_string(),
        };

        if let Some((_, session_uuid)) = self.sessions_by_ip_ua.remove(&ip_ua_key) {
            if let Some((_, mut entry)) = self.sessions.remove(&session_uuid) {
                // Clean up JTI index
                if let Some(jti) = &entry.jti {
                    self.sessions_by_jti.remove(jti);
                }
                entry.status = SessionStatus::LoggedOut;
                entry.last_active_at = Utc::now();
                self.ended_sessions.insert(entry.session_uuid.clone(), entry);
            }
        }
    }

    /// Revoke all sessions for a user.
    pub fn revoke_user_sessions(&self, organization_uuid: &str, user_uuid: &str) {
        // Find all session UUIDs for this user
        let sessions_to_remove: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| entry.value().organization_uuid == organization_uuid && entry.value().user_uuid == user_uuid)
            .map(|entry| entry.key().clone())
            .collect();

        for session_uuid in sessions_to_remove {
            if let Some((_, mut entry)) = self.sessions.remove(&session_uuid) {
                // Clean up JTI index
                if let Some(jti) = &entry.jti {
                    self.sessions_by_jti.remove(jti);
                }
                // Clean up IP/UA index
                let ip_ua_key = IpUaKey {
                    organization_uuid: entry.organization_uuid.clone(),
                    user_uuid: entry.user_uuid.clone(),
                    ip_address: entry.ip_address.clone(),
                    user_agent: entry.user_agent.clone(),
                };
                // Only remove if it points to this session
                if let Some(ref_uuid) = self.sessions_by_ip_ua.get(&ip_ua_key) {
                    if ref_uuid.value() == &session_uuid {
                        self.sessions_by_ip_ua.remove(&ip_ua_key);
                    }
                }
                entry.status = SessionStatus::Revoked;
                entry.last_active_at = Utc::now();
                self.ended_sessions.insert(entry.session_uuid.clone(), entry);
            }
        }
    }

    /// Get active sessions for a user.
    pub fn get_user_sessions(&self, organization_uuid: &str, user_uuid: &str) -> Vec<SessionEntry> {
        self.sessions
            .iter()
            .filter(|entry| entry.value().organization_uuid == organization_uuid && entry.value().user_uuid == user_uuid)
            .map(|entry| entry.value().clone())
            .collect()
    }

    /// Get JTIs for other sessions (excluding the current one identified by jti or ip/user_agent).
    /// Returns a list of JTIs that should be blacklisted when revoking other sessions.
    pub fn get_other_session_jtis(
        &self,
        organization_uuid: &str,
        user_uuid: &str,
        current_ip: &str,
        current_ua: &str,
        current_jti: Option<&str>,
    ) -> Vec<String> {
        self.sessions
            .iter()
            .filter(|entry| entry.value().organization_uuid == organization_uuid && entry.value().user_uuid == user_uuid)
            .filter(|entry| {
                // Exclude current session: match by JTI if available, otherwise by IP/UA
                if let Some(cjti) = current_jti {
                    entry.value().jti.as_deref() != Some(cjti)
                } else {
                    !(entry.value().ip_address == current_ip && entry.value().user_agent == current_ua)
                }
            })
            .filter_map(|entry| entry.value().jti.clone())
            .collect()
    }

    /// Evict oldest sessions when at capacity.
    fn evict_oldest_sessions(&self, count: usize) {
        let mut sessions: Vec<_> = self.sessions.iter().map(|e| (e.key().clone(), e.value().last_active_at)).collect();
        sessions.sort_by_key(|(_, last_active)| *last_active);

        for (session_uuid, _) in sessions.into_iter().take(count) {
            if let Some((_, mut entry)) = self.sessions.remove(&session_uuid) {
                // Clean up JTI index
                if let Some(jti) = &entry.jti {
                    self.sessions_by_jti.remove(jti);
                }
                // Clean up IP/UA index
                let ip_ua_key = IpUaKey {
                    organization_uuid: entry.organization_uuid.clone(),
                    user_uuid: entry.user_uuid.clone(),
                    ip_address: entry.ip_address.clone(),
                    user_agent: entry.user_agent.clone(),
                };
                if let Some(ref_uuid) = self.sessions_by_ip_ua.get(&ip_ua_key) {
                    if ref_uuid.value() == &session_uuid {
                        self.sessions_by_ip_ua.remove(&ip_ua_key);
                    }
                }
                entry.status = SessionStatus::Expired;
                self.ended_sessions.insert(entry.session_uuid.clone(), entry);
            }
        }
    }

    /// Drain new, dirty, and ended sessions for flushing to ClickHouse.
    pub fn drain_rows(&self) -> Vec<SessionHistoryRow> {
        // First, expire stale sessions (idle > timeout)
        self.expire_stale_sessions();

        let mut rows = Vec::new();

        // Drain new sessions (written once on login)
        let new_keys: Vec<String> = self.new_sessions.iter().map(|e| e.key().clone()).collect();
        for key in new_keys {
            if let Some((_, entry)) = self.new_sessions.remove(&key) {
                rows.push(entry.to_row());
            }
        }

        // Drain dirty sessions (active sessions with updated activity)
        // This ensures ClickHouse has current request_count and last_active_at
        let dirty_keys: Vec<String> = self.dirty_sessions.iter().map(|e| e.key().clone()).collect();
        for session_uuid in dirty_keys {
            self.dirty_sessions.remove(&session_uuid);
            // Look up session directly by session_uuid (now the primary key)
            if let Some(entry) = self.sessions.get(&session_uuid) {
                rows.push(entry.value().to_row());
            }
        }

        // Drain ended sessions (written when session ends)
        let ended_keys: Vec<String> = self.ended_sessions.iter().map(|e| e.key().clone()).collect();
        for key in ended_keys {
            if let Some((_, entry)) = self.ended_sessions.remove(&key) {
                rows.push(entry.to_row());
            }
        }

        rows
    }

    /// Expire sessions that have been idle longer than the timeout.
    fn expire_stale_sessions(&self) {
        let now = Utc::now();
        let timeout = chrono::Duration::seconds(SESSION_IDLE_TIMEOUT_SECS);

        let stale_session_uuids: Vec<String> = self
            .sessions
            .iter()
            .filter(|entry| now - entry.value().last_active_at > timeout)
            .map(|entry| entry.key().clone())
            .collect();

        for session_uuid in stale_session_uuids {
            if let Some((_, mut entry)) = self.sessions.remove(&session_uuid) {
                // Clean up JTI index
                if let Some(jti) = &entry.jti {
                    self.sessions_by_jti.remove(jti);
                }
                // Clean up IP/UA index
                let ip_ua_key = IpUaKey {
                    organization_uuid: entry.organization_uuid.clone(),
                    user_uuid: entry.user_uuid.clone(),
                    ip_address: entry.ip_address.clone(),
                    user_agent: entry.user_agent.clone(),
                };
                if let Some(ref_uuid) = self.sessions_by_ip_ua.get(&ip_ua_key) {
                    if ref_uuid.value() == &session_uuid {
                        self.sessions_by_ip_ua.remove(&ip_ua_key);
                    }
                }
                entry.status = SessionStatus::Expired;
                entry.last_active_at = now;
                self.ended_sessions.insert(entry.session_uuid.clone(), entry);
            }
        }
    }
}

/// Store for tracking API usage per user.
pub struct ApiUsageStore {
    /// API usage entries indexed by user UUID.
    entries: DashMap<String, Vec<ApiUsageEntry>>,
}

impl ApiUsageStore {
    fn new() -> Self {
        Self { entries: DashMap::new() }
    }

    /// Record an API usage entry.
    pub fn record(&self, entry: ApiUsageEntry) {
        let user_uuid = entry.user_uuid.clone();
        let mut vec = self.entries.entry(user_uuid).or_default();

        // Cap entries per user
        if vec.len() < MAX_API_USAGE_PER_USER {
            vec.push(entry);
        }
    }

    /// Drain all entries for flushing to ClickHouse.
    pub fn drain_rows(&self) -> Vec<ApiUsageHistoryRow> {
        let mut rows = Vec::new();

        // Collect keys first to avoid holding locks during removal
        let keys: Vec<String> = self.entries.iter().map(|e| e.key().clone()).collect();

        for key in keys {
            // Remove the entry entirely instead of leaving empty Vec
            if let Some((_, entries)) = self.entries.remove(&key) {
                for usage in entries {
                    rows.push(usage.to_row());
                }
            }
        }

        rows
    }
}

/// Parse device type from User-Agent string.
fn parse_device_from_user_agent(user_agent: &str) -> String {
    let ua_lower = user_agent.to_lowercase();
    if ua_lower.contains("mobile") || ua_lower.contains("android") || ua_lower.contains("iphone") {
        "mobile".to_string()
    } else if ua_lower.contains("tablet") || ua_lower.contains("ipad") {
        "tablet".to_string()
    } else if ua_lower.contains("curl") || ua_lower.contains("httpie") || ua_lower.contains("postman") {
        "cli".to_string()
    } else if ua_lower.contains("mozilla") || ua_lower.contains("chrome") || ua_lower.contains("safari") || ua_lower.contains("firefox") {
        "desktop".to_string()
    } else {
        "unknown".to_string()
    }
}

/// Global session store.
pub static SESSION_STORE: Lazy<SessionStore> = Lazy::new(SessionStore::new);

/// Global API usage store.
pub static API_USAGE_STORE: Lazy<ApiUsageStore> = Lazy::new(ApiUsageStore::new);

/// Table names for session/usage storage.
pub mod session_tables {
    pub use analytics_schema::wire::tables::{API_USAGE_HISTORY, SESSION_HISTORY};
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_session_create_and_update() {
        let store = SessionStore::new();

        let uuid1 = store.record_session("tenant1", "user1", "user_id1", "127.0.0.1", "Mozilla/5.0", AuthMethod::Basic);

        // Same user/ip/ua should return same session (when no JTI)
        let uuid2 = store.record_session("tenant1", "user1", "user_id1", "127.0.0.1", "Mozilla/5.0", AuthMethod::Basic);

        assert_eq!(uuid1, uuid2);

        // Different IP should create new session
        let uuid3 = store.record_session("tenant1", "user1", "user_id1", "192.168.1.1", "Mozilla/5.0", AuthMethod::Basic);

        assert_ne!(uuid1, uuid3);
    }

    #[test]
    fn test_session_with_jti_uniqueness() {
        let store = SessionStore::new();

        // Two different JTIs from the same IP/UA should create TWO separate sessions
        let uuid1 = store.record_session_with_jti(
            "tenant1",
            "user1",
            "user_id1",
            "127.0.0.1",
            "Mozilla/5.0",
            AuthMethod::Bearer,
            Some("jti-token-1"),
        );
        let uuid2 = store.record_session_with_jti(
            "tenant1",
            "user1",
            "user_id1",
            "127.0.0.1",
            "Mozilla/5.0",
            AuthMethod::Bearer,
            Some("jti-token-2"),
        );

        assert_ne!(uuid1, uuid2, "Different JTIs should create different sessions");

        // Same JTI should return the same session
        let uuid1_again = store.record_session_with_jti(
            "tenant1",
            "user1",
            "user_id1",
            "127.0.0.1",
            "Mozilla/5.0",
            AuthMethod::Bearer,
            Some("jti-token-1"),
        );
        assert_eq!(uuid1, uuid1_again, "Same JTI should return the same session");

        // Both sessions should be visible
        let sessions = store.get_user_sessions("tenant1", "user1");
        assert_eq!(sessions.len(), 2, "Both JTI-based sessions should be visible");
    }

    #[test]
    fn test_get_other_session_jtis() {
        let store = SessionStore::new();

        // Create three sessions with different JTIs
        store.record_session_with_jti("tenant1", "user1", "user_id1", "127.0.0.1", "Mozilla/5.0", AuthMethod::Bearer, Some("jti-1"));
        store.record_session_with_jti("tenant1", "user1", "user_id1", "127.0.0.1", "Mozilla/5.0", AuthMethod::Bearer, Some("jti-2"));
        store.record_session_with_jti("tenant1", "user1", "user_id1", "127.0.0.1", "Mozilla/5.0", AuthMethod::Bearer, Some("jti-3"));

        // Get other JTIs (excluding jti-1)
        let other_jtis = store.get_other_session_jtis("tenant1", "user1", "127.0.0.1", "Mozilla/5.0", Some("jti-1"));
        assert_eq!(other_jtis.len(), 2, "Should return 2 other JTIs");
        assert!(other_jtis.contains(&"jti-2".to_string()));
        assert!(other_jtis.contains(&"jti-3".to_string()));
        assert!(!other_jtis.contains(&"jti-1".to_string()));
    }

    #[test]
    fn test_device_parsing() {
        assert_eq!(parse_device_from_user_agent("Mozilla/5.0 (Windows NT 10.0; Win64; x64)"), "desktop");
        assert_eq!(parse_device_from_user_agent("Mozilla/5.0 (iPhone; CPU iPhone OS 14_0)"), "mobile");
        assert_eq!(parse_device_from_user_agent("curl/7.68.0"), "cli");
        assert_eq!(parse_device_from_user_agent("some-random-client"), "unknown");
    }

    #[test]
    fn test_api_usage_store() {
        let store = ApiUsageStore::new();

        let entry = ApiUsageEntry {
            request_time: Utc::now(),
            organization_uuid: "tenant1".to_string(),
            user_uuid: "user1".to_string(),
            user_id: "user_id1".to_string(),
            session_uuid: Some("session1".to_string()),
            request_id: "req1".to_string(),
            http_method: "GET".to_string(),
            http_path: "/api/v1/endpoints".to_string(),
            http_status: 200,
            endpoint_uuid: None,
            endpoint_id: None,
            latency_us: 1000,
            request_bytes: 100,
            response_bytes: 500,
            client_ip: "127.0.0.1".to_string(),
            user_agent: "test".to_string(),
            error_code: None,
            error_message: None,
        };

        store.record(entry);

        let rows = store.drain_rows();
        assert_eq!(rows.len(), 1);
        assert_eq!(rows[0].http_path, "/api/v1/endpoints");
    }
}
