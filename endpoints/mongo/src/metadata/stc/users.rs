use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Utc};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, ProfilingRequirement, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use mongo_core::MongoAsync;
use mongodb::bson::{Document, doc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

use super::utils::{DocAccessor, fetch};
/// MongoDB user authentication and authorization statistics
///
/// Comprehensive metrics about user authentication patterns, authorization
/// failures, role usage, and security-related activities.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUserInfo {
    /// Total number of users across all databases
    pub total_users: u32,
    /// Number of users with admin privileges
    pub admin_users: u32,
    /// Number of users with read-only access
    pub read_only_users: u32,
    /// Number of users with read-write access
    pub read_write_users: u32,
    /// Number of custom roles defined
    pub custom_roles: u32,
    /// Number of built-in roles in use
    pub builtin_roles: u32,
    /// Total authentication attempts in the last period
    pub total_auth_attempts: u64,
    /// Successful authentication attempts
    pub successful_auth_attempts: u64,
    /// Failed authentication attempts
    pub failed_auth_attempts: u64,
    /// Number of unique users who authenticated
    pub active_users: u32,
    /// Average authentication time in milliseconds
    pub avg_auth_time_ms: f64,
    /// Maximum authentication time in milliseconds
    pub max_auth_time_ms: f64,
    /// Number of authorization failures
    pub authorization_failures: u64,
    /// Number of privilege escalation attempts
    pub privilege_escalation_attempts: u64,
    /// Number of users with external authentication (LDAP, Kerberos, etc.)
    pub external_auth_users: u32,
    /// Number of users with SCRAM-SHA authentication
    pub scram_auth_users: u32,
    /// Number of X.509 certificate users
    pub x509_auth_users: u32,
    /// Number of users with expired credentials
    pub expired_credential_users: u32,
    /// Number of locked user accounts
    pub locked_accounts: u32,
    /// Average session duration in minutes
    pub avg_session_duration_minutes: f64,
    /// Number of concurrent sessions
    pub concurrent_sessions: u32,
    /// Number of users with excessive privileges
    pub over_privileged_users: u32,
    /// Detailed metrics collected only when security issues are detected
    pub detailed_metrics: Option<MongoUserDetailedMetrics>,
}

/// Detailed metrics collected only when security issues are detected
///
/// This reduces overhead by only collecting expensive security data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUserDetailedMetrics {
    /// Failed authentication details (collected when failure rate is high)
    pub failed_auth_details: Vec<MongoFailedAuthAttempt>,
    /// Authorization failure details (collected when authorization failures occur)
    pub authorization_failure_details: Vec<MongoAuthorizationFailure>,
    /// User privilege analysis (collected when over-privileged users detected)
    pub privilege_analysis: Vec<MongoUserPrivilegeAnalysis>,
    /// Suspicious activity patterns (collected when anomalies detected)
    pub suspicious_activities: Vec<MongoSuspiciousActivity>,
    /// User session details (collected periodically or when issues detected)
    pub user_session_details: Option<Vec<MongoUserSession>>,
    /// Role usage statistics (collected when role analysis is needed)
    pub role_usage_stats: Option<Vec<MongoRoleUsageStats>>,
    /// External authentication details (collected when external auth is used)
    pub external_auth_details: Option<Vec<MongoExternalAuthDetails>>,
}

impl MetadataCollection for MongoUserInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "users_info".to_string(),
                FindInput::new("admin".to_string(), "system.users".to_string(), None, Some(FindOptionsWrapper::new())),
            ),
            (
                "roles_info".to_string(),
                FindInput::new("admin".to_string(), "system.roles".to_string(), None, Some(FindOptionsWrapper::new())),
            ),
            (
                "auth_activities".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.authenticate": { "$exists": true } },
                            { "command.saslStart": { "$exists": true } },
                            { "command.saslContinue": { "$exists": true } },
                            { "errorCode": { "$in": [18, 13] } } // AuthenticationFailed, Unauthorized
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(1)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(1000)),
                ),
            ),
            (
                "failed_auth_attempts".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "errorCode": 18, // AuthenticationFailed
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(1)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(200)),
                ),
            ),
            (
                "authorization_failures".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "errorCode": 13, // Unauthorized
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::hours(1)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(200)),
                ),
            ),
            (
                "current_sessions".to_string(),
                FindInput::new(
                    "config".to_string(),
                    "system.sessions".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "lastUse": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new()),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive user authentication and authorization metrics"
    }

    fn category(&self) -> &'static str {
        "users"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::Medium
    }

    fn profiling_requirement(&self) -> ProfilingRequirement {
        ProfilingRequirement::Level1
    }
}

use function_name::named;
use std::time::Duration;

impl MongoUserInfo {
    const HIGH_FAILURE_RATE_THRESHOLD: f64 = 10.0; // 10% failure rate
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    const SUSPICIOUS_ACTIVITY_THRESHOLD: u32 = 5; // 5 failed attempts from same source
    const LONG_AUTH_TIME_THRESHOLD_MS: f64 = 1000.0; // 1 second

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut user_info = MongoUserInfo::default();
        let requests = self.request();

        // Execute queries to get user information
        let users_docs = fetch(&requests, "users_info", context.clone(), Self::QUERY_TIMEOUT).await?;

        let roles_docs = fetch(&requests, "roles_info", context.clone(), Self::QUERY_TIMEOUT).await?;

        let auth_activities_docs = fetch(&requests, "auth_activities", context.clone(), Self::QUERY_TIMEOUT).await?;

        let sessions_docs = fetch(&requests, "current_sessions", context.clone(), Self::QUERY_TIMEOUT).await?;

        // Parse the results
        Self::parse_users_data(&mut user_info, &users_docs)?;
        Self::parse_roles_data(&mut user_info, &roles_docs)?;
        Self::parse_auth_activities(&mut user_info, &auth_activities_docs)?;
        Self::parse_sessions_data(&mut user_info, &sessions_docs)?;

        // Conditionally collect detailed metrics only when security issues are detected
        user_info.detailed_metrics = self.collect_detailed_metrics_if_needed(&user_info, &requests, context).await?;

        Ok(user_info)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_info: &MongoUserInfo,
        requests: &HashMap<String, FindInput>,
        context: MongoAsync,
    ) -> ResultEP<Option<MongoUserDetailedMetrics>> {
        let needs_failed_auth_details = core_info.auth_failure_rate_percentage() > Self::HIGH_FAILURE_RATE_THRESHOLD;
        let needs_authorization_details = core_info.authorization_failures > 0;
        let needs_privilege_analysis = core_info.over_privileged_users > 0;
        let needs_session_details = core_info.concurrent_sessions > 100; // Arbitrary threshold
        let needs_suspicious_activity_analysis = core_info.failed_auth_attempts > Self::SUSPICIOUS_ACTIVITY_THRESHOLD as u64;

        if !needs_failed_auth_details
            && !needs_authorization_details
            && !needs_privilege_analysis
            && !needs_session_details
            && !needs_suspicious_activity_analysis
        {
            return Ok(None);
        }

        let mut detailed_metrics = MongoUserDetailedMetrics {
            failed_auth_details: Vec::new(),
            authorization_failure_details: Vec::new(),
            privilege_analysis: Vec::new(),
            suspicious_activities: Vec::new(),
            user_session_details: None,
            role_usage_stats: None,
            external_auth_details: None,
        };

        // Collect failed authentication details if needed
        if needs_failed_auth_details {
            let docs = fetch(requests, "failed_auth_attempts", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.failed_auth_details = Self::parse_failed_auth_attempts(docs)?;
        }

        // Collect authorization failure details if needed
        if needs_authorization_details {
            let docs = fetch(requests, "authorization_failures", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.authorization_failure_details = Self::parse_authorization_failures(docs)?;
        }

        // Collect session details if needed
        if needs_session_details {
            let docs = fetch(requests, "current_sessions", context.clone(), Self::QUERY_TIMEOUT).await?;
            detailed_metrics.user_session_details = Some(Self::parse_user_sessions(docs)?);
        }

        Ok(Some(detailed_metrics))
    }

    fn parse_users_data(info: &mut MongoUserInfo, docs: &[Document]) -> ResultEP<()> {
        info.total_users = docs.len() as u32;
        info.admin_users = 0;
        info.read_only_users = 0;
        info.read_write_users = 0;
        info.external_auth_users = 0;
        info.scram_auth_users = 0;
        info.x509_auth_users = 0;
        info.expired_credential_users = 0;
        info.locked_accounts = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            // Analyze user roles
            if let Some(roles_array) = acc.array("roles") {
                let mut has_admin = false;
                let mut has_write = false;
                let mut has_read = false;

                for role_doc in roles_array {
                    if let Some(role_name) = role_doc.opt_string("role") {
                        match role_name.as_str() {
                            "root" | "userAdminAnyDatabase" | "dbAdminAnyDatabase" | "clusterAdmin" => has_admin = true,
                            "readWriteAnyDatabase" | "readWrite" => has_write = true,
                            "readAnyDatabase" | "read" => has_read = true,
                            _ => {}
                        }
                    }
                }

                if has_admin {
                    info.admin_users += 1;
                } else if has_write {
                    info.read_write_users += 1;
                } else if has_read {
                    info.read_only_users += 1;
                }
            }

            // Analyze authentication mechanisms
            if let Some(credentials_doc) = acc.child("credentials") {
                let creds = credentials_doc.raw();
                if creds.contains_key("SCRAM-SHA-1") || creds.contains_key("SCRAM-SHA-256") {
                    info.scram_auth_users += 1;
                }
                if creds.contains_key("external") {
                    info.external_auth_users += 1;
                }
            }

            // Check for account status
            if let Some(account_locked) = acc.opt_bool("accountLocked")
                && account_locked
            {
                info.locked_accounts += 1;
            }

            // Check for credential expiration (if available)
            if let Some(credentials_doc) = acc.child("credentials")
                && let Ok(expiry_date) = credentials_doc.raw().get_datetime("expiryDate")
                && DateTime::<Utc>::from(*expiry_date) < Utc::now()
            {
                info.expired_credential_users += 1;
            }
        }

        Ok(())
    }

    fn parse_roles_data(info: &mut MongoUserInfo, docs: &[Document]) -> ResultEP<()> {
        info.custom_roles = 0;
        info.builtin_roles = 0;

        for doc in docs {
            if let Some(role_name) = DocAccessor::new(doc).opt_string("role") {
                // Check if it's a built-in role
                let builtin_roles = [
                    "read",
                    "readWrite",
                    "dbAdmin",
                    "dbOwner",
                    "userAdmin",
                    "clusterAdmin",
                    "clusterManager",
                    "clusterMonitor",
                    "hostManager",
                    "backup",
                    "restore",
                    "readAnyDatabase",
                    "readWriteAnyDatabase",
                    "userAdminAnyDatabase",
                    "dbAdminAnyDatabase",
                    "root",
                ];

                if builtin_roles.contains(&role_name.as_str()) {
                    info.builtin_roles += 1;
                } else {
                    info.custom_roles += 1;
                }
            }
        }

        Ok(())
    }

    fn parse_auth_activities(info: &mut MongoUserInfo, docs: &[Document]) -> ResultEP<()> {
        let mut auth_times = Vec::new();
        let mut unique_users = std::collections::HashSet::new();
        let mut successful_auths = 0u64;
        let mut failed_auths = 0u64;
        let mut authorization_failures = 0u64;

        for doc in docs {
            let acc = DocAccessor::new(doc);

            // Track authentication attempts
            if acc.child("command").map(|c| c.raw().contains_key("authenticate") || c.raw().contains_key("saslStart")).unwrap_or(false) {
                info.total_auth_attempts += 1;

                // Check if authentication was successful
                if let Some(ok_status) = acc.opt_i32("ok") {
                    if ok_status == 1 {
                        successful_auths += 1;

                        // Track authentication time
                        if let Some(millis) = acc.opt_f64("millis") {
                            auth_times.push(millis);
                        }

                        // Track unique users
                        if let Some(user) = acc.opt_string("user") {
                            unique_users.insert(user);
                        }
                    }
                } else {
                    failed_auths += 1;
                }
            }

            // Check for specific error codes
            if let Some(error_code) = acc.opt_i32("errorCode") {
                match error_code {
                    18 => failed_auths += 1,           // AuthenticationFailed
                    13 => authorization_failures += 1, // Unauthorized
                    _ => {}
                }
            }
        }

        info.successful_auth_attempts = successful_auths;
        info.failed_auth_attempts = failed_auths;
        info.authorization_failures = authorization_failures;
        info.active_users = unique_users.len() as u32;

        // Calculate authentication time statistics
        if !auth_times.is_empty() {
            info.avg_auth_time_ms = auth_times.iter().sum::<f64>() / auth_times.len() as f64;
            info.max_auth_time_ms = auth_times.iter().fold(0.0f64, |a, &b| a.max(b));
        }

        Ok(())
    }

    fn parse_sessions_data(info: &mut MongoUserInfo, docs: &[Document]) -> ResultEP<()> {
        info.concurrent_sessions = docs.len() as u32;

        let mut session_durations = Vec::new();

        for doc in docs {
            // Calculate session duration
            if let (Ok(last_use), Ok(creation_time)) =
                (doc.get_datetime("lastUse"), doc.get_datetime("_id").or_else(|_| doc.get_datetime("createdAt")))
            {
                let duration = DateTime::<Utc>::from(*last_use).signed_duration_since(DateTime::<Utc>::from(*creation_time));
                let duration_minutes: f64 = duration.num_minutes() as f64;
                if duration_minutes > 0.0 {
                    session_durations.push(duration_minutes);
                }
            }
        }

        if !session_durations.is_empty() {
            info.avg_session_duration_minutes = session_durations.iter().sum::<f64>() / session_durations.len() as f64;
        }

        Ok(())
    }

    fn parse_failed_auth_attempts(docs: Vec<Document>) -> ResultEP<Vec<MongoFailedAuthAttempt>> {
        let mut failed_attempts = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let Some(ts) = acc.opt_datetime("ts") {
                let username = acc.opt_string("user").unwrap_or_else(|| "unknown".to_string());
                let database = acc.opt_string("db").unwrap_or_else(|| "unknown".to_string());
                let source_ip = acc.opt_string("remote").unwrap_or_else(|| "unknown".into());
                let mechanism = acc.child("command").and_then(|cmd| cmd.opt_string("mechanism")).unwrap_or_else(|| "unknown".into());
                let error_message = acc.opt_string("errmsg").unwrap_or_else(|| "unknown".to_string());

                failed_attempts.push(MongoFailedAuthAttempt {
                    username,
                    database,
                    source_ip,
                    mechanism,
                    error_message,
                    timestamp: ts,
                    attempt_count: 1, // Would need aggregation for accurate count
                });
            }
        }

        Ok(failed_attempts)
    }

    fn parse_authorization_failures(docs: Vec<Document>) -> ResultEP<Vec<MongoAuthorizationFailure>> {
        let mut auth_failures = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(&doc);
            if let Some(ts) = acc.opt_datetime("ts") {
                let username = acc.opt_string("user").unwrap_or_else(|| "unknown".to_string());
                let database = acc.opt_string("db").unwrap_or_else(|| "unknown".to_string());
                let namespace = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());
                let operation = acc
                    .child("command")
                    .map(|cmd| {
                        let cmd_doc = cmd.raw();
                        if cmd_doc.contains_key("find") {
                            "find"
                        } else if cmd_doc.contains_key("insert") {
                            "insert"
                        } else if cmd_doc.contains_key("update") {
                            "update"
                        } else if cmd_doc.contains_key("delete") {
                            "delete"
                        } else if cmd_doc.contains_key("aggregate") {
                            "aggregate"
                        } else {
                            "other"
                        }
                    })
                    .unwrap_or("unknown")
                    .to_string();
                let required_privilege = acc.opt_string("requiredPrivilege").unwrap_or_else(|| "unknown".to_string());
                let error_message = acc.opt_string("errmsg").unwrap_or_else(|| "unknown".to_string());

                auth_failures.push(MongoAuthorizationFailure {
                    username,
                    database,
                    namespace,
                    operation,
                    required_privilege,
                    error_message,
                    timestamp: ts,
                });
            }
        }

        Ok(auth_failures)
    }

    fn parse_user_sessions(docs: Vec<Document>) -> ResultEP<Vec<MongoUserSession>> {
        let mut sessions = Vec::new();

        for doc in docs {
            if let Ok(session_id) = doc.get_object_id("_id") {
                let acc = DocAccessor::new(&doc);
                let user_id = acc
                    .child("lsid")
                    .and_then(|d| d.raw().get_object_id("id").ok())
                    .map(|id| id.to_string())
                    .unwrap_or_else(|| "unknown".to_string());

                let last_use = acc.opt_datetime("lastUse").unwrap_or_else(|| DateTimeWrapper::from(Utc::now()));
                let duration_minutes = 0.0; // Would need more calculation

                sessions.push(MongoUserSession {
                    session_id: session_id.to_string(),
                    user_id,
                    last_activity: last_use,
                    duration_minutes,
                    operations_count: 0, // Would need additional tracking
                    client_info: acc.opt_string("client"),
                });
            }
        }

        Ok(sessions)
    }
}

/// Information about failed authentication attempts
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFailedAuthAttempt {
    /// Username that failed authentication
    pub username: String,
    /// Database where authentication was attempted
    pub database: String,
    /// Source IP address
    pub source_ip: String,
    /// Authentication mechanism used
    pub mechanism: String,
    /// Error message
    pub error_message: String,
    /// Timestamp of the attempt
    pub timestamp: DateTimeWrapper,
    /// Number of consecutive attempts from this source
    pub attempt_count: u32,
}

/// Information about authorization failures
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoAuthorizationFailure {
    /// Username that attempted the operation
    pub username: String,
    /// Database where operation was attempted
    pub database: String,
    /// Namespace (database.collection)
    pub namespace: String,
    /// Operation that was attempted
    pub operation: String,
    /// Required privilege for the operation
    pub required_privilege: String,
    /// Error message
    pub error_message: String,
    /// Timestamp of the failure
    pub timestamp: DateTimeWrapper,
}

/// User privilege analysis information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUserPrivilegeAnalysis {
    /// Username
    pub username: String,
    /// Database where user has privileges
    pub database: String,
    /// List of roles assigned to the user
    pub assigned_roles: Vec<String>,
    /// List of privileges granted by these roles
    pub granted_privileges: Vec<String>,
    /// List of privileges actually used
    pub used_privileges: Vec<String>,
    /// List of unused privileges (potential over-privilege)
    pub unused_privileges: Vec<String>,
    /// Risk score (0-100)
    pub risk_score: u32,
    /// Recommendations for privilege reduction
    pub recommendations: Vec<String>,
}

/// Information about suspicious activities
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSuspiciousActivity {
    /// Type of suspicious activity
    pub activity_type: String,
    /// Username involved
    pub username: String,
    /// Source IP address
    pub source_ip: String,
    /// Description of the activity
    pub description: String,
    /// Severity level (1-10)
    pub severity_level: u32,
    /// Number of occurrences
    pub occurrence_count: u32,
    /// Time window of the activity
    pub time_window_minutes: u32,
    /// Timestamp of first occurrence
    pub first_occurrence: DateTimeWrapper,
    /// Timestamp of last occurrence
    pub last_occurrence: DateTimeWrapper,
}

/// Information about user sessions
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoUserSession {
    /// Session identifier
    pub session_id: String,
    /// User identifier
    pub user_id: String,
    /// Last activity timestamp
    pub last_activity: DateTimeWrapper,
    /// Session duration in minutes
    pub duration_minutes: f64,
    /// Number of operations in this session
    pub operations_count: u32,
    /// Client application information
    pub client_info: Option<String>,
}

/// Role usage statistics
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoRoleUsageStats {
    /// Role name
    pub role_name: String,
    /// Database where role is defined
    pub database: String,
    /// Number of users with this role
    pub user_count: u32,
    /// Whether this is a built-in or custom role
    pub is_builtin: bool,
    /// List of privileges granted by this role
    pub privileges: Vec<String>,
    /// Usage frequency (operations per day)
    pub usage_frequency: f64,
    /// Last time this role was used
    pub last_used: Option<DateTimeWrapper>,
}

/// External authentication details
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoExternalAuthDetails {
    /// Username
    pub username: String,
    /// External authentication mechanism (LDAP, Kerberos, X.509)
    pub auth_mechanism: String,
    /// External identity provider
    pub identity_provider: String,
    /// Certificate DN (for X.509)
    pub certificate_dn: Option<String>,
    /// Last successful authentication
    pub last_auth: DateTimeWrapper,
    /// Authentication frequency (per day)
    pub auth_frequency: f64,
    /// Whether the external account is still valid
    pub is_active: bool,
}

impl MongoUserInfo {
    /// Calculates the authentication failure rate percentage
    pub fn auth_failure_rate_percentage(&self) -> f64 {
        if self.total_auth_attempts == 0 {
            0.0
        } else {
            (self.failed_auth_attempts as f64 / self.total_auth_attempts as f64) * 100.0
        }
    }

    /// Calculates the authentication success rate percentage
    pub fn auth_success_rate_percentage(&self) -> f64 {
        if self.total_auth_attempts == 0 {
            0.0
        } else {
            (self.successful_auth_attempts as f64 / self.total_auth_attempts as f64) * 100.0
        }
    }

    /// Checks if authentication times are concerning
    pub fn has_slow_authentication(&self, threshold_ms: f64) -> bool {
        self.max_auth_time_ms > threshold_ms
    }

    /// Checks if there are authorization issues
    pub fn has_authorization_issues(&self) -> bool {
        self.authorization_failures > 0
    }

    /// Checks if there are security concerns
    pub fn has_security_concerns(&self) -> bool {
        self.auth_failure_rate_percentage() > Self::HIGH_FAILURE_RATE_THRESHOLD
            || self.authorization_failures > 0
            || self.locked_accounts > 0
            || self.expired_credential_users > 0
            || self.over_privileged_users > 0
    }

    /// Returns the percentage of users with admin privileges
    pub fn admin_user_percentage(&self) -> f64 {
        if self.total_users == 0 {
            0.0
        } else {
            (self.admin_users as f64 / self.total_users as f64) * 100.0
        }
    }

    /// Returns the percentage of users with external authentication
    pub fn external_auth_percentage(&self) -> f64 {
        if self.total_users == 0 {
            0.0
        } else {
            (self.external_auth_users as f64 / self.total_users as f64) * 100.0
        }
    }

    /// Checks if there are too many admin users
    pub fn has_excessive_admin_users(&self, threshold_percentage: f64) -> bool {
        self.admin_user_percentage() > threshold_percentage
    }

    /// Returns the user activity rate percentage
    pub fn user_activity_rate_percentage(&self) -> f64 {
        if self.total_users == 0 {
            0.0
        } else {
            (self.active_users as f64 / self.total_users as f64) * 100.0
        }
    }

    /// Checks if there are dormant user accounts
    pub fn has_dormant_accounts(&self, min_activity_threshold: f64) -> bool {
        self.user_activity_rate_percentage() < min_activity_threshold
    }

    /// Returns the custom role percentage
    pub fn custom_role_percentage(&self) -> f64 {
        let total_roles = self.custom_roles + self.builtin_roles;
        if total_roles == 0 {
            0.0
        } else {
            (self.custom_roles as f64 / total_roles as f64) * 100.0
        }
    }

    /// Checks if there are account security issues
    pub fn has_account_security_issues(&self) -> bool {
        self.locked_accounts > 0 || self.expired_credential_users > 0
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Calculates average sessions per active user
    pub fn avg_sessions_per_user(&self) -> f64 {
        if self.active_users == 0 {
            0.0
        } else {
            self.concurrent_sessions as f64 / self.active_users as f64
        }
    }

    /// Checks if the authentication system is healthy
    pub fn is_auth_system_healthy(&self) -> bool {
        let failure_rate = self.auth_failure_rate_percentage();

        failure_rate < Self::HIGH_FAILURE_RATE_THRESHOLD
            && self.authorization_failures == 0
            && self.avg_auth_time_ms < Self::LONG_AUTH_TIME_THRESHOLD_MS
            && self.locked_accounts == 0
            && self.expired_credential_users == 0
    }

    /// Returns authentication throughput (auths per hour, estimated)
    pub fn estimated_auth_throughput_per_hour(&self) -> f64 {
        // Assuming the metrics cover a 1-hour window
        self.total_auth_attempts as f64
    }

    /// Checks for privilege escalation risks
    pub fn has_privilege_escalation_risk(&self) -> bool {
        self.privilege_escalation_attempts > 0 || self.over_privileged_users > 0
    }

    /// Returns the distribution of authentication mechanisms
    pub fn auth_mechanism_distribution(&self) -> Vec<(String, u32, f64)> {
        let mut mechanisms = Vec::new();

        if self.scram_auth_users > 0 {
            let percentage = (self.scram_auth_users as f64 / self.total_users as f64) * 100.0;
            mechanisms.push(("SCRAM".to_string(), self.scram_auth_users, percentage));
        }

        if self.external_auth_users > 0 {
            let percentage = (self.external_auth_users as f64 / self.total_users as f64) * 100.0;
            mechanisms.push(("External".to_string(), self.external_auth_users, percentage));
        }

        if self.x509_auth_users > 0 {
            let percentage = (self.x509_auth_users as f64 / self.total_users as f64) * 100.0;
            mechanisms.push(("X.509".to_string(), self.x509_auth_users, percentage));
        }

        mechanisms
    }

    /// Generates security recommendations based on current state
    pub fn security_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.auth_failure_rate_percentage() > Self::HIGH_FAILURE_RATE_THRESHOLD {
            recommendations.push("High authentication failure rate detected. Consider implementing account lockout policies.".to_string());
        }

        if self.admin_user_percentage() > 20.0 {
            recommendations.push("High percentage of admin users. Review and reduce administrative privileges.".to_string());
        }

        if self.authorization_failures > 0 {
            recommendations.push("Authorization failures detected. Review user permissions and role assignments.".to_string());
        }

        if self.expired_credential_users > 0 {
            recommendations.push("Users with expired credentials found. Implement credential rotation policies.".to_string());
        }

        if self.external_auth_percentage() < 50.0 && self.total_users > 10 {
            recommendations.push(
                "Consider implementing centralized authentication (LDAP/Active Directory) for better security management.".to_string(),
            );
        }

        if self.over_privileged_users > 0 {
            recommendations.push("Over-privileged users detected. Implement principle of least privilege.".to_string());
        }

        if self.avg_auth_time_ms > Self::LONG_AUTH_TIME_THRESHOLD_MS {
            recommendations.push("Slow authentication times detected. Review authentication infrastructure.".to_string());
        }

        recommendations
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_user_info() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let user_info = MongoUserInfo::default();

        let result = user_info
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
    }

    #[tokio::test]
    async fn test_user_security_calculations() {
        let info = MongoUserInfo {
            total_users: 50,
            admin_users: 5,
            external_auth_users: 30,
            total_auth_attempts: 1000,
            successful_auth_attempts: 950,
            failed_auth_attempts: 50,
            active_users: 40,
            authorization_failures: 0,
            locked_accounts: 0,
            expired_credential_users: 0,
            over_privileged_users: 0,
            avg_auth_time_ms: 500.0,
            ..Default::default()
        };

        assert_eq!(info.admin_user_percentage(), 10.0);
        assert_eq!(info.external_auth_percentage(), 60.0);
        assert_eq!(info.auth_failure_rate_percentage(), 5.0);
        assert_eq!(info.auth_success_rate_percentage(), 95.0);
        assert_eq!(info.user_activity_rate_percentage(), 80.0);
        assert!(!info.has_excessive_admin_users(15.0));
        assert!(info.has_excessive_admin_users(5.0));
        assert!(!info.has_dormant_accounts(50.0));
        assert!(info.is_auth_system_healthy());
    }

    #[tokio::test]
    async fn test_security_issue_detection() {
        let info = MongoUserInfo {
            total_auth_attempts: 100,
            failed_auth_attempts: 15, // 15% failure rate
            authorization_failures: 5,
            locked_accounts: 2,
            expired_credential_users: 3,
            over_privileged_users: 1,
            ..Default::default()
        };

        assert!(info.has_security_concerns());
        assert!(info.has_authorization_issues());
        assert!(info.has_account_security_issues());
        assert!(info.has_privilege_escalation_risk());
        assert!(!info.is_auth_system_healthy());
    }

    #[tokio::test]
    async fn test_authentication_mechanism_distribution() {
        let info = MongoUserInfo {
            total_users: 100,
            scram_auth_users: 60,
            external_auth_users: 30,
            x509_auth_users: 10,
            ..Default::default()
        };

        let distribution = info.auth_mechanism_distribution();
        assert_eq!(distribution.len(), 3);

        // Check SCRAM distribution
        let scram_dist = distribution.iter().find(|(name, _, _)| name == "SCRAM").unwrap();
        assert_eq!(scram_dist.1, 60);
        assert_eq!(scram_dist.2, 60.0);
    }

    #[tokio::test]
    async fn test_security_recommendations() {
        let info = MongoUserInfo {
            total_users: 20,
            admin_users: 10, // 50% admin users
            total_auth_attempts: 100,
            failed_auth_attempts: 20, // 20% failure rate
            authorization_failures: 5,
            expired_credential_users: 2,
            external_auth_users: 5, // 25% external auth
            ..Default::default()
        };

        let recommendations = info.security_recommendations();
        assert!(!recommendations.is_empty());
        assert!(recommendations.iter().any(|r| r.contains("authentication failure rate")));
        assert!(recommendations.iter().any(|r| r.contains("admin users")));
        assert!(recommendations.iter().any(|r| r.contains("Authorization failures")));
        assert!(recommendations.iter().any(|r| r.contains("expired credentials")));
        assert!(recommendations.iter().any(|r| r.contains("centralized authentication")));
    }
}
