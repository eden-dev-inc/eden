use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::{DateTime, Timelike, Utc};
use eden_logger_internal::{log_debug, trace_context};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::ResultEP;
use format::timestamp::DateTimeWrapper;
use function_name::named;
use mongo_core::MongoAsync;
use mongodb::bson::{Document, doc};
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::time::Duration;
use telemetry::TelemetryWrapper;

use super::utils::{DocAccessor, execute_admin_command_as_profiled, fetch};

/// MongoDB connection statistics and performance metrics
///
/// Simplified struct containing essential metrics about connection
/// performance, pool usage, and client connectivity patterns.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoConnectionInfo {
    /// Total number of active connections
    pub total_active_connections: u64,
    /// Number of available connections in the pool
    pub available_connections: u64,
    /// Maximum number of connections configured
    pub max_connections: u64,
    /// Number of connections currently being created
    pub connections_being_created: u64,
    /// Total number of connections ever created
    pub total_connections_created: u64,
    /// Number of connection timeouts in the last period
    pub connection_timeouts: u64,
    /// Number of failed connection attempts
    pub failed_connections: u64,
    /// Average connection establishment time (milliseconds)
    pub avg_connection_time_ms: f64,
    /// Maximum connection establishment time (milliseconds)
    pub max_connection_time_ms: f64,
    /// Minimum connection establishment time (milliseconds)
    pub min_connection_time_ms: f64,
    /// Connection pool utilization percentage
    pub pool_utilization_percentage: f64,
    /// Number of connections idle for extended periods
    pub idle_connections: u64,
    /// Average connection lifetime (seconds)
    pub avg_connection_lifetime_seconds: f64,
    /// Number of connections from different client applications
    pub unique_client_applications: u64,
    /// Number of connections from different IP addresses
    pub unique_client_ips: u64,
    /// Total bytes sent over all connections
    pub total_bytes_sent: u64,
    /// Total bytes received over all connections
    pub total_bytes_received: u64,
    /// Network throughput (bytes per second)
    pub network_throughput_bps: f64,
    /// Number of SSL/TLS connections
    pub ssl_connections: u64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoConnectionDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoConnectionDetailedMetrics {
    /// Slow connection establishments (only collected when threshold exceeded)
    pub slow_connections: Vec<MongoSlowConnection>,
    /// Failed connection attempts with details
    pub failed_connections: Vec<MongoFailedConnection>,
    /// High-usage client applications
    pub high_usage_clients: Vec<MongoHighUsageClient>,
    /// Connection pool exhaustion events
    pub pool_exhaustion_events: Vec<MongoPoolExhaustionEvent>,
    /// Long-running idle connections
    pub long_idle_connections: Option<Vec<MongoLongIdleConnection>>,
    /// Network performance issues
    pub network_issues: Option<Vec<MongoNetworkIssue>>,
    /// Geographic connection distribution
    pub geographic_distribution: Option<Vec<MongoGeographicConnection>>,
}

impl MetadataCollection for MongoConnectionInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "current_connections".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.connPoolStats": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(50)),
                ),
            ),
            (
                "connection_events".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.isMaster": { "$exists": true } },
                            { "command.hello": { "$exists": true } },
                            { "command.serverStatus": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(10)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
            (
                "network_stats".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "command.serverStatus": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(5)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(20)),
                ),
            ),
            (
                "client_connections".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "remote": { "$exists": true },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(15)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(200)),
                ),
            ),
            (
                "connection_errors".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "ok": 0 },
                            { "errmsg": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return essential connection metrics with minimal overhead"
    }

    fn category(&self) -> &'static str {
        "connections"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

impl MongoConnectionInfo {
    const SLOW_CONNECTION_THRESHOLD_MS: f64 = 1000.0; // 1 second
    const HIGH_POOL_UTILIZATION_THRESHOLD: f64 = 80.0; // 80%
    const QUERY_TIMEOUT: Duration = Duration::from_secs(10);
    const LONG_IDLE_THRESHOLD_MINUTES: u64 = 30;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut connection_stats = MongoConnectionInfo::default();
        let requests = self.request();

        // Execute serverStatus directly - contains all connection data
        let server_status_docs =
            execute_admin_command_as_profiled(doc! { "serverStatus": 1 }, context.clone(), Self::QUERY_TIMEOUT, "serverStatus").await?;

        // Parse all connection data from serverStatus
        Self::parse_connection_pool_stats(&mut connection_stats, &server_status_docs)?;
        Self::parse_network_stats(&mut connection_stats, &server_status_docs)?;

        // Supplement with profiler-backed insights when available
        let connection_events = Self::fetch(&requests, "connection_events", context.clone()).await?;
        Self::parse_connection_events(&mut connection_stats, &connection_events)?;

        let client_connections = Self::fetch(&requests, "client_connections", context.clone()).await?;
        Self::parse_client_connections(&mut connection_stats, &client_connections)?;

        let connection_errors = Self::fetch(&requests, "connection_errors", context.clone()).await?;
        Self::parse_connection_errors(&mut connection_stats, &connection_errors)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut connection_stats)?;

        // Conditionally collect detailed metrics only when problems are detected
        connection_stats.detailed_metrics = self
            .collect_detailed_metrics_if_needed(&connection_stats, &connection_events, &connection_errors, &client_connections)
            .await?;

        Ok(connection_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoConnectionInfo,
        connection_events: &[Document],
        connection_errors: &[Document],
        client_connections: &[Document],
    ) -> ResultEP<Option<MongoConnectionDetailedMetrics>> {
        let needs_slow_details = core_stats.max_connection_time_ms > Self::SLOW_CONNECTION_THRESHOLD_MS;
        let needs_failure_details = core_stats.failed_connections > 0 || core_stats.connection_timeouts > 0;
        let needs_high_usage_details = core_stats.is_pool_under_pressure();

        if !needs_slow_details && !needs_failure_details && !needs_high_usage_details {
            return Ok(None);
        }

        let mut detailed_metrics = MongoConnectionDetailedMetrics {
            slow_connections: Vec::new(),
            failed_connections: Vec::new(),
            high_usage_clients: Vec::new(),
            pool_exhaustion_events: Vec::new(),
            long_idle_connections: None,
            network_issues: None,
            geographic_distribution: None,
        };

        if needs_slow_details {
            detailed_metrics.slow_connections = Self::parse_slow_connections(connection_events)?;
        }

        if needs_failure_details {
            detailed_metrics.failed_connections = Self::parse_failed_connections(connection_errors)?;

            if detailed_metrics.failed_connections.iter().any(|failure| failure.error_message.to_lowercase().contains("timeout")) {
                detailed_metrics.network_issues = Some(vec![MongoNetworkIssue {
                    issue_type: "Connection Timeout".to_string(),
                    affected_client: "multiple".to_string(),
                    timestamp: DateTimeWrapper::from(Utc::now()),
                    severity: "High".to_string(),
                    impact_description: "Repeated connection timeouts detected in profiler data".to_string(),
                    recommended_resolution: "Investigate serverStatus.connection pools and network stability".to_string(),
                }]);
            }
        }

        if needs_high_usage_details {
            detailed_metrics.high_usage_clients = Self::parse_high_usage_clients(client_connections)?;

            let exhaustion_events = Self::identify_pool_exhaustion_events(core_stats);
            if !exhaustion_events.is_empty() {
                detailed_metrics.pool_exhaustion_events = exhaustion_events;
            }

            detailed_metrics.long_idle_connections = Self::identify_long_idle_connections(client_connections)?;
            detailed_metrics.geographic_distribution = Self::summarize_geographic_distribution(client_connections)?;
        }

        let has_details = !detailed_metrics.slow_connections.is_empty()
            || !detailed_metrics.failed_connections.is_empty()
            || !detailed_metrics.high_usage_clients.is_empty()
            || !detailed_metrics.pool_exhaustion_events.is_empty()
            || detailed_metrics.long_idle_connections.is_some()
            || detailed_metrics.network_issues.is_some()
            || detailed_metrics.geographic_distribution.is_some();

        if has_details { Ok(Some(detailed_metrics)) } else { Ok(None) }
    }

    async fn fetch(requests: &HashMap<String, FindInput>, key: &'static str, context: MongoAsync) -> ResultEP<Vec<Document>> {
        let _ctx = trace_context();
        let docs = fetch(requests, key, context, Self::QUERY_TIMEOUT).await?;
        if docs.is_empty() {
            log_debug!(
                _ctx,
                "metadata query returned no documents",
                audience = eden_logger_internal::LogAudience::Internal,
                collector = "mongo.connections",
                key = key
            );
        }
        Ok(docs)
    }

    fn parse_connection_pool_stats(stats: &mut MongoConnectionInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result")
                && let Some(connections) = result.child("connections")
            {
                let current = connections.opt_u64("current").unwrap_or(0);
                let active = connections.opt_u64("active").unwrap_or(current);

                stats.total_active_connections = active;
                stats.available_connections = connections.opt_u64("available").unwrap_or(0);
                stats.total_connections_created = connections.opt_u64("totalCreated").unwrap_or(0);

                let connecting = connections.opt_u64("connecting").or_else(|| connections.opt_u64("awaitingTopologyChanges")).unwrap_or(0);
                stats.connections_being_created = connecting;

                if current > 0 || stats.available_connections > 0 {
                    stats.max_connections = current + stats.available_connections;
                }

                if let Some(tls_connections) = connections.child("tls")
                    && let Some(tls_current) = tls_connections.opt_u64("current")
                    && tls_current > 0
                {
                    stats.ssl_connections = tls_current;
                }
            }
        }

        Ok(())
    }

    fn parse_connection_events(stats: &mut MongoConnectionInfo, docs: &[Document]) -> ResultEP<()> {
        let mut connection_times = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(millis) = acc.opt_f64("millis") {
                connection_times.push(millis);
            }

            if let Some(remote) = acc.opt_string("remote")
                && (remote.contains("ssl") || remote.contains("tls"))
            {
                stats.ssl_connections += 1;
            }
        }

        if !connection_times.is_empty() {
            stats.avg_connection_time_ms = connection_times.iter().sum::<f64>() / connection_times.len() as f64;
            stats.max_connection_time_ms = connection_times.iter().fold(0.0f64, |a, &b| a.max(b));
            stats.min_connection_time_ms = connection_times.iter().fold(f64::INFINITY, |a, &b| a.min(b));
        }

        Ok(())
    }

    fn parse_network_stats(stats: &mut MongoConnectionInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(result) = acc.child("result")
                && let Some(network) = result.child("network")
            {
                let bytes_in = network.opt_u64("bytesIn").unwrap_or(0) as f64;
                let bytes_out = network.opt_u64("bytesOut").unwrap_or(0) as f64;

                stats.total_bytes_received = bytes_in as u64;
                stats.total_bytes_sent = bytes_out as u64;

                // Calculate throughput based on time window (assuming 5-minute collection window)
                let collection_window_seconds = 300.0; // 5 minutes
                stats.network_throughput_bps = (bytes_in + bytes_out) / collection_window_seconds;
            }
        }

        Ok(())
    }

    fn parse_client_connections(stats: &mut MongoConnectionInfo, docs: &[Document]) -> ResultEP<()> {
        let mut unique_ips = std::collections::HashSet::new();
        let mut unique_apps = std::collections::HashSet::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);

            if let Some(remote) = acc.opt_string("remote")
                && let Some(ip) = remote.split(':').next()
            {
                unique_ips.insert(ip.to_string());
            }

            if let Some(client_doc) = acc.child("client")
                && let Some(app_doc) = client_doc.child("application")
                && let Some(application_name) = app_doc.opt_string("name")
            {
                unique_apps.insert(application_name);
            }
        }

        stats.unique_client_ips = unique_ips.len() as u64;
        stats.unique_client_applications = unique_apps.len() as u64;

        Ok(())
    }

    fn parse_connection_errors(stats: &mut MongoConnectionInfo, docs: &[Document]) -> ResultEP<()> {
        let mut failed_count = 0;
        let mut timeout_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if acc.opt_i32("ok").unwrap_or(1) == 0 {
                failed_count += 1;

                if acc.opt_string("errmsg").map(|msg| msg.to_lowercase().contains("timeout")).unwrap_or(false) {
                    timeout_count += 1;
                }
            }
        }

        stats.failed_connections = failed_count;
        stats.connection_timeouts = timeout_count;

        Ok(())
    }

    fn calculate_derived_metrics(stats: &mut MongoConnectionInfo) -> ResultEP<()> {
        // Calculate pool utilization
        let total_capacity = stats.total_active_connections + stats.available_connections;
        if total_capacity > 0 {
            stats.pool_utilization_percentage = (stats.total_active_connections as f64 / total_capacity as f64) * 100.0;
        }

        // Set max connections (this would typically come from configuration)
        stats.max_connections = total_capacity;

        // Idle connections are those in the available pool not actively serving requests
        stats.idle_connections = stats.available_connections;

        Ok(())
    }

    fn parse_slow_connections(docs: &[Document]) -> ResultEP<Vec<MongoSlowConnection>> {
        let mut connections = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(millis) = acc.opt_f64("millis")
                && millis > MongoConnectionInfo::SLOW_CONNECTION_THRESHOLD_MS
                && let Some(ts) = acc.opt_datetime("ts")
            {
                connections.push(MongoSlowConnection {
                    client_ip: acc.opt_string("remote").and_then(|r| r.split(':').next().map(|s| s.to_string())),
                    connection_time_ms: millis,
                    timestamp: ts,
                    user_agent: acc.child("client").and_then(|c| c.opt_string("driver")),
                    command_type: acc.opt_string("command"),
                    database: acc.opt_string("ns").map(|ns| ns.split('.').next().unwrap_or("unknown").to_string()),
                });
            }
        }

        Ok(connections)
    }

    fn parse_failed_connections(docs: &[Document]) -> ResultEP<Vec<MongoFailedConnection>> {
        let mut connections = Vec::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if acc.opt_i32("ok").unwrap_or(1) == 0
                && let Some(ts) = acc.opt_datetime("ts")
            {
                let remote = acc.opt_string("remote").unwrap_or_default();
                let client_ip = remote.split(':').next().map(|s| s.to_string());

                connections.push(MongoFailedConnection {
                    client_ip,
                    error_message: acc.opt_string("errmsg").unwrap_or_else(|| "Unknown error".to_string()),
                    error_code: acc.opt_i32("code"),
                    timestamp: ts,
                    retry_count: 0, // Would track actual retry attempts
                    user_agent: acc.child("client").and_then(|c| c.opt_string("driver")),
                    connection_type: if remote.contains("ssl") {
                        "SSL".to_string()
                    } else {
                        "Plain".to_string()
                    },
                });
            }
        }

        Ok(connections)
    }

    fn parse_high_usage_clients(docs: &[Document]) -> ResultEP<Vec<MongoHighUsageClient>> {
        let mut client_usage: HashMap<String, (u64, String)> = HashMap::new();

        // Count operations per client
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(remote) = acc.opt_string("remote")
                && let Some(ip) = remote.split(':').next()
            {
                let user_agent = acc.child("client").and_then(|c| c.opt_string("driver")).unwrap_or_else(|| "Unknown".to_string());

                let entry = client_usage.entry(ip.to_string()).or_insert((0, user_agent));
                entry.0 += 1;
            }
        }

        // Convert to high usage clients (threshold: more than 10 operations)
        let mut high_usage_clients = Vec::new();
        for (ip, (count, user_agent)) in client_usage {
            if count > 10 {
                high_usage_clients.push(MongoHighUsageClient {
                    client_ip: ip,
                    connection_count: count,
                    operations_per_minute: count as f64,
                    user_agent: Some(user_agent),
                    last_activity: DateTimeWrapper::from(Utc::now()),
                    connection_duration_minutes: 0.0,
                    data_transferred_mb: 0.0,
                });
            }
        }

        Ok(high_usage_clients)
    }

    fn identify_pool_exhaustion_events(stats: &MongoConnectionInfo) -> Vec<MongoPoolExhaustionEvent> {
        if !stats.is_pool_under_pressure() {
            return Vec::new();
        }

        let mut contributing_factors = Vec::new();
        if stats.available_connections == 0 {
            contributing_factors.push("No available connections".to_string());
        }
        if stats.connections_being_created > 0 {
            contributing_factors.push("Pool creating new connections".to_string());
        }
        if stats.connection_timeouts > 0 {
            contributing_factors.push("Connection timeouts observed".to_string());
        }

        vec![MongoPoolExhaustionEvent {
            timestamp: DateTimeWrapper::from(Utc::now()),
            duration_seconds: 0.0,
            queued_requests: stats.connections_being_created,
            peak_utilization_percentage: stats.pool_utilization_percentage,
            contributing_factors,
            recovery_time_seconds: 0.0,
        }]
    }

    fn identify_long_idle_connections(docs: &[Document]) -> ResultEP<Option<Vec<MongoLongIdleConnection>>> {
        if docs.is_empty() {
            return Ok(None);
        }

        let now = Utc::now();
        let mut idle_by_client: HashMap<String, MongoLongIdleConnection> = HashMap::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);

            let remote = match acc.opt_string("remote") {
                Some(remote) => remote,
                None => continue,
            };

            let ts = match doc.get_datetime("ts") {
                Ok(ts) => DateTime::<Utc>::from(*ts),
                Err(_) => continue,
            };

            let idle_minutes = (now - ts).num_minutes() as f64;

            if idle_minutes < Self::LONG_IDLE_THRESHOLD_MINUTES as f64 {
                continue;
            }

            let user_agent = acc.child("client").and_then(|c| c.opt_string("driver"));

            let last_database = acc.opt_string("ns").and_then(|ns| ns.split('.').next().map(|db| db.to_string()));

            let ts_wrapper = DateTimeWrapper::from(ts);

            let entry = idle_by_client.entry(remote.clone()).or_insert(MongoLongIdleConnection {
                client_ip: remote.clone(),
                idle_duration_minutes: idle_minutes,
                last_activity: ts_wrapper.clone(),
                user_agent: user_agent.clone(),
                last_database: last_database.clone(),
                recommended_action: "Investigate long-lived idle client connection".to_string(),
            });

            if idle_minutes > entry.idle_duration_minutes {
                entry.idle_duration_minutes = idle_minutes;
                entry.last_activity = ts_wrapper.clone();
                entry.user_agent = user_agent.clone();
                entry.last_database = last_database.clone();
            }
        }

        if idle_by_client.is_empty() {
            Ok(None)
        } else {
            Ok(Some(idle_by_client.into_values().collect()))
        }
    }

    fn summarize_geographic_distribution(docs: &[Document]) -> ResultEP<Option<Vec<MongoGeographicConnection>>> {
        if docs.is_empty() {
            return Ok(None);
        }

        #[derive(Default)]
        struct RegionAggregation {
            connection_count: u64,
            total_latency_ms: f64,
            success_count: u64,
            data_bytes: u64,
            hour_counts: HashMap<u8, u64>,
        }

        let mut region_map: HashMap<String, RegionAggregation> = HashMap::new();

        for doc in docs {
            let acc = DocAccessor::new(doc);

            let remote = match acc.opt_string("remote") {
                Some(remote) => remote,
                None => continue,
            };

            let region = Self::classify_remote_region(&remote);
            let latency = acc.opt_f64("millis").unwrap_or(0.0);
            let ok = acc.opt_i32("ok").unwrap_or(1);
            let bytes_from_client = acc.opt_i64("bytesFromClient").or_else(|| acc.opt_i64("bytesFromCursor")).unwrap_or(0).max(0) as u64;

            let ts_hour = doc.get_datetime("ts").ok().map(|ts| DateTime::<Utc>::from(*ts).hour() as u8).unwrap_or(0);

            let entry = region_map.entry(region).or_default();
            entry.connection_count += 1;
            entry.total_latency_ms += latency;
            if ok == 1 {
                entry.success_count += 1;
            }
            entry.data_bytes += bytes_from_client;
            *entry.hour_counts.entry(ts_hour).or_insert(0) += 1;
        }

        if region_map.is_empty() {
            return Ok(None);
        }

        let mut summary = Vec::with_capacity(region_map.len());
        for (region, data) in region_map {
            let avg_latency = if data.connection_count > 0 {
                data.total_latency_ms / data.connection_count as f64
            } else {
                0.0
            };

            let success_rate = if data.connection_count > 0 {
                (data.success_count as f64 / data.connection_count as f64) * 100.0
            } else {
                100.0
            };

            let peak_hour = data.hour_counts.into_iter().max_by_key(|(_, count)| *count).map(|(hour, _)| hour).unwrap_or(0);

            summary.push(MongoGeographicConnection {
                region,
                connection_count: data.connection_count,
                avg_latency_ms: avg_latency,
                data_transferred_mb: data.data_bytes as f64 / (1024.0 * 1024.0),
                success_rate_percentage: success_rate,
                peak_connection_hour: peak_hour,
            });
        }

        summary.sort_by(|a, b| b.connection_count.cmp(&a.connection_count));

        Ok(Some(summary))
    }

    fn classify_remote_region(remote: &str) -> String {
        if remote.contains(':') {
            return "IPv6".to_string();
        }

        let remote = remote.split('/').next().unwrap_or(remote);

        if remote.starts_with("127.") {
            "Loopback".to_string()
        } else if remote.starts_with("10.")
            || remote.starts_with("192.168.")
            || remote.starts_with("172.16.")
            || remote.starts_with("172.17.")
            || remote.starts_with("172.18.")
            || remote.starts_with("172.19.")
            || remote.starts_with("172.20.")
            || remote.starts_with("172.21.")
            || remote.starts_with("172.22.")
            || remote.starts_with("172.23.")
            || remote.starts_with("172.24.")
            || remote.starts_with("172.25.")
            || remote.starts_with("172.26.")
            || remote.starts_with("172.27.")
            || remote.starts_with("172.28.")
            || remote.starts_with("172.29.")
            || remote.starts_with("172.30.")
            || remote.starts_with("172.31.")
        {
            "Private Network".to_string()
        } else {
            "Public Network".to_string()
        }
    }
}

/// Information about slow connection establishments
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSlowConnection {
    /// Client IP address
    pub client_ip: Option<String>,
    /// Connection establishment time in milliseconds
    pub connection_time_ms: f64,
    /// Timestamp when connection was established
    pub timestamp: DateTimeWrapper,
    /// Client user agent or driver information
    pub user_agent: Option<String>,
    /// Type of command that initiated the connection
    pub command_type: Option<String>,
    /// Database being accessed
    pub database: Option<String>,
}

/// Information about failed connection attempts
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoFailedConnection {
    /// Client IP address
    pub client_ip: Option<String>,
    /// Error message
    pub error_message: String,
    /// Error code if available
    pub error_code: Option<i32>,
    /// Timestamp when connection failed
    pub timestamp: DateTimeWrapper,
    /// Number of retry attempts
    pub retry_count: u32,
    /// Client user agent or driver information
    pub user_agent: Option<String>,
    /// Connection type (SSL, Plain, etc.)
    pub connection_type: String,
}

/// Information about high-usage client applications
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoHighUsageClient {
    /// Client IP address
    pub client_ip: String,
    /// Number of concurrent connections
    pub connection_count: u64,
    /// Operations per minute
    pub operations_per_minute: f64,
    /// Client user agent or driver information
    pub user_agent: Option<String>,
    /// Last activity timestamp
    pub last_activity: DateTimeWrapper,
    /// Average connection duration in minutes
    pub connection_duration_minutes: f64,
    /// Data transferred in megabytes
    pub data_transferred_mb: f64,
}

/// Information about connection pool exhaustion events
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoPoolExhaustionEvent {
    /// Timestamp when exhaustion occurred
    pub timestamp: DateTimeWrapper,
    /// Duration of exhaustion in seconds
    pub duration_seconds: f64,
    /// Number of requests queued during exhaustion
    pub queued_requests: u64,
    /// Peak pool utilization percentage
    pub peak_utilization_percentage: f64,
    /// Contributing factors
    pub contributing_factors: Vec<String>,
    /// Recovery time in seconds
    pub recovery_time_seconds: f64,
}

/// Information about long-running idle connections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoLongIdleConnection {
    /// Client IP address
    pub client_ip: String,
    /// Duration idle in minutes
    pub idle_duration_minutes: f64,
    /// Last activity timestamp
    pub last_activity: DateTimeWrapper,
    /// User agent or application information
    pub user_agent: Option<String>,
    /// Database last accessed
    pub last_database: Option<String>,
    /// Recommended action
    pub recommended_action: String,
}

/// Information about network performance issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkIssue {
    /// Type of network issue
    pub issue_type: String,
    /// Affected client IP or range
    pub affected_client: String,
    /// Timestamp when issue was detected
    pub timestamp: DateTimeWrapper,
    /// Severity level
    pub severity: String,
    /// Impact description
    pub impact_description: String,
    /// Recommended resolution
    pub recommended_resolution: String,
}

/// Geographic distribution of connections
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoGeographicConnection {
    /// Geographic region or country
    pub region: String,
    /// Number of connections from this region
    pub connection_count: u64,
    /// Average latency from this region (milliseconds)
    pub avg_latency_ms: f64,
    /// Data transferred from this region (MB)
    pub data_transferred_mb: f64,
    /// Connection success rate percentage
    pub success_rate_percentage: f64,
    /// Peak connection time (when most connections occur)
    pub peak_connection_hour: u8,
}

impl MongoConnectionInfo {
    /// Checks if connection performance is healthy
    pub fn is_connection_healthy(&self) -> bool {
        self.failed_connections == 0
            && self.connection_timeouts == 0
            && self.pool_utilization_percentage < Self::HIGH_POOL_UTILIZATION_THRESHOLD
            && self.avg_connection_time_ms < Self::SLOW_CONNECTION_THRESHOLD_MS
    }

    /// Returns the connection success rate as a percentage
    pub fn connection_success_rate(&self) -> f64 {
        let total_attempts = self.total_connections_created + self.failed_connections;
        if total_attempts == 0 {
            100.0
        } else {
            (self.total_connections_created as f64 / total_attempts as f64) * 100.0
        }
    }

    /// Checks if the connection pool is under pressure
    pub fn is_pool_under_pressure(&self) -> bool {
        self.pool_utilization_percentage > Self::HIGH_POOL_UTILIZATION_THRESHOLD
            || self.available_connections < 5
            || self.connections_being_created > 0
    }

    /// Returns the average connections per unique client
    pub fn avg_connections_per_client(&self) -> f64 {
        if self.unique_client_ips == 0 {
            0.0
        } else {
            self.total_active_connections as f64 / self.unique_client_ips as f64
        }
    }

    /// Returns the network throughput in megabytes per second
    pub fn network_throughput_mbps(&self) -> f64 {
        self.network_throughput_bps / 1024.0 / 1024.0
    }

    /// Checks if there are performance issues with connections
    pub fn has_performance_issues(&self) -> bool {
        self.max_connection_time_ms > Self::SLOW_CONNECTION_THRESHOLD_MS || self.failed_connections > 0 || self.connection_timeouts > 0
    }

    /// Returns the percentage of SSL connections
    pub fn ssl_connection_percentage(&self) -> f64 {
        if self.total_active_connections == 0 {
            0.0
        } else {
            (self.ssl_connections as f64 / self.total_active_connections as f64) * 100.0
        }
    }

    /// Returns the idle connection percentage
    pub fn idle_connection_percentage(&self) -> f64 {
        let total_capacity = self.total_active_connections + self.available_connections;
        if total_capacity == 0 {
            0.0
        } else {
            (self.idle_connections as f64 / total_capacity as f64) * 100.0
        }
    }

    /// Calculates connection efficiency (active vs total capacity)
    pub fn connection_efficiency(&self) -> f64 {
        let total_capacity = self.total_active_connections + self.available_connections;
        if total_capacity == 0 {
            0.0
        } else {
            (self.total_active_connections as f64 / total_capacity as f64) * 100.0
        }
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Returns total data transferred in megabytes
    pub fn total_data_transferred_mb(&self) -> f64 {
        (self.total_bytes_sent + self.total_bytes_received) as f64 / 1024.0 / 1024.0
    }

    /// Calculates a connection health score from 0-100
    pub fn connection_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct points for connection failures
        if self.failed_connections > 0 {
            let failure_rate = (self.failed_connections as f64 / self.total_connections_created as f64) * 100.0;
            score -= failure_rate.min(30.0); // Max 30 point deduction
        }

        // Deduct points for slow connections
        if self.avg_connection_time_ms > Self::SLOW_CONNECTION_THRESHOLD_MS {
            score -= 20.0;
        }

        // Deduct points for high pool utilization
        if self.pool_utilization_percentage > Self::HIGH_POOL_UTILIZATION_THRESHOLD {
            let over_threshold = self.pool_utilization_percentage - Self::HIGH_POOL_UTILIZATION_THRESHOLD;
            score -= over_threshold.min(25.0); // Max 25 point deduction
        }

        // Deduct points for timeouts
        if self.connection_timeouts > 0 {
            score -= 15.0;
        }

        // Bonus points for good practices
        if self.ssl_connection_percentage() > 80.0 {
            score += 5.0; // Bonus for high SSL usage
        }

        score.clamp(0.0, 100.0)
    }

    /// Predicts if connection pool will be exhausted soon
    pub fn will_exhaust_soon(&self) -> bool {
        self.pool_utilization_percentage > 90.0 && self.connections_being_created == 0
    }

    /// Returns the connection turnover rate (connections created per active connection)
    pub fn connection_turnover_rate(&self) -> f64 {
        if self.total_active_connections == 0 {
            0.0
        } else {
            self.total_connections_created as f64 / self.total_active_connections as f64
        }
    }
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_mongo;
    use endpoint_types::metadata::PermissiveCapabilities;

    #[tokio::test]
    async fn test_mongo_connection_stats() {
        let (_mongo, endpoint_cache_uuid, mongo_ep, mut telemetry_wrapper) = connect_to_mongo().await;

        let telemetry_wrapper = &mut telemetry_wrapper;

        let connection_stats = MongoConnectionInfo::default();

        let result = connection_stats
            .sync_metadata(
                mongo_ep.0.read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let stats = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(stats.pool_utilization_percentage >= 0.0);
    }

    #[test]
    fn test_connection_health_check() {
        let mut stats = MongoConnectionInfo {
            failed_connections: 0,
            connection_timeouts: 0,
            pool_utilization_percentage: 50.0,
            avg_connection_time_ms: 500.0,
            ..Default::default()
        };

        assert!(stats.is_connection_healthy());

        stats.failed_connections = 5;
        assert!(!stats.is_connection_healthy());
    }

    #[test]
    fn test_connection_success_rate() {
        let stats = MongoConnectionInfo {
            total_connections_created: 95,
            failed_connections: 5,
            ..Default::default()
        };

        assert_eq!(stats.connection_success_rate(), 95.0);
    }

    #[test]
    fn test_pool_pressure() {
        let mut stats = MongoConnectionInfo { pool_utilization_percentage: 85.0, ..Default::default() };

        assert!(stats.is_pool_under_pressure());

        stats.pool_utilization_percentage = 50.0;
        stats.available_connections = 2;

        assert!(stats.is_pool_under_pressure());
    }

    #[test]
    fn test_avg_connections_per_client() {
        let stats = MongoConnectionInfo {
            total_active_connections: 20,
            unique_client_ips: 4,
            ..Default::default()
        };

        assert_eq!(stats.avg_connections_per_client(), 5.0);
    }

    #[test]
    fn test_ssl_connection_percentage() {
        let stats = MongoConnectionInfo {
            total_active_connections: 100,
            ssl_connections: 80,
            ..Default::default()
        };

        assert_eq!(stats.ssl_connection_percentage(), 80.0);
    }

    #[test]
    fn test_connection_efficiency() {
        let stats = MongoConnectionInfo {
            total_active_connections: 80,
            available_connections: 20,
            ..Default::default()
        };

        assert_eq!(stats.connection_efficiency(), 80.0);
    }

    #[test]
    fn test_network_throughput() {
        let stats = MongoConnectionInfo {
            network_throughput_bps: 10.0 * 1024.0 * 1024.0, // 10 MB/s
            ..Default::default()
        };

        assert_eq!(stats.network_throughput_mbps(), 10.0);
    }

    #[test]
    fn test_performance_issues() {
        let mut stats = MongoConnectionInfo {
            max_connection_time_ms: 2000.0, // Above threshold
            ..Default::default()
        };

        assert!(stats.has_performance_issues());

        stats.max_connection_time_ms = 500.0;
        stats.failed_connections = 3;

        assert!(stats.has_performance_issues());
    }

    #[test]
    fn test_connection_health_score() {
        let mut stats = MongoConnectionInfo {
            failed_connections: 0,
            avg_connection_time_ms: 500.0,
            pool_utilization_percentage: 50.0,
            connection_timeouts: 0,
            ssl_connections: 90,
            total_active_connections: 100,
            ..Default::default()
        };

        let score = stats.connection_health_score();
        assert!(score >= 95.0); // Should be high with good metrics

        stats.failed_connections = 10;
        stats.total_connections_created = 100;

        let score2 = stats.connection_health_score();
        assert!(score2 < score); // Should be lower with failures
    }

    #[test]
    fn test_pool_exhaustion_prediction() {
        let mut stats = MongoConnectionInfo {
            pool_utilization_percentage: 95.0,
            connections_being_created: 0,
            ..Default::default()
        };

        assert!(stats.will_exhaust_soon());

        stats.connections_being_created = 5; // New connections being created
        assert!(!stats.will_exhaust_soon());
    }

    #[test]
    fn test_connection_turnover_rate() {
        let stats = MongoConnectionInfo {
            total_connections_created: 200,
            total_active_connections: 50,
            ..Default::default()
        };

        assert_eq!(stats.connection_turnover_rate(), 4.0);
    }

    #[test]
    fn test_data_transfer_calculations() {
        let stats = MongoConnectionInfo {
            total_bytes_sent: 512 * 1024 * 1024,     // 512 MB
            total_bytes_received: 512 * 1024 * 1024, // 512 MB
            ..Default::default()
        };

        assert_eq!(stats.total_data_transferred_mb(), 1024.0); // 1 GB total
    }

    #[test]
    fn test_idle_connection_percentage() {
        let stats = MongoConnectionInfo {
            total_active_connections: 70,
            available_connections: 30,
            idle_connections: 15,
            ..Default::default()
        };

        assert_eq!(stats.idle_connection_percentage(), 15.0); // 15 idle out of 100 total
    }
}
