use crate::api::lib::database::collection::FindInput;
use crate::api::wrapper::{DocumentFunction, DocumentWrapper, DocumentWrapperType, FindOptionsWrapper};
use borsh::{BorshDeserialize, BorshSerialize};
use chrono::Utc;
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

/// MongoDB Network statistics and performance metrics
///
/// Comprehensive struct containing essential metrics about network
/// performance, connection patterns, and network-related bottlenecks.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkInfo {
    /// Total bytes received over network (bytes)
    pub total_bytes_received: u64,
    /// Total bytes sent over network (bytes)
    pub total_bytes_sent: u64,
    /// Network throughput incoming (bytes per second)
    pub incoming_throughput_bps: f64,
    /// Network throughput outgoing (bytes per second)
    pub outgoing_throughput_bps: f64,
    /// Average network latency (milliseconds)
    pub avg_network_latency_ms: f64,
    /// Maximum network latency observed (milliseconds)
    pub max_network_latency_ms: f64,
    /// Minimum network latency observed (milliseconds)
    pub min_network_latency_ms: f64,
    /// Number of active connections
    pub active_connections: u32,
    /// Number of available connections
    pub available_connections: u32,
    /// Total connections created
    pub total_connections_created: u64,
    /// Connection utilization percentage
    pub connection_utilization_percentage: f64,
    /// Number of connection timeouts
    pub connection_timeouts: u64,
    /// Number of connection failures
    pub connection_failures: u64,
    /// Average connection establishment time (milliseconds)
    pub avg_connection_time_ms: f64,
    /// Network requests per second
    pub requests_per_second: f64,
    /// Network responses per second
    pub responses_per_second: f64,
    /// Average request size (bytes)
    pub avg_request_size_bytes: f64,
    /// Average response size (bytes)
    pub avg_response_size_bytes: f64,
    /// Network packet loss percentage
    pub packet_loss_percentage: f64,
    /// Network congestion indicators
    pub congestion_window_size: u64,
    /// Round trip time average (milliseconds)
    pub avg_rtt_ms: f64,
    /// Bandwidth utilization percentage
    pub bandwidth_utilization_percentage: f64,
    /// TCP retransmissions per second
    pub tcp_retransmissions_per_sec: f64,
    /// SSL/TLS connection overhead (milliseconds)
    pub ssl_overhead_ms: f64,
    /// Slow network operations count
    pub slow_network_operations: u64,
    /// Network buffer overruns
    pub buffer_overruns: u64,
    /// DNS resolution time average (milliseconds)
    pub avg_dns_resolution_ms: f64,
    /// Number of replica set network calls
    pub replica_set_network_calls: u64,
    /// Sharding network overhead (bytes)
    pub sharding_network_overhead_bytes: u64,
    /// GridFS network transfers (bytes)
    pub gridfs_network_transfers_bytes: u64,
    /// Network compression ratio (0.0 to 1.0)
    pub compression_ratio: f64,
    /// Peak concurrent connections observed
    pub peak_concurrent_connections: u32,
    /// Network health score (0.0 to 1.0)
    pub network_health_score: f64,
    /// Detailed metrics collected only when problems are detected
    pub detailed_metrics: Option<MongoNetworkDetailedMetrics>,
}

/// Detailed metrics collected only when problems are detected
///
/// This reduces overhead by only collecting expensive data when needed.
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkDetailedMetrics {
    /// Network bottlenecks and congestion points
    pub network_bottlenecks: Vec<MongoNetworkBottleneck>,
    /// Slow network operations requiring attention
    pub slow_operations: Vec<MongoSlowNetworkOperation>,
    /// Connection pool issues and inefficiencies
    pub connection_issues: Vec<MongoConnectionIssue>,
    /// Network security concerns and anomalies
    pub security_issues: Vec<MongoNetworkSecurityIssue>,
    /// Bandwidth and throughput optimization opportunities
    pub optimization_opportunities: Vec<MongoNetworkOptimization>,
    /// Network performance issues
    pub performance_issues: Option<Vec<MongoNetworkPerformanceIssue>>,
    /// Client connection patterns and analysis
    pub client_patterns: Option<Vec<MongoClientConnectionPattern>>,
    /// Network configuration recommendations
    pub configuration_recommendations: Option<Vec<MongoNetworkConfigRecommendation>>,
}

/// Information about network bottlenecks and congestion points
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkBottleneck {
    pub bottleneck_type: String, // Bandwidth, Latency, Connection Pool, DNS
    pub location: String,        // Client, Server, Network Path
    pub severity_level: String,  // Critical, High, Medium, Low
    pub affected_operations: u64,
    pub avg_impact_ms: f64,
    pub peak_impact_ms: f64,
    pub congestion_indicators: Vec<String>,
    pub bandwidth_consumed_mbps: f64,
    pub connection_pool_pressure: String,
    pub detection_time: DateTimeWrapper,
    pub duration_minutes: f64,
    pub recommended_mitigation: String,
    pub urgency_level: String,
}

/// Information about slow network operations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoSlowNetworkOperation {
    pub operation_id: String,
    pub operation_type: String,
    pub client_address: String,
    pub target_collection: String,
    pub total_time_ms: f64,
    pub retry_attempts: u32,
    pub compression_used: bool,
    pub optimization_suggestion: String,
    pub priority_level: String,
}

/// Information about connection pool issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoConnectionIssue {
    pub issue_type: String, // Pool Exhaustion, Timeout, Leak, Thrashing
    pub connection_pool: String,
    pub current_active: u32,
    pub current_available: u32,
    pub max_pool_size: u32,
    pub avg_wait_time_ms: f64,
    pub timeout_count: u64,
    pub leak_indicators: Vec<String>,
    pub pool_efficiency: f64,
    pub client_distribution: HashMap<String, u32>,
    pub recommended_action: String,
    pub implementation_complexity: String,
}

/// Information about network security issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkSecurityIssue {
    pub issue_type: String, // Unencrypted Traffic, Certificate Issues, Suspicious Patterns
    pub severity: String,   // Critical, High, Medium, Low
    pub affected_connections: u32,
    pub detection_method: String,
    pub risk_assessment: String,
    pub client_locations: Vec<String>,
    pub traffic_patterns: Vec<String>,
    pub encryption_status: String,
    pub certificate_details: String,
    pub compliance_impact: String,
    pub remediation_steps: Vec<String>,
    pub monitoring_recommendations: Vec<String>,
}

/// Network optimization opportunities
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkOptimization {
    pub optimization_type: String,
    pub target_component: String,
    pub current_performance: String,
    pub expected_improvement: String,
    pub bandwidth_savings_mbps: f64,
    pub latency_reduction_ms: f64,
    pub implementation_effort: String, // Low, Medium, High
    pub cost_benefit_ratio: f64,
    pub prerequisites: Vec<String>,
    pub implementation_steps: Vec<String>,
    pub success_metrics: Vec<String>,
    pub risk_factors: Vec<String>,
}

/// Network performance bottlenecks and issues
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkPerformanceIssue {
    pub issue_type: String,
    pub severity: String, // Critical, High, Medium, Low
    pub affected_clients: u32,
    pub avg_performance_impact_ms: f64,
    pub frequency_per_hour: u64,
    pub network_threshold_exceeded: String,
    pub description: String,
    pub technical_details: String,
    pub business_impact: String,
    pub recommended_solution: String,
    pub estimated_resolution_time: String,
}

/// Client connection patterns and analysis
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoClientConnectionPattern {
    pub client_identifier: String,
    pub connection_count: u32,
    pub avg_session_duration_minutes: f64,
    pub data_transfer_mb: f64,
    pub operation_frequency: f64,
    pub connection_efficiency: f64,
    pub geographic_location: String,
    pub access_pattern: String, // Batch, Interactive, Streaming
    pub optimization_potential: String,
    pub recommended_adjustments: Vec<String>,
}

/// Network configuration recommendations
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct MongoNetworkConfigRecommendation {
    pub configuration_area: String,
    pub current_setting: String,
    pub recommended_setting: String,
    pub rationale: String,
    pub expected_impact: String,
    pub implementation_risk: String,
    pub testing_requirements: Vec<String>,
    pub monitoring_after_change: Vec<String>,
    pub rollback_procedure: String,
}

impl MetadataCollection for MongoNetworkInfo {
    type Request = HashMap<String, FindInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "server_status".to_string(),
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
                "network_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "millis": { "$gte": 100 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(30)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(500)),
                ),
            ),
            (
                "connection_events".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.connPoolStats": { "$exists": true } },
                            { "command.currentOp": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(15)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(200)),
                ),
            ),
            (
                "slow_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "millis": { "$gte": 1000 },
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(60)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "millis": -1 })).with_limit(300)),
                ),
            ),
            (
                "replica_set_operations".to_string(),
                FindInput::new(
                    "admin".to_string(),
                    "system.profile".to_string(),
                    Some(DocumentWrapperType::from_document(doc! {
                        "$or": [
                            { "command.replSetGetStatus": { "$exists": true } },
                            { "command.isMaster": { "$exists": true } },
                            { "command.replSetHeartbeat": { "$exists": true } }
                        ],
                        "ts": { "$gte": mongodb::bson::DateTime::from_chrono(
                            Utc::now() - chrono::Duration::minutes(20)
                        )}
                    })),
                    Some(FindOptionsWrapper::new().with_sort(DocumentWrapper::from(doc! { "ts": -1 })).with_limit(100)),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return comprehensive network performance and connectivity metrics"
    }

    fn category(&self) -> &'static str {
        "network"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High // Network conditions change rapidly
    }
}

impl MongoNetworkInfo {
    const HIGH_LATENCY_THRESHOLD_MS: f64 = 100.0; // 100ms
    const LOW_BANDWIDTH_THRESHOLD_MBPS: f64 = 10.0; // 10 Mbps
    const HIGH_CONNECTION_UTILIZATION: f64 = 80.0; // 80%
    const SLOW_OPERATION_NETWORK_THRESHOLD_MS: f64 = 500.0; // 500ms
    const QUERY_TIMEOUT: Duration = Duration::from_secs(15);
    const MAX_DETAILED_RESULTS: usize = 100;
    const POOR_NETWORK_HEALTH_THRESHOLD: f64 = 0.7; // 70%

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: MongoAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut network_stats = MongoNetworkInfo::default();
        let requests = self.request();

        // Execute serverStatus directly - contains all network data
        let server_status_docs =
            execute_admin_command_as_profiled(doc! { "serverStatus": 1 }, context.clone(), Self::QUERY_TIMEOUT, "serverStatus").await?;
        Self::parse_server_status(&mut network_stats, &server_status_docs)?;

        // Profile-backed context for slower diagnostics
        let network_operations = Self::fetch(&requests, "network_operations", context.clone()).await?;
        Self::parse_network_operations(&mut network_stats, &network_operations)?;

        let connection_events = Self::fetch(&requests, "connection_events", context.clone()).await?;
        Self::parse_connection_events(&mut network_stats, &connection_events)?;

        let slow_operations = Self::fetch(&requests, "slow_operations", context.clone()).await?;
        Self::parse_slow_operations(&mut network_stats, &slow_operations)?;

        let replica_operations = Self::fetch(&requests, "replica_set_operations", context.clone()).await?;
        Self::parse_replica_set_operations(&mut network_stats, &replica_operations)?;

        // Calculate derived metrics
        Self::calculate_derived_metrics(&mut network_stats)?;

        // Conditionally collect detailed metrics only when problems are detected
        network_stats.detailed_metrics = self
            .collect_detailed_metrics_if_needed(
                &network_stats,
                &slow_operations,
                &connection_events,
                &network_operations,
                &replica_operations,
            )
            .await?;

        Ok(network_stats)
    }

    async fn collect_detailed_metrics_if_needed(
        &self,
        core_stats: &MongoNetworkInfo,
        slow_operations: &[Document],
        connection_events: &[Document],
        _network_operations: &[Document],
        _replica_operations: &[Document],
    ) -> ResultEP<Option<MongoNetworkDetailedMetrics>> {
        let needs_bottleneck_analysis = core_stats.avg_network_latency_ms > Self::HIGH_LATENCY_THRESHOLD_MS;
        let needs_slow_ops_analysis = core_stats.slow_network_operations > 10;
        let needs_connection_analysis = core_stats.connection_utilization_percentage > Self::HIGH_CONNECTION_UTILIZATION;
        let needs_security_analysis = core_stats.ssl_overhead_ms > 50.0;
        let needs_bandwidth_analysis = core_stats.incoming_throughput_bps < (Self::LOW_BANDWIDTH_THRESHOLD_MBPS * 1024.0 * 1024.0);
        let needs_health_analysis = core_stats.network_health_score < Self::POOR_NETWORK_HEALTH_THRESHOLD;

        if !needs_bottleneck_analysis
            && !needs_slow_ops_analysis
            && !needs_connection_analysis
            && !needs_security_analysis
            && !needs_bandwidth_analysis
            && !needs_health_analysis
        {
            return Ok(None);
        }

        let mut detailed_metrics = MongoNetworkDetailedMetrics {
            network_bottlenecks: Vec::new(),
            slow_operations: Vec::new(),
            connection_issues: Vec::new(),
            security_issues: Vec::new(),
            optimization_opportunities: Vec::new(),
            performance_issues: None,
            client_patterns: None,
            configuration_recommendations: None,
        };

        if needs_bottleneck_analysis {
            detailed_metrics.network_bottlenecks = Self::identify_network_bottlenecks(core_stats)?;
        }

        if needs_slow_ops_analysis {
            detailed_metrics.slow_operations = Self::analyze_slow_network_operations(slow_operations)?;
        }

        if needs_connection_analysis {
            detailed_metrics.connection_issues = Self::analyze_connection_issues(core_stats)?;
            let event_issues = Self::analyze_connection_events_for_issues(connection_events)?;
            if !event_issues.is_empty() {
                detailed_metrics.connection_issues.extend(event_issues);
            }
        }

        if needs_security_analysis {
            detailed_metrics.security_issues = Self::analyze_security_issues(core_stats)?;
        }

        if needs_bandwidth_analysis {
            detailed_metrics.optimization_opportunities = Self::identify_optimization_opportunities(core_stats)?;
        }

        if needs_health_analysis {
            detailed_metrics.performance_issues = Some(Self::analyze_performance_issues(core_stats)?);
            detailed_metrics.client_patterns = Some(Self::analyze_client_patterns(core_stats)?);
            detailed_metrics.configuration_recommendations = Some(Self::generate_config_recommendations(core_stats)?);
        }

        let has_details = !detailed_metrics.network_bottlenecks.is_empty()
            || !detailed_metrics.slow_operations.is_empty()
            || !detailed_metrics.connection_issues.is_empty()
            || !detailed_metrics.security_issues.is_empty()
            || !detailed_metrics.optimization_opportunities.is_empty()
            || detailed_metrics.performance_issues.is_some()
            || detailed_metrics.client_patterns.is_some()
            || detailed_metrics.configuration_recommendations.is_some();

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
                collector = "mongo.network",
                key = key
            );
        }
        Ok(docs)
    }

    fn parse_server_status(stats: &mut MongoNetworkInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if let Some(network) = result.child("network") {
                    if let Some(bytes_in) = network.opt_u64("bytesIn") {
                        stats.total_bytes_received = bytes_in;
                    }
                    if let Some(bytes_out) = network.opt_u64("bytesOut") {
                        stats.total_bytes_sent = bytes_out;
                    }
                    if let Some(num_requests) = network.opt_u64("numRequests") {
                        stats.requests_per_second = num_requests as f64 / 300.0;
                    }
                    if let Some(compression) = network.child("compression") {
                        Self::parse_compression_stats(stats, compression.raw())?;
                    }
                }

                if let Some(connections) = result.child("connections") {
                    if let Some(current) = connections.opt_i32("current") {
                        stats.active_connections = current as u32;
                    }
                    if let Some(available) = connections.opt_i32("available") {
                        stats.available_connections = available as u32;
                    }
                    if let Some(total_created) = connections.opt_u64("totalCreated") {
                        stats.total_connections_created = total_created;
                    }
                }

                if let Some(opcounters) = result.child("opcounters") {
                    let mut total_ops = 0i64;
                    for (_, count) in opcounters.raw() {
                        if let Some(op_count) = count.as_i64() {
                            total_ops += op_count;
                        }
                    }
                    stats.responses_per_second = total_ops as f64 / 300.0;
                }

                if let Some(metrics) = result.child("metrics")
                    && let Some(document_metrics) = metrics.child("document")
                {
                    Self::parse_document_metrics(stats, document_metrics.raw())?;
                }
            }
        }

        Ok(())
    }

    fn parse_compression_stats(stats: &mut MongoNetworkInfo, compression_doc: &Document) -> ResultEP<()> {
        // Parse compression ratios
        for (_compressor, comp_stats) in compression_doc {
            if let Some(comp_doc) = comp_stats.as_document() {
                let acc = DocAccessor::new(comp_doc);
                if let (Some(compressed), Some(uncompressed)) = (acc.opt_i64("compressed"), acc.opt_i64("uncompressed"))
                    && uncompressed > 0
                {
                    let ratio = compressed as f64 / uncompressed as f64;
                    stats.compression_ratio =
                        std::cmp::max_by(stats.compression_ratio, ratio, |a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
                }
            }
        }

        Ok(())
    }

    fn parse_document_metrics(stats: &mut MongoNetworkInfo, doc_metrics: &Document) -> ResultEP<()> {
        let acc = DocAccessor::new(doc_metrics);
        if let Some(returned) = acc.opt_i64("returned") {
            stats.avg_response_size_bytes = returned as f64 * 1024.0;
        }
        if let Some(inserted) = acc.opt_i64("inserted") {
            stats.avg_request_size_bytes = inserted as f64 * 512.0;
        }

        Ok(())
    }

    fn parse_network_operations(stats: &mut MongoNetworkInfo, docs: &[Document]) -> ResultEP<()> {
        let mut latency_samples = Vec::new();
        let mut slow_network_count = 0;

        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(millis) = acc.opt_f64("millis") {
                latency_samples.push(millis);

                if millis > Self::SLOW_OPERATION_NETWORK_THRESHOLD_MS {
                    slow_network_count += 1;
                }
            }

            if let Some(error_code) = acc.opt_i32("errCode")
                && (error_code == 89 || error_code == 11600)
            {
                stats.connection_timeouts += 1;
            }
        }

        stats.slow_network_operations = slow_network_count;

        if !latency_samples.is_empty() {
            stats.avg_network_latency_ms = latency_samples.iter().sum::<f64>() / latency_samples.len() as f64;
            stats.max_network_latency_ms = latency_samples.iter().fold(0.0f64, |a, &b| a.max(b));
            stats.min_network_latency_ms = latency_samples.iter().fold(f64::INFINITY, |a, &b| a.min(b));

            if stats.min_network_latency_ms == f64::INFINITY {
                stats.min_network_latency_ms = 0.0;
            }
        }

        Ok(())
    }

    fn parse_connection_events(stats: &mut MongoNetworkInfo, docs: &[Document]) -> ResultEP<()> {
        let mut connection_times = Vec::new();
        let mut connection_failures = 0;
        let mut max_concurrent = 0u32;

        for doc in docs {
            if let Some(result) = DocAccessor::new(doc).child("result") {
                if result.raw().contains_key("hosts") {
                    if let Some(in_use) = result.opt_i32("inUse") {
                        max_concurrent = std::cmp::max(max_concurrent, in_use as u32);
                    }
                    if let Some(available) = result.opt_i32("available") {
                        stats.available_connections = std::cmp::max(stats.available_connections, available as u32);
                    }
                    if let Some(created) = result.opt_u64("created") {
                        stats.total_connections_created += created;
                    }
                }

                if let Ok(in_prog) = result.raw().get_array("inprog") {
                    for op_value in in_prog {
                        if let Some(operation) = op_value.as_document() {
                            let op_acc = DocAccessor::new(operation);
                            if let Some(secs_running) = op_acc.opt_i64("secs_running")
                                && secs_running < 5
                            {
                                connection_times.push(secs_running as f64 * 1000.0);
                            }

                            if let Some(error) = op_acc.opt_string("err")
                                && (error.contains("connection") || error.contains("network"))
                            {
                                connection_failures += 1;
                            }
                        }
                    }
                }
            }
        }

        stats.peak_concurrent_connections = std::cmp::max(stats.peak_concurrent_connections, max_concurrent);
        stats.connection_failures = connection_failures;

        if !connection_times.is_empty() {
            stats.avg_connection_time_ms = connection_times.iter().sum::<f64>() / connection_times.len() as f64;
        }

        Ok(())
    }

    fn parse_slow_operations(stats: &mut MongoNetworkInfo, docs: &[Document]) -> ResultEP<()> {
        for doc in docs {
            let acc = DocAccessor::new(doc);
            if let Some(millis) = acc.opt_f64("millis")
                && let Some(ns) = acc.opt_string("ns")
                && (ns.contains(".fs.files") || ns.contains(".fs.chunks"))
            {
                stats.gridfs_network_transfers_bytes += (millis * 1024.0) as u64;
            }
        }

        Ok(())
    }

    fn parse_replica_set_operations(stats: &mut MongoNetworkInfo, docs: &[Document]) -> ResultEP<()> {
        stats.replica_set_network_calls = docs.len() as u64;
        Ok(())
    }

    fn calculate_derived_metrics(stats: &mut MongoNetworkInfo) -> ResultEP<()> {
        // Calculate throughput (assuming 5-minute measurement window)
        let time_window_seconds = 300.0;
        stats.incoming_throughput_bps = stats.total_bytes_received as f64 / time_window_seconds;
        stats.outgoing_throughput_bps = stats.total_bytes_sent as f64 / time_window_seconds;

        // Calculate connection utilization
        let total_connections = stats.active_connections + stats.available_connections;
        if total_connections > 0 {
            stats.connection_utilization_percentage = (stats.active_connections as f64 / total_connections as f64) * 100.0;
        }

        if stats.connection_timeouts > 0 && stats.total_connections_created > 0 {
            stats.packet_loss_percentage = (stats.connection_timeouts as f64 / stats.total_connections_created as f64) * 100.0;
        }

        stats.tcp_retransmissions_per_sec = stats.connection_failures as f64 / time_window_seconds;

        // Calculate network health score
        let mut health_factors = Vec::new();

        // Latency factor (lower is better)
        let latency_factor = if stats.avg_network_latency_ms < 50.0 {
            1.0
        } else if stats.avg_network_latency_ms < 100.0 {
            0.8
        } else if stats.avg_network_latency_ms < 200.0 {
            0.6
        } else {
            0.3
        };
        health_factors.push(latency_factor);

        // Connection stability factor
        let connection_factor = if stats.connection_failures == 0 {
            1.0
        } else if stats.connection_failures < 10 {
            0.8
        } else {
            0.5
        };
        health_factors.push(connection_factor);

        // Throughput factor (derive from actual measured throughput)
        let total_throughput_mbps = (stats.incoming_throughput_bps + stats.outgoing_throughput_bps) / (1024.0 * 1024.0);
        let throughput_factor = if total_throughput_mbps > 50.0 {
            1.0
        } else if total_throughput_mbps > 10.0 {
            0.8
        } else {
            0.6
        };
        health_factors.push(throughput_factor);

        // Packet loss factor
        let packet_loss_factor = if stats.packet_loss_percentage < 0.1 {
            1.0
        } else if stats.packet_loss_percentage < 1.0 {
            0.7
        } else {
            0.3
        };
        health_factors.push(packet_loss_factor);

        stats.network_health_score = health_factors.iter().sum::<f64>() / health_factors.len() as f64;

        // Calculate congestion window size (estimate)
        stats.congestion_window_size = if stats.avg_network_latency_ms > 100.0 { 32768 } else { 65536 };

        // Buffer overruns estimation
        if stats.slow_network_operations > 50 {
            stats.buffer_overruns = stats.slow_network_operations / 10; // Rough estimate
        }

        Ok(())
    }

    fn identify_network_bottlenecks(stats: &MongoNetworkInfo) -> ResultEP<Vec<MongoNetworkBottleneck>> {
        let mut bottlenecks = Vec::new();

        // High latency bottleneck
        if stats.avg_network_latency_ms > Self::HIGH_LATENCY_THRESHOLD_MS {
            bottlenecks.push(MongoNetworkBottleneck {
                bottleneck_type: "High Latency".to_string(),
                location: "Network Path".to_string(),
                severity_level: if stats.avg_network_latency_ms > 200.0 {
                    "Critical".to_string()
                } else {
                    "High".to_string()
                },
                affected_operations: stats.slow_network_operations,
                avg_impact_ms: stats.avg_network_latency_ms,
                peak_impact_ms: stats.max_network_latency_ms,
                congestion_indicators: vec![
                    "High round-trip times".to_string(),
                    "Increased operation duration".to_string(),
                    "Network queue buildup".to_string(),
                ],
                bandwidth_consumed_mbps: (stats.incoming_throughput_bps + stats.outgoing_throughput_bps) / (1024.0 * 1024.0),
                connection_pool_pressure: "Medium".to_string(),
                detection_time: DateTimeWrapper::from(Utc::now()),
                duration_minutes: 15.0,
                recommended_mitigation: "Investigate network path, optimize queries, consider connection pooling".to_string(),
                urgency_level: "High".to_string(),
            });
        }

        // Bandwidth bottleneck
        if stats.bandwidth_utilization_percentage > 80.0 {
            bottlenecks.push(MongoNetworkBottleneck {
                bottleneck_type: "Bandwidth Saturation".to_string(),
                location: "Network Interface".to_string(),
                severity_level: "High".to_string(),
                affected_operations: 0, // All operations affected
                avg_impact_ms: 50.0,
                peak_impact_ms: 150.0,
                congestion_indicators: vec![
                    "High bandwidth utilization".to_string(),
                    "Increased transfer times".to_string(),
                    "Queue delays".to_string(),
                ],
                bandwidth_consumed_mbps: (stats.incoming_throughput_bps + stats.outgoing_throughput_bps) / (1024.0 * 1024.0),
                connection_pool_pressure: "High".to_string(),
                detection_time: DateTimeWrapper::from(Utc::now()),
                duration_minutes: 10.0,
                recommended_mitigation: "Increase bandwidth, implement compression, optimize data transfer patterns".to_string(),
                urgency_level: "Medium".to_string(),
            });
        }

        // Connection pool bottleneck
        if stats.connection_utilization_percentage > Self::HIGH_CONNECTION_UTILIZATION {
            bottlenecks.push(MongoNetworkBottleneck {
                bottleneck_type: "Connection Pool Exhaustion".to_string(),
                location: "Connection Pool".to_string(),
                severity_level: "Medium".to_string(),
                affected_operations: stats.connection_timeouts,
                avg_impact_ms: stats.avg_connection_time_ms,
                peak_impact_ms: stats.avg_connection_time_ms * 2.0,
                congestion_indicators: vec![
                    "High connection utilization".to_string(),
                    "Connection timeouts".to_string(),
                    "Pool exhaustion events".to_string(),
                ],
                bandwidth_consumed_mbps: 0.0,
                connection_pool_pressure: "Critical".to_string(),
                detection_time: DateTimeWrapper::from(Utc::now()),
                duration_minutes: 5.0,
                recommended_mitigation: "Increase connection pool size, optimize connection lifecycle".to_string(),
                urgency_level: "High".to_string(),
            });
        }

        Ok(bottlenecks)
    }

    fn analyze_slow_network_operations(docs: &[Document]) -> ResultEP<Vec<MongoSlowNetworkOperation>> {
        let mut slow_ops = Vec::new();
        let mut processed = 0;

        for doc in docs {
            if processed >= Self::MAX_DETAILED_RESULTS {
                break;
            }

            let acc = DocAccessor::new(doc);
            if let Some(millis) = acc.opt_f64("millis")
                && millis > Self::SLOW_OPERATION_NETWORK_THRESHOLD_MS
            {
                let client_addr = acc.opt_string("client").unwrap_or_else(|| "unknown".to_string());
                let ns = acc.opt_string("ns").unwrap_or_else(|| "unknown".to_string());
                let collection = ns.split('.').next_back().unwrap_or("unknown");

                slow_ops.push(MongoSlowNetworkOperation {
                    operation_id: format!("slow_net_op_{}", processed),
                    operation_type: Self::extract_operation_type(doc),
                    client_address: client_addr,
                    target_collection: collection.to_string(),
                    total_time_ms: millis,
                    retry_attempts: 0,
                    compression_used: acc.opt_bool("compression").unwrap_or(false),
                    optimization_suggestion: Self::suggest_network_optimization(doc, millis),
                    priority_level: if millis > 5000.0 {
                        "High".to_string()
                    } else {
                        "Medium".to_string()
                    },
                });

                processed += 1;
            }
        }

        Ok(slow_ops)
    }

    fn extract_operation_type(doc: &Document) -> String {
        let acc = DocAccessor::new(doc);
        if let Some(command) = acc.child("command") {
            for key in command.raw().keys() {
                match key.as_str() {
                    "find" => return "find".to_string(),
                    "aggregate" => return "aggregate".to_string(),
                    "insert" => return "insert".to_string(),
                    "update" => return "update".to_string(),
                    "delete" => return "delete".to_string(),
                    _ => continue,
                }
            }
        }
        "unknown".to_string()
    }

    fn suggest_network_optimization(_doc: &Document, network_time_ms: f64) -> String {
        if network_time_ms > 1000.0 {
            "Consider result pagination, connection pooling, and data compression".to_string()
        } else if network_time_ms > 500.0 {
            "Optimize query to reduce data transfer, consider indexing".to_string()
        } else {
            "Review connection settings and network path optimization".to_string()
        }
    }

    fn analyze_connection_issues(stats: &MongoNetworkInfo) -> ResultEP<Vec<MongoConnectionIssue>> {
        let mut issues = Vec::new();

        // Connection pool exhaustion
        if stats.connection_utilization_percentage > Self::HIGH_CONNECTION_UTILIZATION {
            issues.push(MongoConnectionIssue {
                issue_type: "Pool Exhaustion".to_string(),
                connection_pool: "Primary Pool".to_string(),
                current_active: stats.active_connections,
                current_available: stats.available_connections,
                max_pool_size: stats.active_connections + stats.available_connections,
                avg_wait_time_ms: stats.avg_connection_time_ms,
                timeout_count: stats.connection_timeouts,
                leak_indicators: if stats.connection_failures > 10 {
                    vec![
                        "High connection failure rate".to_string(),
                        "Unbalanced create/destroy ratio".to_string(),
                    ]
                } else {
                    vec![]
                },
                pool_efficiency: (stats.active_connections as f64 / (stats.active_connections + stats.available_connections) as f64)
                    * 100.0,
                client_distribution: HashMap::new(),
                recommended_action: "Increase pool size, implement connection lifecycle management".to_string(),
                implementation_complexity: "Medium".to_string(),
            });
        }

        // Connection timeout issues
        if stats.connection_timeouts > 10 {
            issues.push(MongoConnectionIssue {
                issue_type: "Timeout".to_string(),
                connection_pool: "All Pools".to_string(),
                current_active: stats.active_connections,
                current_available: stats.available_connections,
                max_pool_size: 100,        // Estimate
                avg_wait_time_ms: 30000.0, // Default timeout
                timeout_count: stats.connection_timeouts,
                leak_indicators: vec!["Network connectivity issues".to_string()],
                pool_efficiency: 50.0, // Reduced due to timeouts
                client_distribution: HashMap::new(),
                recommended_action: "Review network connectivity and timeout settings".to_string(),
                implementation_complexity: "Low".to_string(),
            });
        }

        Ok(issues)
    }

    fn analyze_connection_events_for_issues(connection_events: &[Document]) -> ResultEP<Vec<MongoConnectionIssue>> {
        let mut issues = Vec::new();

        for doc in connection_events.iter().take(Self::MAX_DETAILED_RESULTS) {
            let acc = DocAccessor::new(doc);
            let result = match acc.child("result") {
                Some(result) => result,
                None => continue,
            };

            if !(result.raw().contains_key("inUse") || result.raw().contains_key("available")) {
                continue;
            }

            let in_use = result.opt_u64("inUse").unwrap_or(0) as u32;
            let available = result.opt_u64("available").unwrap_or(0) as u32;
            let queued = result.opt_u64("totalQueued").unwrap_or(0);
            let creating = result.opt_u64("creatingConnections").unwrap_or(0);

            let max_pool_size = in_use + available;
            if max_pool_size == 0 {
                continue;
            }

            let utilization = in_use as f64 / max_pool_size as f64;
            if utilization < 0.85 && available > 0 {
                continue;
            }

            let client_distribution = result
                .raw()
                .get_document("hosts")
                .ok()
                .map(|hosts| {
                    hosts
                        .iter()
                        .filter_map(|(host, value)| value.as_document().map(|doc| (host, doc)))
                        .filter_map(|(host, host_doc)| {
                            let host_acc = DocAccessor::new(host_doc);
                            let host_in_use = host_acc.opt_u64("inUse").unwrap_or(0) as u32;
                            if host_in_use == 0 {
                                None
                            } else {
                                Some((host.to_string(), host_in_use))
                            }
                        })
                        .collect::<HashMap<String, u32>>()
                })
                .unwrap_or_default();

            let avg_wait_time_ms = acc.opt_f64("millis").unwrap_or(0.0);
            let pool_name = acc.opt_string("ns").unwrap_or_else(|| "admin.$cmd".to_string());

            issues.push(MongoConnectionIssue {
                issue_type: if available == 0 {
                    "Pool Exhaustion".to_string()
                } else {
                    "Elevated Pool Utilization".to_string()
                },
                connection_pool: pool_name,
                current_active: in_use,
                current_available: available,
                max_pool_size,
                avg_wait_time_ms,
                timeout_count: queued.max(creating),
                leak_indicators: if creating > 0 {
                    vec!["Pool spawning new connections under load".to_string()]
                } else {
                    Vec::new()
                },
                pool_efficiency: utilization * 100.0,
                client_distribution,
                recommended_action: if available == 0 {
                    "Increase pool size or reduce client concurrency".to_string()
                } else {
                    "Monitor pool usage and tune max pool size thresholds".to_string()
                },
                implementation_complexity: if available == 0 { "Medium".to_string() } else { "Low".to_string() },
            });
        }

        Ok(issues)
    }

    fn analyze_security_issues(stats: &MongoNetworkInfo) -> ResultEP<Vec<MongoNetworkSecurityIssue>> {
        let mut issues = Vec::new();

        // SSL overhead analysis
        if stats.ssl_overhead_ms > 50.0 {
            issues.push(MongoNetworkSecurityIssue {
                issue_type: "High SSL Overhead".to_string(),
                severity: "Medium".to_string(),
                affected_connections: stats.active_connections,
                detection_method: "Performance monitoring".to_string(),
                risk_assessment: "Performance impact without security compromise".to_string(),
                client_locations: vec!["Various".to_string()],
                traffic_patterns: vec!["All encrypted connections".to_string()],
                encryption_status: "TLS enabled with performance impact".to_string(),
                certificate_details: "Standard certificate chain".to_string(),
                compliance_impact: "Low - encryption maintained".to_string(),
                remediation_steps: vec![
                    "Optimize SSL/TLS configuration".to_string(),
                    "Consider certificate caching".to_string(),
                    "Review cipher suite selection".to_string(),
                ],
                monitoring_recommendations: vec![
                    "Track SSL handshake times".to_string(),
                    "Monitor certificate expiration".to_string(),
                    "Alert on encryption failures".to_string(),
                ],
            });
        }

        // Unencrypted traffic detection (if SSL overhead is very low)
        if stats.ssl_overhead_ms < 1.0 && stats.active_connections > 10 {
            issues.push(MongoNetworkSecurityIssue {
                issue_type: "Potential Unencrypted Traffic".to_string(),
                severity: "High".to_string(),
                affected_connections: stats.active_connections,
                detection_method: "Low SSL overhead analysis".to_string(),
                risk_assessment: "High - potential data exposure".to_string(),
                client_locations: vec!["Internal networks".to_string()],
                traffic_patterns: vec!["Unencrypted database connections".to_string()],
                encryption_status: "Possibly disabled or misconfigured".to_string(),
                certificate_details: "Not applicable".to_string(),
                compliance_impact: "High - compliance violations possible".to_string(),
                remediation_steps: vec![
                    "Enable TLS for all connections".to_string(),
                    "Configure proper certificates".to_string(),
                    "Update client connection strings".to_string(),
                ],
                monitoring_recommendations: vec![
                    "Enforce TLS-only connections".to_string(),
                    "Monitor for unencrypted attempts".to_string(),
                    "Regular security audits".to_string(),
                ],
            });
        }

        // Connection failure analysis for potential security issues
        if stats.connection_failures > 20 {
            issues.push(MongoNetworkSecurityIssue {
                issue_type: "Suspicious Connection Patterns".to_string(),
                severity: "Medium".to_string(),
                affected_connections: stats.connection_failures as u32,
                detection_method: "Connection failure pattern analysis".to_string(),
                risk_assessment: "Medium - potential unauthorized access attempts".to_string(),
                client_locations: vec!["Multiple sources".to_string()],
                traffic_patterns: vec![
                    "High connection failure rate".to_string(),
                    "Potential brute force attempts".to_string(),
                ],
                encryption_status: "Unknown - connections failed".to_string(),
                certificate_details: "Connection failed before certificate exchange".to_string(),
                compliance_impact: "Medium - potential security incident".to_string(),
                remediation_steps: vec![
                    "Review connection failure logs".to_string(),
                    "Implement connection rate limiting".to_string(),
                    "Enable detailed security logging".to_string(),
                    "Consider IP-based access controls".to_string(),
                ],
                monitoring_recommendations: vec![
                    "Monitor connection failure rates".to_string(),
                    "Track client IP addresses".to_string(),
                    "Set up alerting for anomalous patterns".to_string(),
                    "Implement intrusion detection".to_string(),
                ],
            });
        }

        // Network health degradation security implications
        if stats.network_health_score < 0.5 && stats.packet_loss_percentage > 2.0 {
            issues.push(MongoNetworkSecurityIssue {
                issue_type: "Network Degradation Security Risk".to_string(),
                severity: "Medium".to_string(),
                affected_connections: stats.active_connections,
                detection_method: "Network health and packet loss analysis".to_string(),
                risk_assessment: "Medium - potential network-based attacks or infrastructure compromise".to_string(),
                client_locations: vec!["All network paths".to_string()],
                traffic_patterns: vec![
                    format!("Packet loss: {:.2}%", stats.packet_loss_percentage),
                    format!("Network health: {:.1}%", stats.network_health_score * 100.0),
                ],
                encryption_status: "Encrypted but potentially compromised path".to_string(),
                certificate_details: "Valid but network integrity concerns".to_string(),
                compliance_impact: "Medium - data integrity concerns".to_string(),
                remediation_steps: vec![
                    "Investigate network infrastructure".to_string(),
                    "Check for network-based attacks".to_string(),
                    "Verify network equipment security".to_string(),
                    "Consider alternative network paths".to_string(),
                ],
                monitoring_recommendations: vec![
                    "Monitor network quality metrics".to_string(),
                    "Implement network intrusion detection".to_string(),
                    "Track packet integrity".to_string(),
                    "Alert on network anomalies".to_string(),
                ],
            });
        }

        // High bandwidth usage as potential data exfiltration indicator
        if stats.outgoing_throughput_bps > stats.incoming_throughput_bps * 3.0 && stats.outgoing_throughput_bps > 100.0 * 1024.0 * 1024.0 {
            // 100 Mbps threshold
            issues.push(MongoNetworkSecurityIssue {
                issue_type: "Anomalous Data Transfer Pattern".to_string(),
                severity: "High".to_string(),
                affected_connections: stats.active_connections,
                detection_method: "Bandwidth pattern analysis".to_string(),
                risk_assessment: "High - potential data exfiltration or unauthorized access".to_string(),
                client_locations: vec!["High-throughput clients".to_string()],
                traffic_patterns: vec![
                    format!("Outgoing: {:.1} Mbps", stats.outgoing_throughput_bps / (1024.0 * 1024.0)),
                    format!("Incoming: {:.1} Mbps", stats.incoming_throughput_bps / (1024.0 * 1024.0)),
                    "Unusual outbound data volume".to_string(),
                ],
                encryption_status: "Encrypted but high volume".to_string(),
                certificate_details: "Valid certificates".to_string(),
                compliance_impact: "High - potential data breach".to_string(),
                remediation_steps: vec![
                    "Audit high-volume data transfers".to_string(),
                    "Review client access permissions".to_string(),
                    "Implement data loss prevention".to_string(),
                    "Investigate unusual query patterns".to_string(),
                ],
                monitoring_recommendations: vec![
                    "Monitor data transfer volumes".to_string(),
                    "Track client data access patterns".to_string(),
                    "Implement anomaly detection".to_string(),
                    "Set up data exfiltration alerts".to_string(),
                ],
            });
        }

        Ok(issues)
    }

    fn identify_optimization_opportunities(stats: &MongoNetworkInfo) -> ResultEP<Vec<MongoNetworkOptimization>> {
        let mut optimizations = Vec::new();

        // Compression optimization
        if stats.compression_ratio < 0.5 && stats.total_bytes_sent > 100 * 1024 * 1024 {
            // 100MB threshold
            optimizations.push(MongoNetworkOptimization {
                optimization_type: "Network Compression".to_string(),
                target_component: "Data Transfer".to_string(),
                current_performance: format!("{:.1}% compression ratio", stats.compression_ratio * 100.0),
                expected_improvement: "30-50% reduction in network traffic".to_string(),
                bandwidth_savings_mbps: (stats.outgoing_throughput_bps / (1024.0 * 1024.0)) * 0.4,
                latency_reduction_ms: stats.avg_network_latency_ms * 0.2,
                implementation_effort: "Low".to_string(),
                cost_benefit_ratio: 4.0, // High benefit, low cost
                prerequisites: vec![
                    "Client driver support for compression".to_string(),
                    "CPU capacity for compression overhead".to_string(),
                ],
                implementation_steps: vec![
                    "Enable compression in MongoDB configuration".to_string(),
                    "Update client connection strings".to_string(),
                    "Monitor compression performance".to_string(),
                ],
                success_metrics: vec![
                    "Bandwidth usage reduced by 30%".to_string(),
                    "Network latency improved".to_string(),
                    "No significant CPU impact".to_string(),
                ],
                risk_factors: vec!["Increased CPU usage".to_string(), "Potential compatibility issues".to_string()],
            });
        }

        // Connection pooling optimization
        if stats.connection_utilization_percentage > 90.0 {
            optimizations.push(MongoNetworkOptimization {
                optimization_type: "Connection Pool Optimization".to_string(),
                target_component: "Connection Management".to_string(),
                current_performance: format!("{:.1}% pool utilization", stats.connection_utilization_percentage),
                expected_improvement: "Reduced connection latency and improved scalability".to_string(),
                bandwidth_savings_mbps: 0.0,
                latency_reduction_ms: stats.avg_connection_time_ms * 0.5,
                implementation_effort: "Medium".to_string(),
                cost_benefit_ratio: 3.0,
                prerequisites: vec![
                    "Application architecture review".to_string(),
                    "Load testing environment".to_string(),
                ],
                implementation_steps: vec![
                    "Increase connection pool size".to_string(),
                    "Implement connection health checks".to_string(),
                    "Optimize connection lifecycle".to_string(),
                ],
                success_metrics: vec![
                    "Pool utilization < 80%".to_string(),
                    "Connection timeouts reduced".to_string(),
                    "Improved application response times".to_string(),
                ],
                risk_factors: vec![
                    "Increased memory usage".to_string(),
                    "Potential server resource pressure".to_string(),
                ],
            });
        }

        Ok(optimizations)
    }

    fn analyze_performance_issues(stats: &MongoNetworkInfo) -> ResultEP<Vec<MongoNetworkPerformanceIssue>> {
        let mut issues = Vec::new();

        // High latency issue
        if stats.avg_network_latency_ms > Self::HIGH_LATENCY_THRESHOLD_MS {
            issues.push(MongoNetworkPerformanceIssue {
                issue_type: "High Network Latency".to_string(),
                severity: if stats.avg_network_latency_ms > 200.0 {
                    "Critical".to_string()
                } else {
                    "High".to_string()
                },
                affected_clients: (stats.active_connections as f64 * 0.8) as u32,
                avg_performance_impact_ms: stats.avg_network_latency_ms,
                frequency_per_hour: 3600, // Constant issue
                network_threshold_exceeded: format!("{}ms > {}ms", stats.avg_network_latency_ms, Self::HIGH_LATENCY_THRESHOLD_MS),
                description: "Network latency exceeding acceptable thresholds".to_string(),
                technical_details: format!(
                    "Average: {:.1}ms, Max: {:.1}ms, Affected operations: {}",
                    stats.avg_network_latency_ms, stats.max_network_latency_ms, stats.slow_network_operations
                ),
                business_impact: "Reduced application responsiveness and user experience".to_string(),
                recommended_solution: "Network path optimization, connection pooling, query optimization".to_string(),
                estimated_resolution_time: "2-8 hours depending on root cause".to_string(),
            });
        }

        // Connection pool pressure
        if stats.connection_utilization_percentage > Self::HIGH_CONNECTION_UTILIZATION {
            issues.push(MongoNetworkPerformanceIssue {
                issue_type: "Connection Pool Pressure".to_string(),
                severity: "High".to_string(),
                affected_clients: stats.active_connections,
                avg_performance_impact_ms: stats.avg_connection_time_ms,
                frequency_per_hour: stats.connection_timeouts,
                network_threshold_exceeded: format!(
                    "{:.1}% > {:.1}%",
                    stats.connection_utilization_percentage,
                    Self::HIGH_CONNECTION_UTILIZATION
                ),
                description: "Connection pool approaching capacity limits".to_string(),
                technical_details: format!(
                    "Active: {}, Available: {}, Timeouts: {}",
                    stats.active_connections, stats.available_connections, stats.connection_timeouts
                ),
                business_impact: "Risk of connection timeouts and service unavailability".to_string(),
                recommended_solution: "Increase pool size, optimize connection usage patterns".to_string(),
                estimated_resolution_time: "1-2 hours for configuration changes".to_string(),
            });
        }

        // Poor network health
        if stats.network_health_score < Self::POOR_NETWORK_HEALTH_THRESHOLD {
            issues.push(MongoNetworkPerformanceIssue {
                issue_type: "Poor Network Health".to_string(),
                severity: "Medium".to_string(),
                affected_clients: stats.active_connections,
                avg_performance_impact_ms: 100.0,
                frequency_per_hour: 1,
                network_threshold_exceeded: format!(
                    "{:.1}% < {:.1}%",
                    stats.network_health_score * 100.0,
                    Self::POOR_NETWORK_HEALTH_THRESHOLD * 100.0
                ),
                description: "Overall network health below acceptable standards".to_string(),
                technical_details: format!(
                    "Health score: {:.1}%, Packet loss: {:.2}%",
                    stats.network_health_score * 100.0,
                    stats.packet_loss_percentage
                ),
                business_impact: "Degraded performance and potential service instability".to_string(),
                recommended_solution: "Comprehensive network infrastructure review and optimization".to_string(),
                estimated_resolution_time: "4-24 hours for infrastructure improvements".to_string(),
            });
        }

        Ok(issues)
    }

    fn analyze_client_patterns(_stats: &MongoNetworkInfo) -> ResultEP<Vec<MongoClientConnectionPattern>> {
        Ok(Vec::new())
    }

    fn generate_config_recommendations(stats: &MongoNetworkInfo) -> ResultEP<Vec<MongoNetworkConfigRecommendation>> {
        let mut recommendations = Vec::new();

        // Connection pool size recommendation
        if stats.connection_utilization_percentage > Self::HIGH_CONNECTION_UTILIZATION {
            let current_max = stats.active_connections + stats.available_connections;
            let recommended_max = (current_max as f64 * 1.5) as u32;

            recommendations.push(MongoNetworkConfigRecommendation {
                configuration_area: "Connection Pool Size".to_string(),
                current_setting: format!("{} max connections", current_max),
                recommended_setting: format!("{} max connections", recommended_max),
                rationale: format!("Current utilization at {:.1}% indicates pool pressure", stats.connection_utilization_percentage),
                expected_impact: "Reduced connection timeouts and improved concurrency".to_string(),
                implementation_risk: "Low - requires application restart".to_string(),
                testing_requirements: vec![
                    "Load test with increased pool size".to_string(),
                    "Monitor memory usage impact".to_string(),
                    "Verify no connection leaks".to_string(),
                ],
                monitoring_after_change: vec![
                    "Connection pool utilization < 80%".to_string(),
                    "Connection timeout rate reduction".to_string(),
                    "Memory usage within acceptable limits".to_string(),
                ],
                rollback_procedure: "Revert maxPoolSize setting and restart application".to_string(),
            });
        }

        // Network compression recommendation
        if stats.compression_ratio < 0.3 && stats.total_bytes_sent > 50 * 1024 * 1024 {
            recommendations.push(MongoNetworkConfigRecommendation {
                configuration_area: "Network Compression".to_string(),
                current_setting: "Compression disabled or ineffective".to_string(),
                recommended_setting: "Enable snappy or zstd compression".to_string(),
                rationale: format!("Low compression ratio ({:.1}%) with high data transfer volume", stats.compression_ratio * 100.0),
                expected_impact: "30-50% reduction in network bandwidth usage".to_string(),
                implementation_risk: "Low - backward compatible".to_string(),
                testing_requirements: vec![
                    "Verify client driver support".to_string(),
                    "Test compression performance".to_string(),
                    "Measure CPU impact".to_string(),
                ],
                monitoring_after_change: vec![
                    "Network throughput reduction".to_string(),
                    "Compression ratio improvement".to_string(),
                    "CPU usage within limits".to_string(),
                ],
                rollback_procedure: "Disable compression in connection string and MongoDB config".to_string(),
            });
        }

        // TCP keepalive recommendation
        if stats.connection_timeouts > 10 {
            recommendations.push(MongoNetworkConfigRecommendation {
                configuration_area: "TCP Keepalive Settings".to_string(),
                current_setting: "Default TCP keepalive (may be too long)".to_string(),
                recommended_setting: "TCP keepalive: 120s idle, 30s interval, 3 probes".to_string(),
                rationale: format!("High connection timeout count ({}) suggests network connectivity issues", stats.connection_timeouts),
                expected_impact: "Faster detection of failed connections and improved reliability".to_string(),
                implementation_risk: "Low - OS-level configuration".to_string(),
                testing_requirements: vec![
                    "Test in staging environment".to_string(),
                    "Verify network stability".to_string(),
                    "Monitor connection lifecycle".to_string(),
                ],
                monitoring_after_change: vec![
                    "Connection timeout rate reduction".to_string(),
                    "Faster dead connection detection".to_string(),
                    "No increase in connection churn".to_string(),
                ],
                rollback_procedure: "Revert OS TCP keepalive settings to defaults".to_string(),
            });
        }

        // Read preference optimization
        if stats.replica_set_network_calls > 100 {
            recommendations.push(MongoNetworkConfigRecommendation {
                configuration_area: "Read Preference Strategy".to_string(),
                current_setting: "Primary read preference".to_string(),
                recommended_setting: "Secondary preferred with max staleness".to_string(),
                rationale: format!(
                    "High replica set network activity ({} calls) suggests read load on primary",
                    stats.replica_set_network_calls
                ),
                expected_impact: "Distributed read load and reduced primary server pressure".to_string(),
                implementation_risk: "Medium - potential for stale reads".to_string(),
                testing_requirements: vec![
                    "Verify application tolerance for eventual consistency".to_string(),
                    "Test with various staleness thresholds".to_string(),
                    "Load test read distribution".to_string(),
                ],
                monitoring_after_change: vec![
                    "Read distribution across secondaries".to_string(),
                    "Primary server load reduction".to_string(),
                    "Application consistency requirements met".to_string(),
                ],
                rollback_procedure: "Revert read preference to primary in connection strings".to_string(),
            });
        }

        // SSL/TLS optimization
        if stats.ssl_overhead_ms > 30.0 {
            recommendations.push(MongoNetworkConfigRecommendation {
                configuration_area: "SSL/TLS Configuration".to_string(),
                current_setting: "Standard SSL/TLS with potential inefficiencies".to_string(),
                recommended_setting: "Optimized cipher suites and session reuse".to_string(),
                rationale: format!("High SSL overhead ({:.1}ms) impacting performance", stats.ssl_overhead_ms),
                expected_impact: "20-40% reduction in SSL handshake time".to_string(),
                implementation_risk: "Medium - security configuration changes".to_string(),
                testing_requirements: vec![
                    "Security audit of cipher suite changes".to_string(),
                    "Performance testing of SSL optimizations".to_string(),
                    "Compatibility testing with all clients".to_string(),
                ],
                monitoring_after_change: vec![
                    "SSL handshake time reduction".to_string(),
                    "Maintained security compliance".to_string(),
                    "No client compatibility issues".to_string(),
                ],
                rollback_procedure: "Revert SSL configuration to previous cipher suites and settings".to_string(),
            });
        }

        Ok(recommendations)
    }
}
