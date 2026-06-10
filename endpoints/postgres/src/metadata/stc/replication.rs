use crate::api::lib::query::QueryInput;
use crate::metadata::stc::utils::{run_query_with_timeout, run_single_row};
use borsh::{BorshDeserialize, BorshSerialize};
use endpoint_types::metadata::{CapabilityChecker, MetadataCollection, SyncFrequency};
use error::{EpError, ResultEP};
use format::timestamp::DateTimeWrapper;
use postgres_core::PgSimpleRow;
use postgres_core::PostgresAsync;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use telemetry::TelemetryWrapper;

/// PostgreSQL replication information and statistics
///
/// Simplified struct containing essential metrics about PostgreSQL replication,
/// including primary/replica status, lag information, and replication health.
#[derive(Debug, Clone, Default, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresReplicationInfo {
    /// Whether this instance is acting as a primary (master)
    pub is_primary: bool,
    /// Whether this instance is acting as a replica (standby)
    pub is_replica: bool,
    /// Whether this instance is in recovery mode
    pub is_in_recovery: bool,
    /// Number of active replica connections
    pub active_replicas: u64,
    /// Current WAL LSN position on primary
    pub current_wal_lsn: Option<String>,
    /// Maximum replication lag across all replicas (seconds)
    pub max_replica_lag_seconds: f64,
    /// Average replication lag across all replicas (seconds)
    pub avg_replica_lag_seconds: f64,
    /// Number of synchronous replicas
    pub synchronous_replicas: u64,
    /// Number of replication slots
    pub total_replication_slots: u64,
    /// Number of active replication slots
    pub active_replication_slots: u64,
    /// Whether WAL replay is paused (replica only)
    pub is_wal_replay_paused: bool,
    /// Overall replication health score (0.0 to 100.0)
    pub health_score: f64,
    /// Detailed metrics collected only when issues are detected
    pub detailed_metrics: Option<PostgresReplicationDetailedMetrics>,
}

/// Detailed replication metrics collected only when issues are detected
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresReplicationDetailedMetrics {
    /// Individual replica lag details (collected when max_replica_lag > 10s)
    pub replica_lag_details: Option<Vec<PostgresReplicaLag>>,
    /// Replica connection details (collected when active_replicas > 0 and issues detected)
    pub replica_connections: Option<Vec<PostgresReplicaConnection>>,
    /// WAL receiver info (collected on replica when issues detected)
    pub wal_receiver_info: Option<PostgresWalReceiverInfo>,
    /// Replication slot details (collected when inactive slots detected)
    pub replication_slots: Option<Vec<PostgresReplicationSlot>>,
    /// Replication recommendations
    pub recommendations: Vec<String>,
}

impl MetadataCollection for PostgresReplicationInfo {
    type Request = HashMap<String, QueryInput>;

    fn request(&self) -> Self::Request {
        HashMap::from([
            (
                "replication_status".to_string(),
                QueryInput::new(
                    "SELECT
                    pg_is_in_recovery() as is_in_recovery,
                    CASE WHEN pg_is_in_recovery() THEN false ELSE true END as is_primary,
                    pg_is_in_recovery() as is_replica,
                    CASE WHEN NOT pg_is_in_recovery() THEN pg_current_wal_lsn()::text END as current_wal_lsn,
                    CASE WHEN NOT pg_is_in_recovery() THEN
                        (SELECT count(*) FROM pg_stat_replication)
                    ELSE 0 END as active_replicas"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "replication_lag_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COALESCE(COUNT(*), 0) as total_replicas,
                    COALESCE(COUNT(*) FILTER (WHERE sync_state = 'sync'), 0) as sync_replicas,
                    COALESCE(MAX(EXTRACT(EPOCH FROM COALESCE(replay_lag, '0 seconds'::interval))), 0)::double precision as max_lag_seconds,
                    COALESCE(AVG(EXTRACT(EPOCH FROM COALESCE(replay_lag, '0 seconds'::interval))), 0)::double precision as avg_lag_seconds
                FROM pg_stat_replication"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "replication_slots_summary".to_string(),
                QueryInput::new(
                    "SELECT
                    COUNT(*) as total_slots,
                    COUNT(*) FILTER (WHERE active = true) as active_slots
                FROM pg_replication_slots"
                        .to_string(),
                    Vec::new(),
                ),
            ),
            (
                "standby_status".to_string(),
                QueryInput::new(
                    "SELECT
                    CASE WHEN pg_is_in_recovery() THEN pg_is_wal_replay_paused() ELSE false END as is_wal_replay_paused"
                        .to_string(),
                    Vec::new(),
                ),
            ),
        ])
    }

    fn description(&self) -> &'static str {
        "Return PostgreSQL replication information with minimal overhead"
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn category(&self) -> &'static str {
        "replication"
    }

    fn interval(&self) -> SyncFrequency {
        SyncFrequency::High
    }
}

use function_name::named;
use std::time::Duration;

impl PostgresReplicationInfo {
    const QUERY_TIMEOUT: Duration = Duration::from_secs(5);
    const LAG_THRESHOLD_SECONDS: f64 = 10.0;
    const CRITICAL_LAG_THRESHOLD: f64 = 60.0;

    #[named]
    pub(crate) async fn sync_metadata(
        &self,
        context: PostgresAsync,
        telemetry_wrapper: &mut TelemetryWrapper,
        _capabilities: &dyn CapabilityChecker,
    ) -> ResultEP<Self> {
        let _span = telemetry_wrapper.client_tracer(function_name!().to_string());

        let mut replication_info = PostgresReplicationInfo::default();
        let requests = self.request();

        // Execute basic replication status
        if let Some(row) = run_single_row(&requests, "replication_status", context.clone(), Self::QUERY_TIMEOUT).await? {
            replication_info.is_in_recovery = Self::safe_get_bool(&row, "is_in_recovery")?;
            replication_info.is_primary = Self::safe_get_bool(&row, "is_primary")?;
            replication_info.is_replica = Self::safe_get_bool(&row, "is_replica")?;
            replication_info.current_wal_lsn = Self::safe_get_optional_string(&row, "current_wal_lsn")?;
            replication_info.active_replicas = Self::safe_i64_to_u64(&row, "active_replicas")?;
        }

        // Parse lag summary (only meaningful on primary)
        if let Some(row) = run_single_row(&requests, "replication_lag_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            replication_info.synchronous_replicas = Self::safe_i64_to_u64(&row, "sync_replicas")?;
            replication_info.max_replica_lag_seconds = Self::safe_get_f64(&row, "max_lag_seconds")?;
            replication_info.avg_replica_lag_seconds = Self::safe_get_f64(&row, "avg_lag_seconds")?;
        }

        // Parse slots summary
        if let Some(row) = run_single_row(&requests, "replication_slots_summary", context.clone(), Self::QUERY_TIMEOUT).await? {
            replication_info.total_replication_slots = Self::safe_i64_to_u64(&row, "total_slots")?;
            replication_info.active_replication_slots = Self::safe_i64_to_u64(&row, "active_slots")?;
        }

        // Parse standby status
        if let Some(row) = run_single_row(&requests, "standby_status", context.clone(), Self::QUERY_TIMEOUT).await? {
            replication_info.is_wal_replay_paused = Self::safe_get_bool(&row, "is_wal_replay_paused")?;
        }

        // Calculate health score
        replication_info.health_score = replication_info.calculate_health_score();

        // Conditionally collect detailed metrics only when issues are detected
        replication_info.detailed_metrics = Self::collect_detailed_metrics_if_needed(&replication_info, context).await?;

        Ok(replication_info)
    }

    async fn collect_detailed_metrics_if_needed(
        core_info: &PostgresReplicationInfo,
        context: PostgresAsync,
    ) -> ResultEP<Option<PostgresReplicationDetailedMetrics>> {
        let needs_lag_details = core_info.max_replica_lag_seconds > Self::LAG_THRESHOLD_SECONDS;
        let needs_connection_details = core_info.active_replicas > 0 && core_info.health_score < 80.0;
        let needs_wal_receiver_details = core_info.is_replica && core_info.health_score < 80.0;
        let needs_slot_details = core_info.total_replication_slots > core_info.active_replication_slots;

        if !needs_lag_details && !needs_connection_details && !needs_wal_receiver_details && !needs_slot_details {
            return Ok(None);
        }

        let mut detailed_metrics = PostgresReplicationDetailedMetrics {
            replica_lag_details: None,
            replica_connections: None,
            wal_receiver_info: None,
            replication_slots: None,
            recommendations: core_info.generate_recommendations(),
        };

        // Collect detailed lag information
        if needs_lag_details {
            let lag_details_input = QueryInput::new(
                "SELECT
                    application_name,
                    client_addr::text,
                    EXTRACT(EPOCH FROM COALESCE(write_lag, '0 seconds'::interval)) as write_lag_seconds,
                    EXTRACT(EPOCH FROM COALESCE(flush_lag, '0 seconds'::interval)) as flush_lag_seconds,
                    EXTRACT(EPOCH FROM COALESCE(replay_lag, '0 seconds'::interval)) as replay_lag_seconds,
                    sync_state,
                    CASE WHEN sync_state = 'sync' THEN true ELSE false END as is_synchronous
                FROM pg_stat_replication
                ORDER BY replay_lag DESC NULLS LAST
                LIMIT 10"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&lag_details_input, context.clone(), Self::QUERY_TIMEOUT, "replica_lag_details").await
            {
                detailed_metrics.replica_lag_details = Some(Self::parse_replica_lag_details(rows)?);
            }
        }

        // Collect replica connection details
        if needs_connection_details {
            let connections_input = QueryInput::new(
                "SELECT
                    pid, usename, application_name, client_addr::text, client_hostname, client_port,
                    backend_start, state, sync_state, sync_priority,
                    sent_lsn::text, write_lsn::text, flush_lsn::text, replay_lsn::text,
                    reply_time
                FROM pg_stat_replication
                ORDER BY backend_start
                LIMIT 10"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&connections_input, context.clone(), Self::QUERY_TIMEOUT, "replica_connections").await
            {
                detailed_metrics.replica_connections = Some(Self::parse_replica_connections(rows)?);
            }
        }

        // Collect WAL receiver details (for replicas)
        if needs_wal_receiver_details {
            let wal_receiver_input = QueryInput::new(
                "SELECT
                    pid, status, sender_host, sender_port,
                    receive_start_lsn::text, written_lsn::text, flushed_lsn::text,
                    last_msg_send_time, last_msg_receipt_time,
                    slot_name, conninfo
                FROM pg_stat_wal_receiver"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&wal_receiver_input, context.clone(), Self::QUERY_TIMEOUT, "wal_receiver_info").await
                && let Some(row) = rows.first()
            {
                detailed_metrics.wal_receiver_info = Some(Self::parse_wal_receiver_info(row)?);
            }
        }

        // Collect replication slot details
        if needs_slot_details {
            let slots_input = QueryInput::new(
                "SELECT
                    slot_name, slot_type, database, active,
                    active_pid, plugin, temporary,
                    restart_lsn::text, confirmed_flush_lsn::text,
                    wal_status, safe_wal_size
                FROM pg_replication_slots
                ORDER BY active DESC, slot_name
                LIMIT 20"
                    .to_string(),
                Vec::new(),
            );

            if let Ok(rows) = run_query_with_timeout(&slots_input, context.clone(), Self::QUERY_TIMEOUT, "replication_slots").await {
                detailed_metrics.replication_slots = Some(Self::parse_replication_slots(rows)?);
            }
        }

        Ok(Some(detailed_metrics))
    }

    // Helper functions for safe type conversion and extraction
    fn safe_i64_to_u64(row: &PgSimpleRow, column: &str) -> ResultEP<u64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        let value = text.parse::<i64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))?;

        if value < 0 {
            return Err(EpError::metadata(format!("Negative value for {}: {}", column, value)));
        }
        Ok(value as u64)
    }

    fn safe_get_f64(row: &PgSimpleRow, column: &str) -> ResultEP<f64> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<f64>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_string(row: &PgSimpleRow, column: &str) -> ResultEP<String> {
        row.get(column)
            .map(|s| s.to_string())
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_bool(row: &PgSimpleRow, column: &str) -> ResultEP<bool> {
        row.get(column)
            .map(|s| s == "t" || s == "true" || s == "1")
            .ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))
    }

    fn safe_get_optional_string(row: &PgSimpleRow, column: &str) -> ResultEP<Option<String>> {
        Ok(row.get(column).map(|s| s.to_string()))
    }

    fn safe_get_i32(row: &PgSimpleRow, column: &str) -> ResultEP<i32> {
        let text = row.get(column).ok_or_else(|| EpError::metadata(format!("Failed to get column {column}: column not found or NULL")))?;
        text.parse::<i32>().map_err(|e| EpError::metadata(format!("Failed to get column {column}: {e}")))
    }

    fn safe_get_optional_i32(row: &PgSimpleRow, column: &str) -> ResultEP<Option<i32>> {
        Ok(row.get(column).and_then(|s| s.parse::<i32>().ok()))
    }

    fn safe_get_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<DateTimeWrapper> {
        let text = row
            .get(column)
            .ok_or_else(|| EpError::metadata(format!("Failed to get datetime column {column}: column not found or NULL")))?;
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z") {
            return Ok(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z") {
            return Ok(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc)));
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
            return Ok(DateTimeWrapper::from(ndt.and_utc()));
        }
        if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
            return Ok(DateTimeWrapper::from(ndt.and_utc()));
        }
        Err(EpError::metadata(format!("Failed to parse datetime column {column}: {text}")))
    }

    fn safe_get_optional_datetime(row: &PgSimpleRow, column: &str) -> ResultEP<Option<DateTimeWrapper>> {
        match row.get(column) {
            Some(text) => {
                if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f%#z") {
                    return Ok(Some(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc))));
                }
                if let Ok(dt) = chrono::DateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%#z") {
                    return Ok(Some(DateTimeWrapper::from(dt.with_timezone(&chrono::Utc))));
                }
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S%.f") {
                    return Ok(Some(DateTimeWrapper::from(ndt.and_utc())));
                }
                if let Ok(ndt) = chrono::NaiveDateTime::parse_from_str(text, "%Y-%m-%d %H:%M:%S") {
                    return Ok(Some(DateTimeWrapper::from(ndt.and_utc())));
                }
                Err(EpError::metadata(format!("Failed to parse datetime column {column}: {text}")))
            }
            None => Ok(None),
        }
    }

    fn parse_replica_lag_details(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresReplicaLag>> {
        let mut lag_details = Vec::with_capacity(rows.len());

        for row in rows {
            lag_details.push(PostgresReplicaLag {
                application_name: Self::safe_get_string(&row, "application_name")?,
                client_addr: Self::safe_get_optional_string(&row, "client_addr")?,
                write_lag_seconds: Some(Self::safe_get_f64(&row, "write_lag_seconds")?),
                flush_lag_seconds: Some(Self::safe_get_f64(&row, "flush_lag_seconds")?),
                replay_lag_seconds: Some(Self::safe_get_f64(&row, "replay_lag_seconds")?),
                is_synchronous: Self::safe_get_bool(&row, "is_synchronous")?,
                sync_state: Self::safe_get_string(&row, "sync_state")?,
            });
        }

        Ok(lag_details)
    }

    fn parse_replica_connections(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresReplicaConnection>> {
        let mut connections = Vec::with_capacity(rows.len());

        for row in rows {
            connections.push(PostgresReplicaConnection {
                pid: Self::safe_get_i32(&row, "pid")?,
                username: Self::safe_get_string(&row, "usename")?,
                application_name: Self::safe_get_string(&row, "application_name")?,
                client_addr: Self::safe_get_optional_string(&row, "client_addr")?,
                client_hostname: Self::safe_get_optional_string(&row, "client_hostname")?,
                client_port: Self::safe_get_optional_i32(&row, "client_port")?,
                backend_start: Self::safe_get_datetime(&row, "backend_start")?,
                state: Self::safe_get_string(&row, "state")?,
                sync_state: Self::safe_get_string(&row, "sync_state")?,
                sync_priority: Self::safe_get_i32(&row, "sync_priority")?,
                sent_lsn: Self::safe_get_optional_string(&row, "sent_lsn")?,
                write_lsn: Self::safe_get_optional_string(&row, "write_lsn")?,
                flush_lsn: Self::safe_get_optional_string(&row, "flush_lsn")?,
                replay_lsn: Self::safe_get_optional_string(&row, "replay_lsn")?,
                reply_time: Self::safe_get_optional_datetime(&row, "reply_time")?,
            });
        }

        Ok(connections)
    }

    fn parse_wal_receiver_info(row: &PgSimpleRow) -> ResultEP<PostgresWalReceiverInfo> {
        Ok(PostgresWalReceiverInfo {
            pid: Self::safe_get_i32(row, "pid")?,
            status: Self::safe_get_string(row, "status")?,
            sender_host: Self::safe_get_string(row, "sender_host")?,
            sender_port: Self::safe_get_i32(row, "sender_port")?,
            receive_start_lsn: Self::safe_get_optional_string(row, "receive_start_lsn")?,
            written_lsn: Self::safe_get_optional_string(row, "written_lsn")?,
            flushed_lsn: Self::safe_get_optional_string(row, "flushed_lsn")?,
            last_msg_send_time: Self::safe_get_optional_datetime(row, "last_msg_send_time")?,
            last_msg_receipt_time: Self::safe_get_optional_datetime(row, "last_msg_receipt_time")?,
            slot_name: Self::safe_get_optional_string(row, "slot_name")?,
            conninfo: Self::safe_get_string(row, "conninfo")?,
        })
    }

    fn parse_replication_slots(rows: Vec<PgSimpleRow>) -> ResultEP<Vec<PostgresReplicationSlot>> {
        let mut slots = Vec::with_capacity(rows.len());

        for row in rows {
            slots.push(PostgresReplicationSlot {
                slot_name: Self::safe_get_string(&row, "slot_name")?,
                slot_type: Self::safe_get_string(&row, "slot_type")?,
                database: Self::safe_get_optional_string(&row, "database")?,
                active: Self::safe_get_bool(&row, "active")?,
                active_pid: Self::safe_get_optional_i32(&row, "active_pid")?,
                plugin: Self::safe_get_optional_string(&row, "plugin")?,
                temporary: Self::safe_get_bool(&row, "temporary")?,
                restart_lsn: Self::safe_get_optional_string(&row, "restart_lsn")?,
                confirmed_flush_lsn: Self::safe_get_optional_string(&row, "confirmed_flush_lsn")?,
                wal_status: Self::safe_get_optional_string(&row, "wal_status")?,
            });
        }

        Ok(slots)
    }
}

impl PostgresReplicationInfo {
    /// Calculates overall replication health score
    fn calculate_health_score(&self) -> f64 {
        let mut score = 100.0;

        // Deduct for high replication lag
        if self.max_replica_lag_seconds > Self::CRITICAL_LAG_THRESHOLD {
            score -= 40.0;
        } else if self.max_replica_lag_seconds > Self::LAG_THRESHOLD_SECONDS {
            score -= (self.max_replica_lag_seconds - Self::LAG_THRESHOLD_SECONDS) * 2.0;
        }

        // Deduct for paused WAL replay on replica
        if self.is_wal_replay_paused {
            score -= 30.0;
        }

        // Deduct for inactive replication slots
        if self.total_replication_slots > 0 {
            let inactive_slots = self.total_replication_slots - self.active_replication_slots;
            if inactive_slots > 0 {
                score -= (inactive_slots as f64 * 5.0).min(20.0);
            }
        }

        // Deduct for no replicas on primary (if this might be expected)
        if self.is_primary && self.active_replicas == 0 {
            score -= 15.0;
        }

        // Deduct for no synchronous replicas on primary with replicas
        if self.is_primary && self.active_replicas > 0 && self.synchronous_replicas == 0 {
            score -= 10.0;
        }

        score.max(0.0)
    }

    /// Generates replication recommendations
    fn generate_recommendations(&self) -> Vec<String> {
        let mut recommendations = Vec::new();

        if self.max_replica_lag_seconds > Self::CRITICAL_LAG_THRESHOLD {
            recommendations.push(format!(
                "CRITICAL: High replication lag detected ({:.1}s) - investigate network, disk I/O, or query load",
                self.max_replica_lag_seconds
            ));
        } else if self.max_replica_lag_seconds > Self::LAG_THRESHOLD_SECONDS {
            recommendations.push(format!(
                "High replication lag detected ({:.1}s) - monitor and investigate if persistent",
                self.max_replica_lag_seconds
            ));
        }

        if self.is_wal_replay_paused {
            recommendations.push("WAL replay is paused on this replica - investigate and resume if appropriate".to_string());
        }

        let inactive_slots = self.total_replication_slots - self.active_replication_slots;
        if inactive_slots > 0 {
            recommendations.push(format!(
                "{} inactive replication slot(s) detected - consider dropping unused slots to prevent WAL retention",
                inactive_slots
            ));
        }

        if self.is_primary && self.active_replicas == 0 {
            recommendations
                .push("No active replicas detected on primary - consider setting up replication for high availability".to_string());
        }

        if self.is_primary && self.active_replicas > 0 && self.synchronous_replicas == 0 {
            recommendations.push("No synchronous replicas configured - consider synchronous replication for data safety".to_string());
        }

        if self.is_primary && self.active_replicas > 3 {
            recommendations.push("Many replicas detected - ensure primary can handle the replication load".to_string());
        }

        if recommendations.is_empty() {
            recommendations.push("Replication appears healthy".to_string());
        }

        recommendations
    }

    /// Checks if replication is properly configured
    pub fn is_replication_healthy(&self) -> bool {
        self.health_score >= 80.0
    }

    /// Checks if replication lag exceeds threshold
    pub fn has_excessive_replication_lag(&self) -> bool {
        self.max_replica_lag_seconds > Self::LAG_THRESHOLD_SECONDS
    }

    /// Gets the number of synchronous replicas
    pub fn get_synchronous_replica_count(&self) -> u64 {
        self.synchronous_replicas
    }

    /// Checks if there are stale replication slots
    pub fn has_stale_replication_slots(&self) -> bool {
        self.total_replication_slots > self.active_replication_slots
    }

    /// Returns true if detailed metrics were collected
    pub fn has_detailed_metrics(&self) -> bool {
        self.detailed_metrics.is_some()
    }

    /// Gets replication health summary
    pub fn get_replication_health_summary(&self) -> String {
        match self.health_score as u8 {
            90..=100 => "Excellent - Replication is healthy".to_string(),
            75..=89 => "Good - Replication is performing well".to_string(),
            60..=74 => "Fair - Some replication issues detected".to_string(),
            40..=59 => "Poor - Significant replication problems".to_string(),
            _ => "Critical - Replication requires immediate attention".to_string(),
        }
    }

    /// Gets replication role description
    pub fn get_replication_role(&self) -> String {
        if self.is_primary {
            format!("Primary with {} active replica(s)", self.active_replicas)
        } else if self.is_replica {
            "Replica (Standby)".to_string()
        } else {
            "Standalone (No replication)".to_string()
        }
    }
}

/// Replication lag information for a specific replica
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresReplicaLag {
    /// Application name of the replica
    pub application_name: String,
    /// Client address of the replica
    pub client_addr: Option<String>,
    /// Write lag in seconds
    pub write_lag_seconds: Option<f64>,
    /// Flush lag in seconds
    pub flush_lag_seconds: Option<f64>,
    /// Replay lag in seconds
    pub replay_lag_seconds: Option<f64>,
    /// Whether this replica is synchronous
    pub is_synchronous: bool,
    /// Sync state of the replica
    pub sync_state: String,
}

/// Simplified replica connection information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresReplicaConnection {
    /// Process ID of the WAL sender process
    pub pid: i32,
    /// Username used for replication connection
    pub username: String,
    /// Application name of the replica
    pub application_name: String,
    /// Client IP address
    pub client_addr: Option<String>,
    /// Client hostname
    pub client_hostname: Option<String>,
    /// Client port
    pub client_port: Option<i32>,
    /// When the connection was established
    pub backend_start: DateTimeWrapper,
    /// Current state of the connection
    pub state: String,
    /// Last WAL location sent to replica
    pub sent_lsn: Option<String>,
    /// Last WAL location written by replica
    pub write_lsn: Option<String>,
    /// Last WAL location flushed by replica
    pub flush_lsn: Option<String>,
    /// Last WAL location replayed by replica
    pub replay_lsn: Option<String>,
    /// Synchronous replication priority
    pub sync_priority: i32,
    /// Synchronous state
    pub sync_state: String,
    /// Last reply time from replica
    pub reply_time: Option<DateTimeWrapper>,
}

/// Simplified WAL receiver information (for replicas)
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresWalReceiverInfo {
    /// Process ID of the WAL receiver
    pub pid: i32,
    /// Status of the WAL receiver
    pub status: String,
    /// Primary server hostname
    pub sender_host: String,
    /// Primary server port
    pub sender_port: i32,
    /// LSN at which WAL receiving started
    pub receive_start_lsn: Option<String>,
    /// Last WAL location written to disk
    pub written_lsn: Option<String>,
    /// Last WAL location flushed to disk
    pub flushed_lsn: Option<String>,
    /// Time of last message sent to primary
    pub last_msg_send_time: Option<DateTimeWrapper>,
    /// Time of last message received from primary
    pub last_msg_receipt_time: Option<DateTimeWrapper>,
    /// Replication slot name being used
    pub slot_name: Option<String>,
    /// Connection string used to connect to primary
    pub conninfo: String,
}

/// Simplified replication slot information
#[derive(Debug, Clone, Serialize, Deserialize, BorshSerialize, BorshDeserialize)]
pub struct PostgresReplicationSlot {
    /// Name of the replication slot
    pub slot_name: String,
    /// Type of slot (physical or logical)
    pub slot_type: String,
    /// Database name (for logical slots)
    pub database: Option<String>,
    /// Whether the slot is currently active
    pub active: bool,
    /// PID of the process using this slot
    pub active_pid: Option<i32>,
    /// Plugin name (for logical slots)
    pub plugin: Option<String>,
    /// Whether this is a temporary slot
    pub temporary: bool,
    /// LSN at which slot guarantees WAL availability
    pub restart_lsn: Option<String>,
    /// LSN up to which consumer has confirmed receipt
    pub confirmed_flush_lsn: Option<String>,
    /// WAL status for this slot
    pub wal_status: Option<String>,
}

#[cfg(all(test, external_db))]
mod tests {
    use super::*;
    use crate::test_utils::database_test_utils::connect_to_postgres;
    use endpoint_types::metadata::PermissiveCapabilities;
    use ep_core::GetPool;

    #[tokio::test]
    async fn test_postgres_replication_metadata() {
        let (_postgres, endpoint_cache_uuid, postgres_ep, mut telemetry_wrapper) = connect_to_postgres().await;
        let telemetry_wrapper = &mut telemetry_wrapper;

        let replication_info = PostgresReplicationInfo::default();

        let result = replication_info
            .sync_metadata(
                postgres_ep.pool().read_conn_async(&endpoint_cache_uuid).await.expect("failed to get connection").to_owned(),
                telemetry_wrapper,
                &PermissiveCapabilities,
            )
            .await;

        assert!(result.is_ok());
        let info = result.unwrap_or_default();

        // Verify core metrics are collected
        assert!(info.health_score >= 0.0);
        assert!(info.health_score <= 100.0);
    }
}
