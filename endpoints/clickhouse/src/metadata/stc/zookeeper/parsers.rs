use super::{
    ClickhouseDetachedReplica, ClickhouseFailedZooKeeperOperation, ClickhouseLaggingReplica, ClickhouseReplicationQueueInfo,
    ClickhouseZooKeeperSession,
};
use crate::metadata::stc::utils::{RowExt, parse_rows};
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_lagging_replicas(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLaggingReplica>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseLaggingReplica {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            replication_lag_entries: row.required_u64("replication_lag_entries")?,
            queue_size: row.required_u64("queue_size")?,
            is_readonly: row.required_u64("is_readonly")? == 1,
            is_session_expired: row.required_u64("is_session_expired")? == 1,
            last_queue_update: row.required_datetime("last_queue_update")?,
            absolute_delay: row.required_u64("absolute_delay")?,
            total_replicas: row.required_u64("total_replicas")?,
            active_replicas: row.required_u64("active_replicas")?,
        })
    })
}

// Retained for future use if system.zookeeper_log becomes available.
#[allow(dead_code)]
pub(super) fn parse_failed_operations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFailedZooKeeperOperation>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseFailedZooKeeperOperation {
            operation_type: row.required_string("type")?,
            path: row.required_string("path")?,
            error_code: row.required_u64("error")?,
            event_time: row.required_datetime("event_time")?,
            session_id: row.required_u64("session_id")?,
            request_idx: row.required_u64("request_idx")?,
            response_idx: row.required_u64("response_idx")?,
            duration_ms: row.required_f64("duration_ms")?,
        })
    })
}

pub(super) fn parse_detached_replicas(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDetachedReplica>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseDetachedReplica {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            is_session_expired: row.required_u64("is_session_expired")? == 1,
            is_readonly: row.required_u64("is_readonly")? == 1,
            queue_size: row.required_u64("queue_size")?,
            replication_lag: row.required_u64("replication_lag")?,
            last_queue_update: row.required_datetime("last_queue_update")?,
            zookeeper_path: row.required_string("zookeeper_path")?,
            replica_name: row.required_string("replica_name")?,
        })
    })
}

// Retained for future use if system.zookeeper_connection becomes available.
#[allow(dead_code)]
pub(super) fn parse_session_details(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseZooKeeperSession>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseZooKeeperSession {
            session_id: row.required_u64("session_id")?,
            host: row.required_string("host")?,
            port: row.required_u64("port")?,
            latency: row.required_f64("latency")?,
            is_expired: row.required_u64("is_expired")? == 1,
            session_uptime_seconds: row.required_u64("session_uptime_elapsed_seconds")?,
            queries: row.required_u64("queries")?,
            bytes_sent: row.required_u64("bytes_sent")?,
            bytes_received: row.required_u64("bytes_received")?,
        })
    })
}

pub(super) fn parse_replication_queue_analysis(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseReplicationQueueInfo>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseReplicationQueueInfo {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            queue_size: row.required_u64("queue_size")?,
            inserts_in_queue: row.required_u64("inserts_in_queue")?,
            merges_in_queue: row.required_u64("merges_in_queue")?,
            mutations_in_queue: row.required_u64("part_mutations_in_queue")?,
            total_replicas: row.required_u64("total_replicas")?,
            active_replicas: row.required_u64("active_replicas")?,
        })
    })
}
