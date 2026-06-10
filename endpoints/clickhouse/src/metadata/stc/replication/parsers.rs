use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_high_lag_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseHighLagTable>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        tables.push(ClickhouseHighLagTable {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            replica_name: row.required_string("replica_name")?,
            absolute_delay: row.required_f64("absolute_delay")?,
            log_max_index: row.required_u64("log_max_index")?,
            log_pointer: row.required_u64("log_pointer")?,
            queue_size: row.required_u64("queue_size")?,
            inserts_in_queue: row.required_u64("inserts_in_queue")?,
            merges_in_queue: row.required_u64("merges_in_queue")?,
            last_queue_update: row.optional_datetime("last_queue_update")?,
            is_session_expired: row.required_bool("is_session_expired")?,
            zookeeper_path: row.required_string("zookeeper_path")?,
            active_replicas: row.required_u64("active_replicas")?,
            total_replicas: row.required_u64("total_replicas")?,
        });
    }

    Ok(tables)
}

pub(super) fn parse_error_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseReplicationError>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        tables.push(ClickhouseReplicationError {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            replica_name: row.required_string("replica_name")?,
            last_exception: row.required_string("last_exception")?,
            last_exception_time: row.optional_datetime("last_exception_time")?,
            queue_size: row.required_u64("queue_size")?,
            absolute_delay: row.required_f64("absolute_delay")?,
            is_readonly: row.required_bool("is_readonly")?,
            is_session_expired: row.required_bool("is_session_expired")?,
            zookeeper_path: row.required_string("zookeeper_path")?,
            replica_path: row.required_string("replica_path")?,
            log_max_index: row.required_u64("log_max_index")?,
            log_pointer: row.required_u64("log_pointer")?,
        });
    }

    Ok(tables)
}

pub(super) fn parse_readonly_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseReadonlyTable>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        tables.push(ClickhouseReadonlyTable {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            replica_name: row.required_string("replica_name")?,
            last_exception: row.optional_string("last_exception")?,
            last_exception_time: row.optional_datetime("last_exception_time")?,
            absolute_delay: row.required_f64("absolute_delay")?,
            queue_size: row.required_u64("queue_size")?,
            is_session_expired: row.required_bool("is_session_expired")?,
            zookeeper_path: row.required_string("zookeeper_path")?,
            log_max_index: row.required_u64("log_max_index")?,
            log_pointer: row.required_u64("log_pointer")?,
            active_replicas: row.required_u64("active_replicas")?,
            total_replicas: row.required_u64("total_replicas")?,
        });
    }

    Ok(tables)
}

pub(super) fn parse_large_queue_entries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLargeQueueEntry>> {
    let mut entries = Vec::with_capacity(rows.len());

    for row in rows {
        entries.push(ClickhouseLargeQueueEntry {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            operation_type: row.required_string("type")?,
            create_time: row.required_datetime("create_time")?,
            required_quorum: row.required_u64("required_quorum")?,
            source_replica: row.optional_string("source_replica")?,
            new_part_name: row.optional_string("new_part_name")?,
            parts_to_merge: row.optional_string("parts_to_merge")?,
            is_currently_executing: row.required_bool("is_currently_executing")?,
            num_tries: row.required_u64("num_tries")?,
            last_attempt_time: row.optional_datetime("last_attempt_time")?,
            last_exception: row.optional_string("last_exception")?,
            postpone_reason: row.optional_string("postpone_reason")?,
        });
    }

    Ok(entries)
}

pub(super) fn parse_failed_operations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFailedReplication>> {
    let mut operations = Vec::with_capacity(rows.len());

    for row in rows {
        operations.push(ClickhouseFailedReplication {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            replica_name: row.required_string("replica_name")?,
            last_exception: row.required_string("last_exception")?,
            last_exception_time: row.required_datetime("last_exception_time")?,
            queue_size: row.required_u64("queue_size")?,
            absolute_delay: row.required_f64("absolute_delay")?,
            last_queue_update: row.optional_datetime("last_queue_update")?,
            zookeeper_path: row.required_string("zookeeper_path")?,
        });
    }

    Ok(operations)
}

pub(super) fn parse_replica_status(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseReplicaStatus>> {
    let mut replicas = Vec::with_capacity(rows.len());

    for row in rows {
        replicas.push(ClickhouseReplicaStatus {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            replica_name: row.required_string("replica_name")?,
            is_leader: row.required_bool("is_leader")?,
            is_readonly: row.required_bool("is_readonly")?,
            is_session_expired: row.required_bool("is_session_expired")?,
            absolute_delay: row.required_f64("absolute_delay")?,
            queue_size: row.required_u64("queue_size")?,
            active_replicas: row.required_u64("active_replicas")?,
            total_replicas: row.required_u64("total_replicas")?,
            zookeeper_path: row.required_string("zookeeper_path")?,
            replica_path: row.required_string("replica_path")?,
            log_max_index: row.required_u64("log_max_index")?,
            log_pointer: row.required_u64("log_pointer")?,
            last_queue_update: row.optional_datetime("last_queue_update")?,
        });
    }

    Ok(replicas)
}

pub(super) fn parse_zookeeper_status(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseZookeeperStatus>> {
    let mut status = Vec::with_capacity(rows.len());

    for row in rows {
        status.push(ClickhouseZookeeperStatus {
            zookeeper_path: row.required_string("zookeeper_path")?,
            replica_count: row.required_u64("replica_count")?,
            active_replicas: row.required_u64("active_replicas")?,
            readonly_replicas: row.required_u64("readonly_replicas")?,
            max_lag: row.required_f64("max_lag")?,
            total_queue_size: row.required_u64("total_queue_size")?,
            error_count: row.required_u64("error_count")?,
        });
    }

    Ok(status)
}

pub(super) fn parse_recovery_operations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseRecoveryOperation>> {
    let mut operations = Vec::with_capacity(rows.len());

    for row in rows {
        operations.push(ClickhouseRecoveryOperation {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            replica_name: row.required_string("replica_name")?,
            last_exception: row.required_string("last_exception")?,
            last_exception_time: row.required_datetime("last_exception_time")?,
            absolute_delay: row.required_f64("absolute_delay")?,
            queue_size: row.required_u64("queue_size")?,
            is_readonly: row.required_bool("is_readonly")?,
            is_session_expired: row.required_bool("is_session_expired")?,
            zookeeper_path: row.required_string("zookeeper_path")?,
            recovery_duration: row.required_f64("recovery_duration")?,
        });
    }

    Ok(operations)
}
