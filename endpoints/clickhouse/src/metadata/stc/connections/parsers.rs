use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_protocol_stats(rows: &[ClickhouseRow]) -> ResultEP<Vec<ClickhouseProtocolStats>> {
    let mut stats = Vec::new();

    for row in rows {
        stats.push(ClickhouseProtocolStats {
            protocol: row.string_or_empty("protocol")?,
            connection_count: row.u64_or_zero("connection_count")?,
            avg_duration: row.f64_or_zero("avg_duration")?,
            max_duration: row.f64_or_zero("max_duration")?,
            total_memory: row.u64_or_zero("total_memory")?,
            avg_memory: row.u64_or_zero("avg_memory")?,
        });
    }

    Ok(stats)
}

pub(super) fn parse_user_connections(rows: &[ClickhouseRow]) -> ResultEP<Vec<ClickhouseUserConnection>> {
    let mut connections = Vec::new();

    for row in rows {
        connections.push(ClickhouseUserConnection {
            user: row.string_or_empty("user")?,
            database: row.string_or_empty("database")?,
            protocol: row.string_or_empty("protocol")?,
            connection_count: row.u64_or_zero("connection_count")?,
            total_memory: row.u64_or_zero("total_memory")?,
            avg_duration: row.f64_or_zero("avg_duration")?,
            idle_connections: row.u64_or_zero("idle_connections")?,
        });
    }

    Ok(connections)
}

pub(super) fn parse_long_connections(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLongConnection>> {
    let mut connections = Vec::new();

    for row in rows {
        connections.push(ClickhouseLongConnection {
            user: row.string_or_empty("user")?,
            database: row.string_or_empty("database")?,
            protocol: row.string_or_empty("protocol")?,
            query_id: row.string_or_empty("query_id")?,
            query_text: row.string_or_empty("query_text")?,
            duration: row.f64_or_zero("duration")?,
            memory_usage: row.u64_or_zero("memory_usage")?,
            read_rows: row.u64_or_zero("read_rows")?,
            read_bytes: row.u64_or_zero("read_bytes")?,
            client_name: row.optional_string("client_name")?,
            client_hostname: row.optional_string("client_hostname")?,
            client_version: row.optional_string("client_version")?,
            start_time: row.required_datetime("query_start_time")?,
        });
    }

    Ok(connections)
}

pub(super) fn parse_high_memory_connections(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseHighMemoryConnection>> {
    let mut connections = Vec::new();

    for row in rows {
        connections.push(ClickhouseHighMemoryConnection {
            user: row.string_or_empty("user")?,
            database: row.string_or_empty("database")?,
            protocol: row.string_or_empty("protocol")?,
            query_id: row.string_or_empty("query_id")?,
            query_text: row.string_or_empty("query_text")?,
            memory_usage: row.u64_or_zero("memory_usage")?,
            duration: row.f64_or_zero("duration")?,
            read_rows: row.u64_or_zero("read_rows")?,
            read_bytes: row.u64_or_zero("read_bytes")?,
            client_name: row.optional_string("client_name")?,
        });
    }

    Ok(connections)
}

pub(super) fn parse_connection_failures(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseConnectionFailure>> {
    let mut failures = Vec::new();

    for row in rows {
        failures.push(ClickhouseConnectionFailure {
            user: row.string_or_empty("user")?,
            database: row.string_or_empty("database")?,
            client_name: row.optional_string("client_name")?,
            client_hostname: row.optional_string("client_hostname")?,
            exception: row.string_or_empty("exception")?,
            failure_time: row.required_datetime("event_time")?,
            duration: row.f64_or_zero("duration")?,
            query_text: row.string_or_empty("query_text")?,
        });
    }

    Ok(failures)
}

pub(super) fn parse_client_stats(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseClientStats>> {
    let mut stats = Vec::new();

    for row in rows {
        stats.push(ClickhouseClientStats {
            client_name: row.string_or_empty("client_name")?,
            client_version: row.optional_string("client_version")?,
            client_hostname: row.optional_string("client_hostname")?,
            connection_count: row.u64_or_zero("connection_count")?,
            total_memory: row.u64_or_zero("total_memory")?,
            avg_duration: row.f64_or_zero("avg_duration")?,
        });
    }

    Ok(stats)
}

pub(super) fn parse_idle_connections(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseIdleConnection>> {
    let mut connections = Vec::new();

    for row in rows {
        connections.push(ClickhouseIdleConnection {
            user: row.string_or_empty("user")?,
            database: row.string_or_empty("database")?,
            protocol: row.string_or_empty("protocol")?,
            query_id: row.string_or_empty("query_id")?,
            idle_duration: row.f64_or_zero("idle_duration")?,
            memory_usage: row.u64_or_zero("memory_usage")?,
            client_name: row.optional_string("client_name")?,
            client_hostname: row.optional_string("client_hostname")?,
        });
    }

    Ok(connections)
}
