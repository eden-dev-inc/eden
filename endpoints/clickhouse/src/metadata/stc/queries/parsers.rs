use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_slow_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseSlowQuery>> {
    let mut queries = Vec::with_capacity(rows.len());

    for row in rows {
        queries.push(ClickhouseSlowQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.optional_string("database")?,
            query: row.required_string("query")?,
            elapsed_seconds: row.required_f64("elapsed")?,
            memory_usage: row.required_u64("memory_usage")?,
            read_bytes: row.required_u64("read_bytes")?,
            read_rows: row.required_u64("read_rows")?,
            total_rows_approx: row.required_u64("total_rows_approx")?,
            client_name: row.optional_string("client_name")?,
            client_hostname: row.optional_string("client_hostname")?,
            http_user_agent: row.optional_string("http_user_agent")?,
        });
    }

    Ok(queries)
}

pub(super) fn parse_high_memory_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseHighMemoryQuery>> {
    let mut queries = Vec::with_capacity(rows.len());

    for row in rows {
        queries.push(ClickhouseHighMemoryQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.optional_string("database")?,
            query: row.required_string("query")?,
            elapsed_seconds: row.required_f64("elapsed")?,
            memory_usage: row.required_u64("memory_usage")?,
            peak_memory_usage: row.required_u64("peak_memory_usage")?,
            read_bytes: row.required_u64("read_bytes")?,
            read_rows: row.required_u64("read_rows")?,
            written_bytes: row.required_u64("written_bytes")?,
            written_rows: row.required_u64("written_rows")?,
            client_name: row.optional_string("client_name")?,
            http_user_agent: row.optional_string("http_user_agent")?,
        });
    }

    Ok(queries)
}

pub(super) fn parse_long_running_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLongRunningQuery>> {
    let mut queries = Vec::with_capacity(rows.len());

    for row in rows {
        queries.push(ClickhouseLongRunningQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.optional_string("database")?,
            query: row.required_string("query")?,
            elapsed_seconds: row.required_f64("elapsed")?,
            memory_usage: row.required_u64("memory_usage")?,
            read_bytes: row.required_u64("read_bytes")?,
            read_rows: row.required_u64("read_rows")?,
            client_name: row.optional_string("client_name")?,
            client_hostname: row.optional_string("client_hostname")?,
            http_user_agent: row.optional_string("http_user_agent")?,
        });
    }

    Ok(queries)
}

pub(super) fn parse_failed_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFailedQuery>> {
    let mut queries = Vec::with_capacity(rows.len());

    for row in rows {
        queries.push(ClickhouseFailedQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.optional_string("database")?,
            query: row.required_string("query")?,
            exception: row.required_string("exception")?,
            event_time: row.required_datetime("event_time")?,
            query_duration_ms: row.required_u64("query_duration_ms")?,
            memory_usage: row.required_u64("memory_usage")?,
            read_bytes: row.required_u64("read_bytes")?,
            read_rows: row.required_u64("read_rows")?,
            written_bytes: row.required_u64("written_bytes")?,
            written_rows: row.required_u64("written_rows")?,
            result_bytes: row.required_u64("result_bytes")?,
            result_rows: row.required_u64("result_rows")?,
            client_name: row.optional_string("client_name")?,
            http_user_agent: row.optional_string("http_user_agent")?,
        });
    }

    Ok(queries)
}

pub(super) fn parse_blocked_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseBlockedQuery>> {
    let mut queries = Vec::with_capacity(rows.len());

    for row in rows {
        queries.push(ClickhouseBlockedQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.optional_string("database")?,
            query: row.required_string("query")?,
            elapsed_seconds: row.required_f64("elapsed")?,
            memory_usage: row.required_u64("memory_usage")?,
            read_bytes: row.required_u64("read_bytes")?,
            read_rows: row.required_u64("read_rows")?,
            client_name: row.optional_string("client_name")?,
            http_user_agent: row.optional_string("http_user_agent")?,
        });
    }

    Ok(queries)
}

pub(super) fn parse_expensive_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseExpensiveQuery>> {
    let mut queries = Vec::with_capacity(rows.len());

    for row in rows {
        queries.push(ClickhouseExpensiveQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.optional_string("database")?,
            query: row.required_string("query")?,
            elapsed_seconds: row.required_f64("elapsed")?,
            memory_usage: row.required_u64("memory_usage")?,
            peak_memory_usage: row.required_u64("peak_memory_usage")?,
            read_bytes: row.required_u64("read_bytes")?,
            read_rows: row.required_u64("read_rows")?,
            written_bytes: row.required_u64("written_bytes")?,
            written_rows: row.required_u64("written_rows")?,
            cpu_time_microseconds: row.required_u64("cpu_time_microseconds")?,
            io_wait_microseconds: row.required_u64("io_wait_microseconds")?,
            client_name: row.optional_string("client_name")?,
            http_user_agent: row.optional_string("http_user_agent")?,
        });
    }

    Ok(queries)
}

pub(super) fn parse_database_stats(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDatabaseQueryStats>> {
    let mut stats = Vec::with_capacity(rows.len());

    for row in rows {
        stats.push(ClickhouseDatabaseQueryStats {
            database: row.required_string("database")?,
            query_count: row.required_u64("query_count")?,
            avg_duration_seconds: row.required_f64("avg_duration_seconds")?,
            max_duration_seconds: row.required_f64("max_duration_seconds")?,
            total_memory_usage: row.required_u64("total_memory_usage")?,
            avg_memory_usage: row.required_u64("avg_memory_usage")?,
            total_bytes_read: row.required_u64("total_bytes_read")?,
            total_rows_read: row.required_u64("total_rows_read")?,
            failed_queries: row.required_u64("failed_queries")?,
        });
    }

    Ok(stats)
}

pub(super) fn parse_user_stats(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseUserQueryStats>> {
    let mut stats = Vec::with_capacity(rows.len());

    for row in rows {
        stats.push(ClickhouseUserQueryStats {
            user: row.required_string("user")?,
            query_count: row.required_u64("query_count")?,
            avg_duration_seconds: row.required_f64("avg_duration_seconds")?,
            max_duration_seconds: row.required_f64("max_duration_seconds")?,
            total_memory_usage: row.required_u64("total_memory_usage")?,
            avg_memory_usage: row.required_u64("avg_memory_usage")?,
            total_bytes_read: row.required_u64("total_bytes_read")?,
            total_rows_read: row.required_u64("total_rows_read")?,
            failed_queries: row.required_u64("failed_queries")?,
        });
    }

    Ok(stats)
}
