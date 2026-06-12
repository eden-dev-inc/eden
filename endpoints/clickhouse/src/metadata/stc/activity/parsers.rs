use super::{ClickhouseActiveQuery, ClickhouseFailedQuery, ClickhouseMemoryQuery};
use crate::metadata::stc::utils::{RowExt, parse_rows};
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_long_running_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseActiveQuery>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseActiveQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.required_string("database")?,
            query: row.required_string("query")?,
            duration: row.required_f64("duration")?,
            memory_usage: row.required_u64("memory_usage")?,
            read_rows: row.required_u64("read_rows")?,
            read_bytes: row.required_u64("read_bytes")?,
            query_start_time: row.required_datetime("query_start_time")?,
            query_kind: row.required_string("query_kind")?,
            client_name: row.optional_string("client_name")?,
            client_hostname: row.optional_string("client_hostname")?,
            main_thread_id: row.required_u64("main_thread_id")?,
        })
    })
}

pub(super) fn parse_failed_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFailedQuery>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseFailedQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.required_string("database")?,
            query: row.required_string("query")?,
            exception: row.required_string("exception")?,
            duration: row.required_f64("duration")?,
            event_time: row.required_datetime("event_time")?,
            query_kind: row.required_string("query_kind")?,
            client_name: row.optional_string("client_name")?,
            client_hostname: row.optional_string("client_hostname")?,
            memory_usage: row.required_u64("memory_usage")?,
            read_rows: row.required_u64("read_rows")?,
            read_bytes: row.required_u64("read_bytes")?,
        })
    })
}

pub(super) fn parse_memory_queries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseMemoryQuery>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseMemoryQuery {
            query_id: row.required_string("query_id")?,
            user: row.required_string("user")?,
            database: row.required_string("database")?,
            query: row.required_string("query")?,
            memory_usage: row.required_u64("memory_usage")?,
            duration: row.required_f64("duration")?,
            read_rows: row.required_u64("read_rows")?,
            read_bytes: row.required_u64("read_bytes")?,
            query_start_time: row.required_datetime("query_start_time")?,
        })
    })
}
