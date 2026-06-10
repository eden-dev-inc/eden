use super::{ClickhouseDatabaseInfo, ClickhouseFragmentedTable, ClickhouseTableInfo, ClickhouseTableModification};
use crate::metadata::stc::utils::{RowExt, parse_rows};
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_database_info(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDatabaseInfo>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseDatabaseInfo {
            database: row.required_string("database")?,
            table_count: row.required_u64("table_count")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            total_parts: row.required_u64("total_parts")?,
            avg_compression_ratio: row.required_f64("avg_compression_ratio")?,
        })
    })
}

pub(super) fn parse_table_info(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseTableInfo>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseTableInfo {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            part_count: row.required_u64("part_count")?,
            last_modified: row.required_datetime("last_modified")?,
            engine: row.required_string("engine")?,
            uncompressed_size: row.required_u64("uncompressed_size")?,
            compressed_size: row.required_u64("compressed_size")?,
        })
    })
}

pub(super) fn parse_fragmented_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFragmentedTable>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseFragmentedTable {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_count: row.required_u64("part_count")?,
            total_size: row.required_u64("total_size")?,
            partition_count: row.required_u64("partition_count")?,
            last_modified: row.required_datetime("last_modified")?,
        })
    })
}

pub(super) fn parse_table_modifications(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseTableModification>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseTableModification {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            last_modified: row.required_datetime("last_modified")?,
            current_size: row.required_u64("current_size")?,
            recent_parts: row.required_u64("recent_parts")?,
        })
    })
}
