use super::{
    ClickhouseBrokenPartsTable, ClickhouseLargeTable, ClickhousePartitionInfo, ClickhouseProblematicTable, ClickhouseStorageByDatabase,
};
use crate::metadata::stc::utils::{RowExt, parse_rows};
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_problematic_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseProblematicTable>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseProblematicTable {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_count: row.required_u64("part_count")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            last_modification: row.required_datetime("last_modification")?,
            partition_count: row.required_u64("partition_count")?,
            compression_ratio: row.required_f64("compression_ratio")?,
        })
    })
}

pub(super) fn parse_large_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLargeTable>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseLargeTable {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            part_count: row.required_u64("part_count")?,
            partition_count: row.required_u64("partition_count")?,
            last_modification: row.required_datetime("last_modification")?,
            compression_ratio: row.required_f64("compression_ratio")?,
            engine: row.required_string("engine")?,
        })
    })
}

pub(super) fn parse_broken_parts_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseBrokenPartsTable>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseBrokenPartsTable {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            broken_part_count: row.required_u64("broken_part_count")?,
            last_error_time: row.required_datetime("last_error_time")?,
            sample_exception: row.optional_string("sample_exception")?,
        })
    })
}

pub(super) fn parse_partition_info(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePartitionInfo>> {
    parse_rows(rows, |row| {
        Ok(ClickhousePartitionInfo {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            partition: row.required_string("partition")?,
            partition_size: row.required_u64("partition_size")?,
            partition_rows: row.required_u64("partition_rows")?,
            part_count: row.required_u64("part_count")?,
            oldest_date: row.required_datetime("oldest_date")?,
            newest_date: row.required_datetime("newest_date")?,
        })
    })
}

pub(super) fn parse_storage_by_database(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseStorageByDatabase>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseStorageByDatabase {
            database: row.required_string("database")?,
            table_count: row.required_u64("table_count")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            total_parts: row.required_u64("total_parts")?,
        })
    })
}
