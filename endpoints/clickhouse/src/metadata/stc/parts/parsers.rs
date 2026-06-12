use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_fragmented_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFragmentedTable>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        tables.push(ClickhouseFragmentedTable {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_count: row.required_u64("part_count")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            partition_count: row.required_u64("partition_count")?,
            last_modification: row.optional_datetime("last_modification")?,
            first_modification: row.optional_datetime("first_modification")?,
            engine: row.required_string("engine")?,
            avg_compression_ratio: row.required_f64("avg_compression_ratio")?,
        });
    }

    Ok(tables)
}

pub(super) fn parse_large_parts(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLargePart>> {
    let mut parts = Vec::with_capacity(rows.len());

    for row in rows {
        parts.push(ClickhouseLargePart {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_name: row.required_string("part_name")?,
            partition: row.required_string("partition")?,
            bytes_on_disk: row.required_u64("bytes_on_disk")?,
            data_uncompressed_bytes: row.required_u64("data_uncompressed_bytes")?,
            rows: row.required_u64("rows")?,
            modification_time: row.optional_datetime("modification_time")?,
            compression_ratio: row.required_f64("compression_ratio")?,
            level: row.required_u64("level")?,
            is_mutation: row.required_bool("is_mutation")?,
            marks_count: row.required_u64("marks_count")?,
            primary_key_bytes_in_memory: row.required_u64("primary_key_bytes_in_memory")?,
        });
    }

    Ok(parts)
}

pub(super) fn parse_poor_compression_parts(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePoorCompressionPart>> {
    let mut parts = Vec::with_capacity(rows.len());

    for row in rows {
        parts.push(ClickhousePoorCompressionPart {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_name: row.required_string("part_name")?,
            partition: row.required_string("partition")?,
            bytes_on_disk: row.required_u64("bytes_on_disk")?,
            data_uncompressed_bytes: row.required_u64("data_uncompressed_bytes")?,
            compression_ratio: row.required_f64("compression_ratio")?,
            rows: row.required_u64("rows")?,
            modification_time: row.optional_datetime("modification_time")?,
            marks_count: row.required_u64("marks_count")?,
            level: row.required_u64("level")?,
        });
    }

    Ok(parts)
}

pub(super) fn parse_recent_parts(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseRecentPart>> {
    let mut parts = Vec::with_capacity(rows.len());

    for row in rows {
        parts.push(ClickhouseRecentPart {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_name: row.required_string("part_name")?,
            partition: row.required_string("partition")?,
            bytes_on_disk: row.required_u64("bytes_on_disk")?,
            rows: row.required_u64("rows")?,
            modification_time: row.optional_datetime("modification_time")?,
            level: row.required_u64("level")?,
            is_mutation: row.required_bool("is_mutation")?,
            compression_ratio: row.required_f64("compression_ratio")?,
        });
    }

    Ok(parts)
}

pub(super) fn parse_detached_parts(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDetachedPart>> {
    let mut parts = Vec::with_capacity(rows.len());

    for row in rows {
        parts.push(ClickhouseDetachedPart {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            partition_id: row.required_string("partition_id")?,
            part_name: row.required_string("part_name")?,
            disk: row.required_string("disk")?,
            reason: row.optional_string("reason")?,
            min_block_number: row.required_u64("min_block_number")?,
            max_block_number: row.required_u64("max_block_number")?,
            level: row.required_u64("level")?,
        });
    }

    Ok(parts)
}

pub(super) fn parse_old_parts(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseOldPart>> {
    let mut parts = Vec::with_capacity(rows.len());

    for row in rows {
        parts.push(ClickhouseOldPart {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_name: row.required_string("part_name")?,
            partition: row.required_string("partition")?,
            bytes_on_disk: row.required_u64("bytes_on_disk")?,
            rows: row.required_u64("rows")?,
            modification_time: row.optional_datetime("modification_time")?,
            age_seconds: row.required_u64("age_seconds")?,
            level: row.required_u64("level")?,
            marks_count: row.required_u64("marks_count")?,
        });
    }

    Ok(parts)
}

pub(super) fn parse_size_distribution(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePartSizeDistribution>> {
    let mut distribution = Vec::with_capacity(rows.len());

    for row in rows {
        distribution.push(ClickhousePartSizeDistribution {
            size_category: row.required_string("size_category")?,
            part_count: row.required_u64("part_count")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            avg_compression_ratio: row.required_f64("avg_compression_ratio")?,
        });
    }

    Ok(distribution)
}

pub(super) fn parse_partition_analysis(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePartitionInfo>> {
    let mut partitions = Vec::with_capacity(rows.len());

    for row in rows {
        partitions.push(ClickhousePartitionInfo {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            partition: row.required_string("partition")?,
            part_count: row.required_u64("part_count")?,
            total_size: row.required_u64("total_size")?,
            total_rows: row.required_u64("total_rows")?,
            latest_part_time: row.optional_datetime("latest_part_time")?,
            earliest_part_time: row.optional_datetime("earliest_part_time")?,
            avg_compression_ratio: row.required_f64("avg_compression_ratio")?,
            total_marks: row.required_u64("total_marks")?,
        });
    }

    Ok(partitions)
}
