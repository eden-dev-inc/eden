use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;

pub(super) fn parse_large_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLargeTable>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        tables.push(ClickhouseLargeTable {
            database: row.required_string("database")?,
            table_name: row.required_string("table_name")?,
            engine: row.required_string("engine")?,
            total_bytes: row.required_u64("total_bytes")?,
            total_rows: row.required_u64("total_rows")?,
            uncompressed_bytes: row.required_u64("data_uncompressed_bytes")?,
            compressed_bytes: row.required_u64("data_compressed_bytes")?,
            compression_ratio: row.required_f64("compression_ratio")?,
            readable_size: row.required_string("readable_size")?,
            partition_key: row.optional_string("partition_key")?,
            sorting_key: row.optional_string("sorting_key")?,
            primary_key: row.optional_string("primary_key")?,
        });
    }

    Ok(tables)
}

pub(super) fn parse_compression_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePoorCompressionTable>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        let uncompressed_bytes = row.required_u64("data_uncompressed_bytes")?;
        let compression_ratio = row.required_f64("compression_ratio")?;
        let engine = row.required_string("engine")?;

        tables.push(ClickhousePoorCompressionTable {
            database: row.required_string("database")?,
            table_name: row.required_string("table_name")?,
            engine: engine.clone(),
            total_bytes: row.required_u64("total_bytes")?,
            total_rows: row.required_u64("total_rows")?,
            uncompressed_bytes,
            compressed_bytes: row.required_u64("data_compressed_bytes")?,
            compression_ratio,
            readable_size: row.required_string("readable_size")?,
            compression_codec: row.optional_string("compression_codec")?,
            potential_savings: ClickhouseStorageInfo::calculate_potential_savings(uncompressed_bytes, compression_ratio),
            recommended_codec: ClickhouseStorageInfo::get_recommended_compression_codec(&engine),
        });
    }

    Ok(tables)
}

pub(super) fn parse_fragmented_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFragmentedTable>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        let parts_count = row.required_u64("parts_count")?;
        let total_size = row.required_u64("total_size")?;

        tables.push(ClickhouseFragmentedTable {
            database: row.required_string("database")?,
            table_name: row.required_string("table")?,
            parts_count,
            total_size,
            total_rows: row.required_u64("total_rows")?,
            last_modification: row.optional_datetime("last_modification")?,
            oldest_partition: row.optional_string("oldest_partition")?,
            newest_partition: row.optional_string("newest_partition")?,
            fragmentation_level: ClickhouseStorageInfo::calculate_fragmentation_level(parts_count),
            optimization_urgency: ClickhouseStorageInfo::calculate_optimization_urgency(parts_count, total_size),
        });
    }

    Ok(tables)
}

pub(super) fn parse_active_merges(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseActiveMerge>> {
    let mut merges = Vec::with_capacity(rows.len());

    for row in rows {
        let elapsed = row.required_f64("elapsed")?;
        let progress = row.required_f64("progress")?;

        merges.push(ClickhouseActiveMerge {
            database: row.required_string("database")?,
            table_name: row.required_string("table")?,
            elapsed_seconds: elapsed,
            progress,
            num_parts: row.required_u64("num_parts")?,
            result_part_name: row.optional_string("result_part_name")?,
            bytes_read_uncompressed: row.required_u64("bytes_read_uncompressed")?,
            bytes_written_uncompressed: row.required_u64("bytes_written_uncompressed")?,
            rows_read: row.required_u64("rows_read")?,
            rows_written: row.required_u64("rows_written")?,
            columns_written: row.required_u64("columns_written")?,
            memory_usage: row.required_u64("memory_usage")?,
            thread_id: row.required_u64("thread_id")?,
            estimated_completion_time: ClickhouseStorageInfo::calculate_estimated_completion(progress, elapsed),
        });
    }

    Ok(merges)
}

pub(super) fn parse_database_stats(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDatabaseStorageStats>> {
    let mut stats = Vec::with_capacity(rows.len());

    for row in rows {
        let table_count = row.required_u64("table_count")?;
        let total_size = row.required_u64("total_size")?;
        let avg_compression_ratio = row.required_f64("avg_compression_ratio")?;

        stats.push(ClickhouseDatabaseStorageStats {
            database: row.required_string("database")?,
            table_count,
            total_size,
            total_rows: row.required_u64("total_rows")?,
            avg_compression_ratio,
            total_uncompressed: row.required_u64("total_uncompressed")?,
            total_compressed: row.required_u64("total_compressed")?,
            readable_size: row.required_string("readable_size")?,
            avg_table_size: if table_count > 0 { total_size / table_count } else { 0 },
            storage_efficiency: ClickhouseStorageInfo::calculate_storage_efficiency(avg_compression_ratio, table_count),
        });
    }

    Ok(stats)
}

pub(super) fn parse_partition_info(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePartitionInfo>> {
    let mut partitions = Vec::with_capacity(rows.len());

    for row in rows {
        let parts_in_partition = row.required_u64("parts_in_partition")?;
        let partition_size = row.required_u64("partition_size")?;

        partitions.push(ClickhousePartitionInfo {
            database: row.required_string("database")?,
            table_name: row.required_string("table")?,
            partition: row.required_string("partition")?,
            parts_in_partition,
            partition_size,
            partition_rows: row.required_u64("partition_rows")?,
            partition_min_date: row.optional_string("partition_min_date")?,
            partition_max_date: row.optional_string("partition_max_date")?,
            last_modified: row.optional_datetime("last_modified")?,
            partition_health: ClickhouseStorageInfo::calculate_partition_health(parts_in_partition, partition_size),
        });
    }

    Ok(partitions)
}
