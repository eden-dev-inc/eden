use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_long_merges(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLongMerge>> {
    let mut merges = Vec::with_capacity(rows.len());

    for row in rows {
        merges.push(ClickhouseLongMerge {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            elapsed: row.required_f64("elapsed")?,
            progress: row.required_f64("progress")?,
            total_size_bytes: row.required_u64("total_size_bytes_compressed")?,
            total_size_marks: row.required_u64("total_size_marks")?,
            num_parts: row.required_u64("num_parts")?,
            result_part_name: row.optional_string("result_part_name")?,
            merge_type: row.required_string("merge_type")?,
            merge_algorithm: row.required_string("merge_algorithm")?,
            first_source_part: row.optional_string("first_source_part")?,
            is_mutation: row.required_bool("is_mutation")?,
        });
    }

    Ok(merges)
}

pub(super) fn parse_large_merges(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLargeMerge>> {
    let mut merges = Vec::with_capacity(rows.len());

    for row in rows {
        merges.push(ClickhouseLargeMerge {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            total_size_bytes: row.required_u64("total_size_bytes_compressed")?,
            total_size_marks: row.required_u64("total_size_marks")?,
            num_parts: row.required_u64("num_parts")?,
            elapsed: row.required_f64("elapsed")?,
            progress: row.required_f64("progress")?,
            merge_type: row.required_string("merge_type")?,
            merge_algorithm: row.required_string("merge_algorithm")?,
            result_part_name: row.optional_string("result_part_name")?,
            is_mutation: row.required_bool("is_mutation")?,
        });
    }

    Ok(merges)
}

pub(super) fn parse_mutations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseMutationInfo>> {
    let mut mutations = Vec::with_capacity(rows.len());

    for row in rows {
        mutations.push(ClickhouseMutationInfo {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            mutation_id: row.required_string("mutation_id")?,
            command: row.required_string("command")?,
            create_time: row.required_datetime("create_time")?,
            block_number: row.required_u64("block_number")?,
            parts_to_do: row.optional_string("parts_to_do_names")?,
            is_done: row.required_bool("is_done")?,
            latest_failed_part: row.optional_string("latest_failed_part")?,
            latest_fail_time: row.optional_datetime("latest_fail_time")?,
            latest_fail_reason: row.optional_string("latest_fail_reason")?,
        });
    }

    Ok(mutations)
}

pub(super) fn parse_fragmented_tables(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFragmentedTableMerge>> {
    let mut tables = Vec::with_capacity(rows.len());

    for row in rows {
        tables.push(ClickhouseFragmentedTableMerge {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            part_count: row.required_u64("part_count")?,
            total_size: row.required_u64("total_size")?,
            partition_count: row.required_u64("partition_count")?,
            last_modified: row.required_datetime("last_modified")?,
            engine: row.required_string("engine")?,
        });
    }

    Ok(tables)
}

pub(super) fn parse_queue_analysis(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseMergeQueueInfo>> {
    let mut queue_items = Vec::with_capacity(rows.len());

    for row in rows {
        queue_items.push(ClickhouseMergeQueueInfo {
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

    Ok(queue_items)
}

pub(super) fn parse_background_processes(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseBackgroundProcess>> {
    let mut processes = Vec::with_capacity(rows.len());

    for row in rows {
        processes.push(ClickhouseBackgroundProcess {
            task_name: row.required_string("task_name")?,
            process_type: row.required_string("type")?,
            schedule_time: None,
            last_execution_time: None,
            exception: row.optional_string("description")?,
        });
    }

    Ok(processes)
}
