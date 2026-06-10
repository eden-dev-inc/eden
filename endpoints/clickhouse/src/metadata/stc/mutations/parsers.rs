use super::*;
use crate::metadata::stc::utils::RowExt;
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_long_mutations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLongMutation>> {
    let mut mutations = Vec::with_capacity(rows.len());

    for row in rows {
        mutations.push(ClickhouseLongMutation {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            mutation_id: row.required_string("mutation_id")?,
            command: row.required_string("command")?,
            create_time: row.required_datetime("create_time")?,
            duration: row.required_f64("duration")?,
            parts_to_do: row.required_u64("parts_to_do")?,
            parts_completed: row.required_u64("parts_completed")?,
            latest_failed_part: row.optional_string("latest_failed_part")?,
            latest_fail_time: row.optional_datetime("latest_fail_time")?,
            latest_fail_reason: row.optional_string("latest_fail_reason")?,
            block_number: row.required_u64("block_number")?,
        });
    }

    Ok(mutations)
}

pub(super) fn parse_failed_mutations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFailedMutation>> {
    let mut mutations = Vec::with_capacity(rows.len());

    for row in rows {
        mutations.push(ClickhouseFailedMutation {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            mutation_id: row.required_string("mutation_id")?,
            command: row.required_string("command")?,
            create_time: row.required_datetime("create_time")?,
            latest_failed_part: row.required_string("latest_failed_part")?,
            latest_fail_time: row.required_datetime("latest_fail_time")?,
            latest_fail_reason: row.optional_string("latest_fail_reason")?,
            parts_to_do: row.required_u64("parts_to_do")?,
            parts_completed_before_failure: row.required_u64("parts_completed_before_failure")?,
            block_number: row.required_u64("block_number")?,
        });
    }

    Ok(mutations)
}

pub(super) fn parse_stuck_mutations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseStuckMutation>> {
    let mut mutations = Vec::with_capacity(rows.len());

    for row in rows {
        mutations.push(ClickhouseStuckMutation {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            mutation_id: row.required_string("mutation_id")?,
            command: row.required_string("command")?,
            create_time: row.required_datetime("create_time")?,
            stuck_duration: row.required_f64("stuck_duration")?,
            parts_to_do: row.required_u64("parts_to_do")?,
            parts_completed: row.required_u64("parts_completed")?,
            latest_fail_time: row.optional_datetime("latest_fail_time")?,
            latest_fail_reason: row.optional_string("latest_fail_reason")?,
            latest_failed_part: row.optional_string("latest_failed_part")?,
        });
    }

    Ok(mutations)
}

pub(super) fn parse_large_mutations(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseLargeMutation>> {
    let mut mutations = Vec::with_capacity(rows.len());

    for row in rows {
        mutations.push(ClickhouseLargeMutation {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            mutation_id: row.required_string("mutation_id")?,
            command: row.required_string("command")?,
            create_time: row.required_datetime("create_time")?,
            parts_to_do: row.required_u64("parts_to_do")?,
            parts_completed: row.required_u64("parts_completed")?,
            total_parts: row.required_u64("total_parts")?,
            duration: row.required_f64("duration")?,
            latest_fail_time: row.optional_datetime("latest_fail_time")?,
            is_done: row.required_bool("is_done")?,
        });
    }

    Ok(mutations)
}

pub(super) fn parse_mutation_completions(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseMutationCompletion>> {
    let mut completions = Vec::with_capacity(rows.len());

    for row in rows {
        completions.push(ClickhouseMutationCompletion {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            mutation_id: row.required_string("mutation_id")?,
            command: row.required_string("command")?,
            create_time: row.required_datetime("create_time")?,
            completion_time: row.required_datetime("completion_time")?,
            total_duration: row.required_f64("total_duration")?,
            parts_processed: row.required_u64("parts_processed")?,
            block_number: row.required_u64("block_number")?,
        });
    }

    Ok(completions)
}

pub(super) fn parse_command_stats(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseMutationCommandStats>> {
    let mut stats = Vec::with_capacity(rows.len());

    for row in rows {
        stats.push(ClickhouseMutationCommandStats {
            command_type: row.required_string("command_type")?,
            total_count: row.required_u64("total_count")?,
            active_count: row.required_u64("active_count")?,
            completed_count: row.required_u64("completed_count")?,
            failed_count: row.required_u64("failed_count")?,
            avg_duration: row.required_f64("avg_duration")?,
        });
    }

    Ok(stats)
}

pub(super) fn parse_table_mutation_info(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseTableMutationInfo>> {
    let mut table_info = Vec::with_capacity(rows.len());

    for row in rows {
        table_info.push(ClickhouseTableMutationInfo {
            database: row.required_string("database")?,
            table: row.required_string("table")?,
            mutation_count: row.required_u64("mutation_count")?,
            active_mutation_count: row.required_u64("active_mutation_count")?,
            total_parts_to_mutate: row.required_u64("total_parts_to_mutate")?,
            oldest_mutation_age: row.required_f64("oldest_mutation_age")?,
            failed_mutation_count: row.required_u64("failed_mutation_count")?,
        });
    }

    Ok(table_info)
}
