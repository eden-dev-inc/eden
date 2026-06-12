use super::{
    ClickhouseDictionarySourceInfo, ClickhouseDictionaryUpdate, ClickhouseFailedDictionary, ClickhouseMemoryDictionary,
    ClickhousePoorPerformanceDictionary, ClickhouseSlowDictionary,
};
use crate::metadata::stc::utils::{RowExt, parse_rows};
use crate::output::ClickhouseRow;
use error::ResultEP;

pub(super) fn parse_failed_dictionaries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseFailedDictionary>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseFailedDictionary {
            name: row.required_string("name")?,
            database: row.required_string("database")?,
            source: row.required_string("source")?,
            last_exception: row.required_string("last_exception")?,
            last_exception_time: row.optional_datetime("last_exception_time")?,
            loading_start_time: row.optional_datetime("loading_start_time")?,
            loading_duration: row.required_f64("loading_duration")?,
            origin: row.required_string("origin")?,
            dictionary_type: row.required_string("type")?,
            key_definition: row.required_string("key")?,
            lifetime_min: row.required_u64("lifetime_min")?,
            lifetime_max: row.required_u64("lifetime_max")?,
        })
    })
}

pub(super) fn parse_slow_dictionaries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseSlowDictionary>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseSlowDictionary {
            name: row.required_string("name")?,
            database: row.required_string("database")?,
            source: row.required_string("source")?,
            loading_duration: row.required_f64("loading_duration")?,
            loading_start_time: row.optional_datetime("loading_start_time")?,
            element_count: row.required_u64("element_count")?,
            bytes_allocated: row.required_u64("bytes_allocated")?,
            status: row.required_string("status")?,
            origin: row.required_string("origin")?,
            dictionary_type: row.required_string("type")?,
        })
    })
}

pub(super) fn parse_memory_dictionaries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseMemoryDictionary>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseMemoryDictionary {
            name: row.required_string("name")?,
            database: row.required_string("database")?,
            source: row.required_string("source")?,
            bytes_allocated: row.required_u64("bytes_allocated")?,
            element_count: row.required_u64("element_count")?,
            loading_duration: row.required_f64("loading_duration")?,
            last_successful_update_time: row.optional_datetime("last_successful_update_time")?,
            status: row.required_string("status")?,
            dictionary_type: row.required_string("type")?,
            origin: row.required_string("origin")?,
        })
    })
}

pub(super) fn parse_poor_performance_dictionaries(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhousePoorPerformanceDictionary>> {
    parse_rows(rows, |row| {
        Ok(ClickhousePoorPerformanceDictionary {
            name: row.required_string("name")?,
            database: row.required_string("database")?,
            source: row.required_string("source")?,
            hits: row.required_u64("hits")?,
            misses: row.required_u64("misses")?,
            hit_rate: row.required_f64("hit_rate")?,
            element_count: row.required_u64("element_count")?,
            bytes_allocated: row.required_u64("bytes_allocated")?,
            last_successful_update_time: row.optional_datetime("last_successful_update_time")?,
            dictionary_type: row.required_string("type")?,
        })
    })
}

pub(super) fn parse_dictionary_updates(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDictionaryUpdate>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseDictionaryUpdate {
            name: row.required_string("name")?,
            database: row.required_string("database")?,
            source: row.required_string("source")?,
            last_successful_update_time: row.optional_datetime("last_successful_update_time")?,
            element_count: row.required_u64("element_count")?,
            bytes_allocated: row.required_u64("bytes_allocated")?,
            loading_duration: row.required_f64("loading_duration")?,
            status: row.required_string("status")?,
            dictionary_type: row.required_string("type")?,
        })
    })
}

pub(super) fn parse_source_info(rows: Vec<ClickhouseRow>) -> ResultEP<Vec<ClickhouseDictionarySourceInfo>> {
    parse_rows(rows, |row| {
        Ok(ClickhouseDictionarySourceInfo {
            source: row.required_string("source")?,
            dictionary_count: row.required_u64("dictionary_count")?,
            loaded_count: row.required_u64("loaded_count")?,
            failed_count: row.required_u64("failed_count")?,
            total_memory: row.required_u64("total_memory")?,
            total_elements: row.required_u64("total_elements")?,
            avg_load_time: row.required_f64("avg_load_time")?,
        })
    })
}
