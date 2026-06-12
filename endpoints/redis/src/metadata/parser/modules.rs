use std::collections::{HashMap, HashSet};

use crate::command;
use eden_logger_internal::{ctx_with_trace, log_debug};
use endpoint_types::metadata::CapabilityChecker;
use error::{EpError, ResultEP};
use function_name::named;
use redis::Value;
use redis_core::RedisAsync;
use telemetry::TelemetryWrapper;

use super::common::{
    execute_command, fetch_info_section, mark_endpoint_response, parse_default, value_as_f64, value_as_string, value_as_u64, value_to_map,
    value_to_vec,
};
use crate::metadata::stc::module::RedisModulesInfo;

const MODULE_SCAN_COUNT: usize = 512;
const MODULE_DEFAULT_MAX_KEYS: usize = 128;

fn ensure_module_placeholder(modules_info: &mut RedisModulesInfo, module_name: &str) {
    match module_name {
        "search" | "redisearch" => {
            modules_info.redisearch.get_or_insert_with(crate::metadata::stc::module::RediSearchInfo::default);
        }
        "rejson" | "json" => {
            modules_info.rejson.get_or_insert_with(crate::metadata::stc::module::ReJSONInfo::default);
        }
        "timeseries" | "ts" => {
            modules_info.timeseries.get_or_insert_with(crate::metadata::stc::module::TimeSeriesInfo::default);
        }
        "bf" | "redisbloom" | "bloom" => {
            modules_info.redisbloom.get_or_insert_with(crate::metadata::stc::module::RedisBloomInfo::default);
        }
        "graph" | "redisgraph" => {
            modules_info.redisgraph.get_or_insert_with(crate::metadata::stc::module::RedisGraphInfo::default);
        }
        _ => {}
    }
}

fn normalize_field_name(field: &str) -> String {
    field.chars().map(|c| if c == ' ' || c == '-' { '_' } else { c.to_ascii_lowercase() }).collect()
}

fn map_get<'a>(map: &'a HashMap<String, Value>, key: &str) -> Option<&'a Value> {
    let needle = normalize_field_name(key);
    map.iter().find(|(k, _)| normalize_field_name(k) == needle).map(|(_, v)| v)
}

#[named]
async fn scan_keys_internal(pool: &RedisAsync, desired_type: &str, allow_scan_type: bool, max_keys: usize) -> ResultEP<Vec<String>> {
    let _ctx = ctx_with_trace!();
    let mut connection = pool.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut cursor = "0".to_string();
    let mut keys = HashSet::new();
    let mut allow_scan_type_flag = allow_scan_type;

    loop {
        let mut command = command::cmd("SCAN");
        command.arg(&cursor);
        if allow_scan_type_flag {
            command.arg("TYPE").arg(desired_type);
        }
        command.arg("COUNT").arg(MODULE_SCAN_COUNT);

        let scan_result = execute_command::<(String, Vec<String>)>(&mut connection, command).await;
        let (next_cursor, batch) = match scan_result {
            Ok(batch) => batch,
            Err(_err) if allow_scan_type_flag => {
                log_debug!(
                    _ctx.clone(),
                    format!("SCAN with TYPE '{}' failed ({}); falling back to per-key TYPE", desired_type, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
                allow_scan_type_flag = false;
                cursor = "0".to_string();
                keys.clear();
                continue;
            }
            Err(err) => return Err(err),
        };

        for key in batch {
            if !allow_scan_type_flag {
                let mut type_cmd = command::cmd("TYPE");
                type_cmd.arg(&key);
                let key_type: String = execute_command(&mut connection, type_cmd).await?;
                if !key_type.eq_ignore_ascii_case(desired_type) {
                    continue;
                }
            }

            keys.insert(key);
            if keys.len() >= max_keys {
                break;
            }
        }

        if next_cursor == "0" || keys.len() >= max_keys {
            break;
        }

        cursor = next_cursor;
    }

    Ok(keys.into_iter().take(max_keys).collect())
}

async fn scan_module_keys(pool: &RedisAsync, desired_type: &str, max_keys: usize) -> ResultEP<Vec<String>> {
    scan_keys_internal(pool, desired_type, true, max_keys).await
}

pub async fn load_modules_info(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    _capabilities: &dyn CapabilityChecker,
) -> ResultEP<RedisModulesInfo> {
    let section = fetch_info_section(context.clone(), telemetry, "modules").await?;
    let map = section.map;

    let mut modules_info = RedisModulesInfo::default();
    let mut module_metrics: HashMap<String, HashMap<String, String>> = HashMap::new();

    // Parse module entries from raw INFO text.
    // The parsed HashMap loses duplicate `module:` keys (all map to key "module"),
    // so we extract module names directly from the raw text.
    // Format: module:name=search,ver=20807,api=1,ffilters=0,...
    for line in section.raw.lines() {
        let line = line.trim();
        if let Some(rest) = line.strip_prefix("module:") {
            let mut module_data = HashMap::new();
            let mut module_name = None;
            for part in rest.split(',') {
                if let Some((k, v)) = part.split_once('=') {
                    module_data.insert(k.to_string(), v.to_string());
                    if k == "name" {
                        module_name = Some(v.to_lowercase());
                    }
                }
            }
            if let Some(name) = module_name {
                ensure_module_placeholder(&mut modules_info, &name);
                module_metrics.insert(name, module_data);
            }
        }
    }

    // Parse module-specific metrics from the HashMap (these have unique keys like search_*)
    for (key, value) in &map {
        if key.contains("search_") || key.starts_with("search") {
            // This is RediSearch-specific metrics
            ensure_module_placeholder(&mut modules_info, "search");

            if let Some(ref mut redisearch) = modules_info.redisearch {
                let mut search_pairs = HashMap::new();
                for (k, v) in &map {
                    if k.starts_with("search") {
                        search_pairs.insert(k.clone(), v.clone());
                    }
                }
                redisearch.parse_from_pairs(&search_pairs);
            }
        } else if key.contains("json_") || key.starts_with("json") {
            // ReJSON module metrics
            ensure_module_placeholder(&mut modules_info, "rejson");

            if let Some(ref mut rejson) = modules_info.rejson {
                parse_rejson_metrics(&map, rejson);
            }
        } else if key.contains("ts_") || key.starts_with("ts") {
            // RedisTimeSeries module metrics
            ensure_module_placeholder(&mut modules_info, "timeseries");

            if let Some(ref mut timeseries) = modules_info.timeseries {
                parse_timeseries_metrics(&map, timeseries);
            }
        } else if key.contains("bf_") || key.contains("cf_") || key.contains("cms_") || key.contains("topk_") {
            // RedisBloom module metrics
            ensure_module_placeholder(&mut modules_info, "bf");

            if let Some(ref mut redisbloom) = modules_info.redisbloom {
                parse_redisbloom_metrics(&map, redisbloom);
            }
        } else if key.contains("graph_") || key.starts_with("graph") {
            // RedisGraph module metrics
            ensure_module_placeholder(&mut modules_info, "graph");

            if let Some(ref mut redisgraph) = modules_info.redisgraph {
                parse_redisgraph_metrics(&map, redisgraph);
            }
        } else {
            // Other module metrics
            let parts: Vec<&str> = key.splitn(2, ':').collect();
            if parts.len() == 2 {
                let module_name = parts[0];
                let metric_name = parts[1];

                let module_data = module_metrics.entry(module_name.to_string()).or_default();
                module_data.insert(metric_name.to_string(), value.clone());
            }
        }
    }

    // Now execute module-specific commands to get detailed metrics
    load_module_specific_metrics(context, telemetry, &mut modules_info).await?;

    // Process module metadata for version information
    for (module_name, module_data) in &module_metrics {
        match module_name.as_str() {
            "search" => {
                if let Some(redisearch) = &mut modules_info.redisearch
                    && let Some(version) = module_data.get("ver")
                {
                    redisearch.version = version.clone();
                }
            }
            "rejson" => {
                if let Some(rejson) = &mut modules_info.rejson
                    && let Some(version) = module_data.get("ver")
                {
                    rejson.version = version.clone();
                }
            }
            "timeseries" => {
                if let Some(timeseries) = &mut modules_info.timeseries
                    && let Some(version) = module_data.get("ver")
                {
                    timeseries.version = version.clone();
                }
            }
            "bf" => {
                if let Some(redisbloom) = &mut modules_info.redisbloom
                    && let Some(version) = module_data.get("ver")
                {
                    redisbloom.version = version.clone();
                }
            }
            "graph" => {
                if let Some(redisgraph) = &mut modules_info.redisgraph
                    && let Some(version) = module_data.get("ver")
                {
                    redisgraph.version = version.clone();
                }
            }
            _ => {
                // Keep in other_modules for unknown modules
            }
        }
    }

    modules_info.other_modules = module_metrics;

    Ok(modules_info)
}

/// Execute module-specific commands to get detailed metrics
#[named]
async fn load_module_specific_metrics(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    modules_info: &mut RedisModulesInfo,
) -> ResultEP<()> {
    let _ctx = ctx_with_trace!();
    if let Some(ref mut redisearch) = modules_info.redisearch
        && let Err(_err) = enrich_redisearch_details(context.clone(), redisearch).await
    {
        log_debug!(
            _ctx.clone(),
            format!("Failed to load RediSearch details: {_err}"),
            audience = eden_logger_internal::LogAudience::Internal
        );
    }

    if let Some(ref mut rejson) = modules_info.rejson
        && let Err(_err) = enrich_rejson_details(context.clone(), rejson).await
    {
        log_debug!(
            _ctx.clone(),
            format!("Failed to load ReJSON details: {_err}"),
            audience = eden_logger_internal::LogAudience::Internal
        );
    }

    if let Some(ref mut timeseries) = modules_info.timeseries
        && let Err(_err) = enrich_timeseries_details(context.clone(), timeseries).await
    {
        log_debug!(
            _ctx.clone(),
            format!("Failed to load RedisTimeSeries details: {_err}"),
            audience = eden_logger_internal::LogAudience::Internal
        );
    }

    if let Some(ref mut redisbloom) = modules_info.redisbloom
        && let Err(_err) = enrich_redisbloom_details(context.clone(), redisbloom).await
    {
        log_debug!(
            _ctx.clone(),
            format!("Failed to load RedisBloom details: {_err}"),
            audience = eden_logger_internal::LogAudience::Internal
        );
    }

    if let Some(ref mut redisgraph) = modules_info.redisgraph
        && let Err(_err) = enrich_redisgraph_details(context.clone(), redisgraph).await
    {
        log_debug!(
            _ctx,
            format!("Failed to load RedisGraph details: {_err}"),
            audience = eden_logger_internal::LogAudience::Internal
        );
    }

    mark_endpoint_response(telemetry);

    Ok(())
}

#[named]
async fn enrich_redisearch_details(context: RedisAsync, redisearch: &mut crate::metadata::stc::module::RediSearchInfo) -> ResultEP<()> {
    let _ctx = ctx_with_trace!();
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let indexes = {
        let cmd = command::cmd("FT._LIST");
        match execute_command::<Vec<String>>(&mut connection, cmd).await {
            Ok(list) if !list.is_empty() => list,
            _ => return Ok(()),
        }
    };

    let mut total_docs = 0u64;
    let mut max_doc_id = redisearch.search_max_doc_id;
    let mut total_terms = 0u64;
    let mut total_records = 0u64;
    let mut inverted_mb = 0.0;
    let mut vector_mb = 0.0;
    let mut total_blocks = 0u64;
    let mut offset_vectors_mb = 0.0;
    let mut doc_table_mb = 0.0;
    let mut sortable_values_mb = 0.0;
    let mut key_table_mb = 0.0;
    let mut total_indexing_time = 0.0;
    let mut total_hash_failures = 0u64;
    let mut indexing_in_progress = false;
    let mut percent_indexed: f64 = 0.0;
    let mut total_uses = 0u64;

    for index in indexes {
        let stats_values = {
            let mut cmd = command::cmd("FT.INFO");
            cmd.arg(&index);
            match execute_command::<Vec<Value>>(&mut connection, cmd).await {
                Ok(values) => Value::Array(values),
                Err(_err) => {
                    log_debug!(
                        _ctx.clone(),
                        format!("FT.INFO {} failed: {}", index, _err),
                        audience = eden_logger_internal::LogAudience::Internal
                    );
                    continue;
                }
            }
        };

        if let Some(map) = value_to_map(&stats_values) {
            if let Some(value) = map.get("num_docs").and_then(value_as_u64) {
                total_docs += value;
            }

            if let Some(value) = map.get("max_doc_id").and_then(value_as_u64) {
                max_doc_id = max_doc_id.max(value);
            }

            if let Some(value) = map.get("num_terms").and_then(value_as_u64) {
                total_terms += value;
            }

            if let Some(value) = map.get("num_records").and_then(value_as_u64) {
                total_records += value;
            }

            if let Some(value) = map.get("inverted_sz_mb").and_then(value_as_f64) {
                inverted_mb += value;
            }

            if let Some(value) = map.get("vector_index_sz_mb").and_then(value_as_f64) {
                vector_mb += value;
            }

            if let Some(value) = map.get("total_inverted_index_blocks").and_then(value_as_u64) {
                total_blocks += value;
            }

            if let Some(value) = map.get("offset_vectors_sz_mb").and_then(value_as_f64) {
                offset_vectors_mb += value;
            }

            if let Some(value) = map.get("doc_table_size_mb").and_then(value_as_f64) {
                doc_table_mb += value;
            }

            if let Some(value) = map.get("sortable_values_size_mb").and_then(value_as_f64) {
                sortable_values_mb += value;
            }

            if let Some(value) = map.get("key_table_size_mb").and_then(value_as_f64) {
                key_table_mb += value;
            }

            if let Some(value) = map.get("hash_indexing_failures").and_then(value_as_u64) {
                total_hash_failures += value;
            }

            if let Some(value) = map.get("total_indexing_time").and_then(value_as_f64) {
                total_indexing_time += value;
            }

            if let Some(value) = map.get("indexing")
                && let Some(flag) = value_as_string(value)
                && flag == "1"
            {
                indexing_in_progress = true;
            }

            if let Some(value) = map.get("percent_indexed").and_then(value_as_f64) {
                percent_indexed = percent_indexed.max(value);
            }

            if let Some(value) = map.get("number_of_uses").and_then(value_as_u64) {
                total_uses += value;
            }
        }
    }

    if total_docs > 0 {
        redisearch.search_number_of_documents = total_docs;
    }
    redisearch.search_max_doc_id = max_doc_id;
    if total_terms > 0 {
        redisearch.search_number_of_terms = total_terms;
    }
    if total_records > 0 {
        redisearch.search_number_of_records = total_records;
    }
    if inverted_mb > 0.0 {
        redisearch.search_inverted_index_mb = inverted_mb;
    }
    if vector_mb > 0.0 {
        redisearch.search_vector_index_mb = vector_mb;
    }
    if total_blocks > 0 {
        redisearch.search_total_inverted_index_blocks = total_blocks;
    }
    if offset_vectors_mb > 0.0 {
        redisearch.search_offset_vectors_mb = offset_vectors_mb;
    }
    if doc_table_mb > 0.0 {
        redisearch.search_doc_table_size_mb = doc_table_mb;
    }
    if sortable_values_mb > 0.0 {
        redisearch.search_sortable_values_size_mb = sortable_values_mb;
    }
    if key_table_mb > 0.0 {
        redisearch.search_key_table_size_mb = key_table_mb;
    }
    if total_hash_failures > 0 {
        redisearch.search_hash_indexing_failures = total_hash_failures;
    }
    if total_indexing_time > 0.0 {
        redisearch.search_total_indexing_time_sec = total_indexing_time;
    }
    if percent_indexed > 0.0 {
        redisearch.search_indexing_percentage = percent_indexed;
    }
    if total_uses > 0 {
        redisearch.search_number_of_uses = total_uses;
    }
    if indexing_in_progress {
        redisearch.search_indexing_in_progress = true;
    }
    if redisearch.search_number_of_documents > 0 {
        redisearch.search_global_stats_available = true;
    }

    Ok(())
}

#[named]
async fn enrich_rejson_details(context: RedisAsync, rejson: &mut crate::metadata::stc::module::ReJSONInfo) -> ResultEP<()> {
    let _ctx = ctx_with_trace!();
    let keys = scan_module_keys(&context, "ReJSON-RL", MODULE_DEFAULT_MAX_KEYS).await?;
    if keys.is_empty() {
        return Ok(());
    }

    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut total_docs = 0u64;
    let mut total_memory = 0u64;

    for key in &keys {
        total_docs = total_docs.saturating_add(1);

        let mut cmd = command::cmd("JSON.DEBUG");
        cmd.arg("MEMORY").arg(key);
        match execute_command::<u64>(&mut connection, cmd).await {
            Ok(memory) => total_memory = total_memory.saturating_add(memory),
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("JSON.DEBUG MEMORY {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }
    }

    if total_docs > 0 {
        rejson.json_docs = rejson.json_docs.max(total_docs);
    }

    if total_memory > 0 {
        rejson.json_memory_usage = total_memory;
    }

    Ok(())
}

#[named]
async fn enrich_timeseries_details(context: RedisAsync, timeseries: &mut crate::metadata::stc::module::TimeSeriesInfo) -> ResultEP<()> {
    let _ctx = ctx_with_trace!();
    let keys = scan_module_keys(&context, "TSDB-TYPE", MODULE_DEFAULT_MAX_KEYS).await?;
    if keys.is_empty() {
        return Ok(());
    }

    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut total_samples = 0u64;
    let mut total_memory = 0u64;
    let mut total_chunks = 0u64;
    let mut total_chunks_disk = 0u64;
    let mut duplicate_samples = 0u64;

    for key in &keys {
        let mut cmd = command::cmd("TS.INFO");
        cmd.arg(key);
        match execute_command::<Vec<Value>>(&mut connection, cmd).await {
            Ok(raw) => {
                if let Some(map) = value_to_map(&Value::Array(raw)) {
                    if let Some(value) = map_get(&map, "totalSamples").and_then(value_as_u64) {
                        total_samples = total_samples.saturating_add(value);
                    }
                    if let Some(value) = map_get(&map, "memoryUsage").and_then(value_as_u64) {
                        total_memory = total_memory.saturating_add(value);
                    }
                    if let Some(value) = map_get(&map, "chunksCount").and_then(value_as_u64) {
                        total_chunks = total_chunks.saturating_add(value);
                    }
                    if let Some(value) = map_get(&map, "chunksCompressed").and_then(value_as_u64) {
                        total_chunks_disk = total_chunks_disk.saturating_add(value);
                    }
                    if let Some(value) = map_get(&map, "duplicateSamples").and_then(value_as_u64) {
                        duplicate_samples = duplicate_samples.saturating_add(value);
                    }
                }
            }
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("TS.INFO {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }
    }

    let series_count = timeseries.ts_num_series.max(timeseries.ts_number_of_series);
    let discovered_series = keys.len() as u64;
    let effective_series = series_count.max(discovered_series);
    timeseries.ts_num_series = effective_series;
    timeseries.ts_number_of_series = effective_series;

    if total_samples > 0 {
        timeseries.ts_num_samples = total_samples;
        timeseries.ts_total_samples = total_samples;
    }

    if total_memory > 0 {
        timeseries.ts_memory_usage = total_memory;
    }

    if total_chunks > 0 {
        timeseries.ts_num_chunks = total_chunks;
        timeseries.ts_total_chunks = total_chunks;
    }

    if total_chunks_disk > 0 {
        timeseries.ts_num_chunks_disk = total_chunks_disk;
    }

    if duplicate_samples > 0 {
        timeseries.ts_duplicate_samples = duplicate_samples;
    }

    Ok(())
}

#[named]
async fn enrich_redisbloom_details(context: RedisAsync, redisbloom: &mut crate::metadata::stc::module::RedisBloomInfo) -> ResultEP<()> {
    let _ctx = ctx_with_trace!();
    let bloom_keys = match scan_module_keys(&context, "bloom", MODULE_DEFAULT_MAX_KEYS / 2).await {
        Ok(keys) => keys,
        Err(_err) => {
            log_debug!(
                _ctx.clone(),
                format!("Failed to scan bloom keys: {_err}"),
                audience = eden_logger_internal::LogAudience::Internal
            );
            Vec::new()
        }
    };
    let cuckoo_keys = match scan_module_keys(&context, "cuckoo", MODULE_DEFAULT_MAX_KEYS / 2).await {
        Ok(keys) => keys,
        Err(_err) => {
            log_debug!(
                _ctx.clone(),
                format!("Failed to scan cuckoo keys: {_err}"),
                audience = eden_logger_internal::LogAudience::Internal
            );
            Vec::new()
        }
    };
    let cms_keys = match scan_module_keys(&context, "cms", MODULE_DEFAULT_MAX_KEYS / 2).await {
        Ok(keys) => keys,
        Err(_err) => {
            log_debug!(
                _ctx.clone(),
                format!("Failed to scan CMS keys: {_err}"),
                audience = eden_logger_internal::LogAudience::Internal
            );
            Vec::new()
        }
    };
    let topk_keys = match scan_module_keys(&context, "topk", MODULE_DEFAULT_MAX_KEYS / 2).await {
        Ok(keys) => keys,
        Err(_err) => {
            log_debug!(
                _ctx.clone(),
                format!("Failed to scan TopK keys: {_err}"),
                audience = eden_logger_internal::LogAudience::Internal
            );
            Vec::new()
        }
    };

    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut bf_capacity = 0u64;
    let mut bf_size_bytes = 0u64;
    let mut bf_memory = 0u64;

    for key in &bloom_keys {
        let mut bf_info_cmd = command::cmd("BF.INFO");
        bf_info_cmd.arg(key);
        match execute_command::<Vec<Value>>(&mut connection, bf_info_cmd).await {
            Ok(raw) => {
                if let Some(map) = value_to_map(&Value::Array(raw)) {
                    if let Some(value) = map_get(&map, "Capacity").and_then(value_as_u64) {
                        bf_capacity = bf_capacity.saturating_add(value);
                    }
                    if let Some(value) = map_get(&map, "Size").and_then(value_as_u64) {
                        bf_size_bytes = bf_size_bytes.saturating_add(value);
                    }
                }
            }
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("BF.INFO {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }

        let mut memory_cmd = command::cmd("MEMORY");
        memory_cmd.arg("USAGE").arg(key);
        match execute_command::<u64>(&mut connection, memory_cmd).await {
            Ok(memory) => bf_memory = bf_memory.saturating_add(memory),
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("MEMORY USAGE {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }
    }

    let mut cf_memory = 0u64;
    for key in &cuckoo_keys {
        let mut memory_cmd = command::cmd("MEMORY");
        memory_cmd.arg("USAGE").arg(key);
        match execute_command::<u64>(&mut connection, memory_cmd).await {
            Ok(memory) => cf_memory = cf_memory.saturating_add(memory),
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("MEMORY USAGE {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }
    }

    let mut cms_memory = 0u64;
    for key in &cms_keys {
        let mut memory_cmd = command::cmd("MEMORY");
        memory_cmd.arg("USAGE").arg(key);
        match execute_command::<u64>(&mut connection, memory_cmd).await {
            Ok(memory) => cms_memory = cms_memory.saturating_add(memory),
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("MEMORY USAGE {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }
    }

    let mut topk_memory = 0u64;
    for key in &topk_keys {
        let mut memory_cmd = command::cmd("MEMORY");
        memory_cmd.arg("USAGE").arg(key);
        match execute_command::<u64>(&mut connection, memory_cmd).await {
            Ok(memory) => topk_memory = topk_memory.saturating_add(memory),
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("MEMORY USAGE {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }
    }

    let bf_filters = bloom_keys.len() as u64;
    redisbloom.bf_num_filters = redisbloom.bf_num_filters.max(bf_filters);
    redisbloom.bf_number_of_filters = redisbloom.bf_num_filters;

    if bf_capacity > 0 {
        redisbloom.bf_total_capacity = bf_capacity;
    }
    if bf_size_bytes > 0 {
        redisbloom.bf_total_size_bytes = bf_size_bytes;
    }
    if bf_memory > 0 {
        redisbloom.bf_memory_usage = bf_memory;
    }

    let cf_filters = cuckoo_keys.len() as u64;
    redisbloom.cf_num_filters = redisbloom.cf_num_filters.max(cf_filters);
    if cf_memory > 0 {
        redisbloom.cf_memory_usage = cf_memory;
    }

    let cms_sketches = cms_keys.len() as u64;
    redisbloom.cms_num_sketches = redisbloom.cms_num_sketches.max(cms_sketches);
    redisbloom.cms_number_of_sketches = redisbloom.cms_num_sketches;
    if cms_memory > 0 {
        redisbloom.cms_memory_usage = cms_memory;
    }

    let topk_lists = topk_keys.len() as u64;
    redisbloom.topk_num_sketches = redisbloom.topk_num_sketches.max(topk_lists);
    redisbloom.topk_number_of_lists = redisbloom.topk_num_sketches;
    if topk_memory > 0 {
        redisbloom.topk_memory_usage = topk_memory;
    }

    Ok(())
}

#[named]
async fn enrich_redisgraph_details(context: RedisAsync, redisgraph: &mut crate::metadata::stc::module::RedisGraphInfo) -> ResultEP<()> {
    let _ctx = ctx_with_trace!();
    let keys = scan_module_keys(&context, "graph", MODULE_DEFAULT_MAX_KEYS / 2).await?;
    if keys.is_empty() {
        return Ok(());
    }

    let mut total_memory = 0u64;
    let mut total_nodes = 0u64;
    let mut total_relationships = 0u64;

    for key in &keys {
        match memory_usage_for_key(&context, key).await {
            Ok(memory) => total_memory = total_memory.saturating_add(memory),
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("MEMORY USAGE {} failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }

        match graph_query_count(&context, key, "MATCH (n) RETURN count(n)").await {
            Ok(Some(count)) => total_nodes = total_nodes.saturating_add(count),
            Ok(None) => {}
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("Graph node count for '{}' failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }

        match graph_query_count(&context, key, "MATCH ()-[r]->() RETURN count(r)").await {
            Ok(Some(count)) => total_relationships = total_relationships.saturating_add(count),
            Ok(None) => {}
            Err(_err) => {
                log_debug!(
                    _ctx.clone(),
                    format!("Graph relationship count for '{}' failed: {}", key, _err),
                    audience = eden_logger_internal::LogAudience::Internal
                );
            }
        }
    }

    let graph_count = keys.len() as u64;
    redisgraph.graph_num_graphs = redisgraph.graph_num_graphs.max(graph_count);
    redisgraph.graph_number_of_graphs = redisgraph.graph_num_graphs;

    if total_nodes > 0 {
        redisgraph.graph_num_nodes = total_nodes;
        redisgraph.graph_total_nodes = total_nodes;
    }

    if total_relationships > 0 {
        redisgraph.graph_num_relationships = total_relationships;
        redisgraph.graph_total_relationships = total_relationships;
    }

    if total_memory > 0 {
        redisgraph.graph_memory_usage = total_memory;
    }

    Ok(())
}

async fn memory_usage_for_key(context: &RedisAsync, key: &str) -> Result<u64, EpError> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut cmd = command::cmd("MEMORY");
    cmd.arg("USAGE").arg(key);
    execute_command(&mut connection, cmd).await
}

async fn graph_query_count(context: &RedisAsync, graph: &str, query: &str) -> Result<Option<u64>, EpError> {
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;

    let mut cmd = command::cmd("GRAPH.QUERY");
    cmd.arg(graph).arg(query).arg("--compact");
    let response: Vec<Value> = execute_command(&mut connection, cmd).await?;

    Ok(parse_graph_query_result(response))
}

fn parse_graph_query_result(response: Vec<Value>) -> Option<u64> {
    if response.len() < 2 {
        return None;
    }

    let rows_value = response.get(1)?;
    let rows = value_to_vec(rows_value)?;
    let first_row = rows.first()?;
    let columns = value_to_vec(first_row)?;
    let first_value = columns.first()?;

    if let Some(count) = value_as_u64(first_value) {
        return Some(count);
    }

    if let Some(count_str) = value_as_string(first_value)
        && let Ok(count) = count_str.parse::<u64>()
    {
        return Some(count);
    }

    None
}

fn parse_rejson_metrics(map: &HashMap<String, String>, rejson: &mut crate::metadata::stc::module::ReJSONInfo) {
    rejson.json_docs = parse_default(map.get("json_docs"));
    rejson.json_memory_usage = parse_default(map.get("json_memory_usage"));
    rejson.json_paths = parse_default(map.get("json_paths"));
}

fn parse_timeseries_metrics(map: &HashMap<String, String>, timeseries: &mut crate::metadata::stc::module::TimeSeriesInfo) {
    timeseries.ts_num_series = parse_default(map.get("ts_num_series"));
    timeseries.ts_num_samples = parse_default(map.get("ts_num_samples"));
    timeseries.ts_memory_usage = parse_default(map.get("ts_memory_usage"));
    timeseries.ts_num_chunks = parse_default(map.get("ts_num_chunks"));
    timeseries.ts_num_chunks_disk = parse_default(map.get("ts_num_chunks_disk"));
    timeseries.ts_duplicate_samples = parse_default(map.get("ts_duplicate_samples"));
}

fn parse_redisbloom_metrics(map: &HashMap<String, String>, redisbloom: &mut crate::metadata::stc::module::RedisBloomInfo) {
    redisbloom.bf_num_filters = parse_default(map.get("bf_num_filters"));
    redisbloom.bf_memory_usage = parse_default(map.get("bf_memory_usage"));
    redisbloom.cf_num_filters = parse_default(map.get("cf_num_filters"));
    redisbloom.cf_memory_usage = parse_default(map.get("cf_memory_usage"));
    redisbloom.cms_num_sketches = parse_default(map.get("cms_num_sketches"));
    redisbloom.cms_memory_usage = parse_default(map.get("cms_memory_usage"));
    redisbloom.topk_num_sketches = parse_default(map.get("topk_num_sketches"));
    redisbloom.topk_memory_usage = parse_default(map.get("topk_memory_usage"));
}

fn parse_redisgraph_metrics(map: &HashMap<String, String>, redisgraph: &mut crate::metadata::stc::module::RedisGraphInfo) {
    redisgraph.graph_num_graphs = parse_default(map.get("graph_num_graphs"));
    redisgraph.graph_num_nodes = parse_default(map.get("graph_num_nodes"));
    redisgraph.graph_num_relationships = parse_default(map.get("graph_num_relationships"));
    redisgraph.graph_memory_usage = parse_default(map.get("graph_memory_usage"));
    redisgraph.graph_queries_executed = parse_default(map.get("graph_queries_executed"));
    redisgraph.graph_query_execution_time_ms = parse_default(map.get("graph_query_execution_time_ms"));
    redisgraph.graph_cached_queries = parse_default(map.get("graph_cached_queries"));
}
