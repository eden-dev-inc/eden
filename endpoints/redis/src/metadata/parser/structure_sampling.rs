use crate::command;
use crate::metadata::parser::common::{execute_command, execute_command_raw, mark_endpoint_response, value_to_map, value_to_vec};
use crate::metadata::stc::structure_sampling::{RedisAttributeSample, RedisPatternSample, RedisStructureSamples};
use chrono::Utc;
use deadpool::managed::Object;
use endpoint_types::metadata::{CapabilityChecker, CapabilityId};
use error::{EpError, ResultEP};
use redis::{FromRedisValue, Value};
use redis_core::{RedisAsync, RedisConnectionManager};
use std::collections::{HashMap, HashSet};
use telemetry::TelemetryWrapper;
use tracing::warn;

const SCAN_BUDGET: usize = 10_000;
const SCAN_BATCH: usize = 500;
const MAX_PATTERNS: usize = 1_024;
const PER_PATTERN_SAMPLE_CAP: usize = 64;
const PER_ATTRIBUTE_VALUE_CAP: usize = 16;
const VALUE_BYTE_CAP: usize = 128;
const JSON_ATTRIBUTE_NAME_CAP: usize = 16;

#[derive(Default)]
struct PatternAccumulator {
    keys: Vec<String>,
}

#[derive(Default)]
struct AttributeAccumulator {
    presence_count: u32,
    sample_values: Vec<String>,
}

#[derive(Default)]
struct SampleAccumulator {
    sample_size: u32,
    ttl_millis_samples: Vec<i64>,
    size_bytes_samples: Vec<u32>,
    attributes: HashMap<String, AttributeAccumulator>,
    kind_counts: HashMap<String, u32>,
}

struct KeyInspection {
    value_kind: String,
    ttl_millis: i64,
    size_bytes: u32,
    attributes: Vec<(String, Option<String>)>,
}

pub async fn load_structure_samples(
    context: RedisAsync,
    telemetry: &mut TelemetryWrapper,
    capabilities: &dyn CapabilityChecker,
) -> ResultEP<RedisStructureSamples> {
    let _span = telemetry.client_tracer("redis.structure_sampling.load".to_string());
    let mut connection = context.get().await.map_err(|err| EpError::database(err.to_string()))?;
    let (candidate_keys, scan_budget_used, truncated) = scan_candidate_keys(&mut connection, telemetry).await?;
    let (pattern_buckets, dropped_patterns) = bucket_keys(candidate_keys);

    if dropped_patterns > 0 {
        warn!(
            "redis structure sampling dropped {} distinct patterns after hitting the {}-pattern cap",
            dropped_patterns, MAX_PATTERNS
        );
    }

    let rejson_loaded = capabilities.has(&CapabilityId("redis.module.rejson"));
    let distinct_patterns_observed = u32::try_from(pattern_buckets.len()).unwrap_or(u32::MAX);
    let mut sample_count = 0u32;
    let mut patterns = Vec::with_capacity(pattern_buckets.len());
    let mut buckets: Vec<(String, PatternAccumulator)> = pattern_buckets.into_iter().collect();
    buckets.sort_by(|left, right| left.0.cmp(&right.0));

    for (raw_pattern, accumulator) in buckets {
        let pattern_sample = process_pattern(&mut connection, telemetry, &raw_pattern, accumulator.keys, rejson_loaded).await;
        sample_count = sample_count.saturating_add(pattern_sample.sample_size);
        patterns.push(pattern_sample);
    }

    patterns.sort_by(|left, right| right.sample_size.cmp(&left.sample_size).then_with(|| left.raw_pattern.cmp(&right.raw_pattern)));

    Ok(RedisStructureSamples {
        sampled_at_unix_secs: Utc::now().timestamp().try_into().unwrap_or_default(),
        sample_count,
        scan_budget_used: u32::try_from(scan_budget_used).unwrap_or(u32::MAX),
        distinct_patterns_observed,
        patterns,
        truncated,
    })
}

async fn scan_candidate_keys(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
) -> ResultEP<(Vec<String>, usize, bool)> {
    let mut cursor = "0".to_string();
    let mut keys = Vec::new();
    let mut scan_budget_used = 0usize;

    loop {
        let mut cmd = command::cmd("SCAN");
        cmd.arg(&cursor).arg("COUNT").arg(SCAN_BATCH);
        let response = execute_command_raw(connection, cmd).await;
        mark_endpoint_response(telemetry);
        let response = response?;

        let parts =
            value_to_vec(&response).ok_or_else(|| EpError::metadata("Invalid SCAN response: expected a top-level array".to_string()))?;
        if parts.len() < 2 {
            return Err(EpError::metadata("Invalid SCAN response: expected cursor and key list".to_string()));
        }

        let next_cursor = value_to_owned_string(&parts[0])
            .ok_or_else(|| EpError::metadata("Invalid SCAN response: cursor was not string-like".to_string()))?;
        let batch =
            value_to_vec(&parts[1]).ok_or_else(|| EpError::metadata("Invalid SCAN response: key list was not an array".to_string()))?;

        let remaining_budget = SCAN_BUDGET.saturating_sub(scan_budget_used);
        let batch_budget = batch.len().min(remaining_budget);
        scan_budget_used = scan_budget_used.saturating_add(batch_budget);

        for key in batch.into_iter().take(batch_budget) {
            if let Some(key) = value_to_owned_string(&key) {
                keys.push(key);
            }
        }

        if next_cursor == "0" {
            return Ok((keys, scan_budget_used, false));
        }

        if scan_budget_used >= SCAN_BUDGET {
            warn!("redis structure sampling hit the SCAN budget of {} keys before cursor completion", SCAN_BUDGET);
            return Ok((keys, scan_budget_used, true));
        }

        cursor = next_cursor;
    }
}

fn bucket_keys(candidate_keys: Vec<String>) -> (HashMap<String, PatternAccumulator>, usize) {
    let mut buckets: HashMap<String, PatternAccumulator> = HashMap::new();
    let mut dropped_patterns = 0usize;

    for key in candidate_keys {
        let pattern = raw_pattern(&key);

        if let Some(existing) = buckets.get_mut(&pattern) {
            if existing.keys.len() < PER_PATTERN_SAMPLE_CAP {
                existing.keys.push(key);
            }
            continue;
        }

        if buckets.len() >= MAX_PATTERNS {
            dropped_patterns = dropped_patterns.saturating_add(1);
            continue;
        }

        buckets.insert(pattern, PatternAccumulator { keys: vec![key] });
    }

    (buckets, dropped_patterns)
}

async fn process_pattern(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
    raw_pattern_key: &str,
    keys: Vec<String>,
    rejson_loaded: bool,
) -> RedisPatternSample {
    let mut sample = SampleAccumulator::default();
    let mut warned_for_pattern = false;

    for key in keys {
        match inspect_key(connection, telemetry, &key, rejson_loaded).await {
            Ok(inspection) => {
                sample.sample_size = sample.sample_size.saturating_add(1);
                if sample.ttl_millis_samples.len() < PER_PATTERN_SAMPLE_CAP {
                    sample.ttl_millis_samples.push(inspection.ttl_millis);
                }
                if sample.size_bytes_samples.len() < PER_PATTERN_SAMPLE_CAP {
                    sample.size_bytes_samples.push(inspection.size_bytes);
                }
                *sample.kind_counts.entry(inspection.value_kind).or_insert(0) += 1;
                merge_attributes(&mut sample.attributes, inspection.attributes);
            }
            Err(err) => {
                if !warned_for_pattern {
                    warn!("redis structure sampling skipped one or more keys for pattern '{}': {}", raw_pattern_key, err);
                    warned_for_pattern = true;
                }
            }
        }
    }

    let mut attributes: Vec<RedisAttributeSample> = sample
        .attributes
        .into_iter()
        .map(|(name, accumulator)| RedisAttributeSample {
            name,
            presence_count: accumulator.presence_count,
            sample_values: accumulator.sample_values,
        })
        .collect();
    attributes.sort_by(|left, right| right.presence_count.cmp(&left.presence_count).then_with(|| left.name.cmp(&right.name)));

    RedisPatternSample {
        raw_pattern: raw_pattern_key.to_string(),
        value_kind: dominant_kind(&sample.kind_counts),
        sample_size: sample.sample_size,
        ttl_millis_samples: sample.ttl_millis_samples,
        size_bytes_samples: sample.size_bytes_samples,
        attributes,
    }
}

async fn inspect_key(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
    key: &str,
    rejson_loaded: bool,
) -> ResultEP<KeyInspection> {
    let mut type_cmd = command::cmd("TYPE");
    type_cmd.arg(key);
    let raw_type = execute_typed_with_mark::<String>(connection, telemetry, type_cmd).await?;
    let value_kind = normalize_value_kind(&raw_type);

    let mut pttl_cmd = command::cmd("PTTL");
    pttl_cmd.arg(key);
    let ttl_millis = execute_typed_with_mark::<i64>(connection, telemetry, pttl_cmd).await?;

    let mut memory_cmd = command::cmd("MEMORY");
    memory_cmd.arg("USAGE").arg(key);
    let memory_response = execute_raw_with_mark(connection, telemetry, memory_cmd).await;
    let size_bytes = match memory_response {
        Ok(Value::Int(value)) => u32::try_from(value).unwrap_or(u32::MAX),
        Ok(_) => 0,
        Err(_) => 0,
    };

    let attributes = match value_kind.as_str() {
        "hash" => collect_hash_attributes(connection, telemetry, key).await?,
        "stream" => collect_stream_attributes(connection, telemetry, key).await?,
        "rejson" if rejson_loaded => collect_rejson_attributes(connection, telemetry, key).await?,
        _ => Vec::new(),
    };

    Ok(KeyInspection { value_kind, ttl_millis, size_bytes, attributes })
}

async fn collect_hash_attributes(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
    key: &str,
) -> ResultEP<Vec<(String, Option<String>)>> {
    let mut cmd = command::cmd("HRANDFIELD");
    cmd.arg(key).arg(16).arg("WITHVALUES");
    let response = execute_raw_with_mark(connection, telemetry, cmd).await?;
    let pairs = value_to_vec(&response)
        .ok_or_else(|| EpError::metadata(format!("Invalid HRANDFIELD response for key '{}': expected array", key)))?;

    let mut attributes = Vec::new();
    let mut iter = pairs.into_iter();
    while let (Some(name), Some(value)) = (iter.next(), iter.next()) {
        let Some(name) = value_to_owned_string(&name) else {
            continue;
        };
        attributes.push((name, value_to_owned_string(&value)));
    }

    Ok(attributes)
}

async fn collect_stream_attributes(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
    key: &str,
) -> ResultEP<Vec<(String, Option<String>)>> {
    let mut cmd = command::cmd("XINFO");
    cmd.arg("STREAM").arg(key);
    let response = execute_raw_with_mark(connection, telemetry, cmd).await?;
    let Some(field_names) = extract_stream_field_names(&response) else {
        return Ok(Vec::new());
    };

    Ok(field_names.into_iter().map(|name| (name, None)).collect())
}

async fn collect_rejson_attributes(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
    key: &str,
) -> ResultEP<Vec<(String, Option<String>)>> {
    let mut cmd = command::cmd("JSON.OBJKEYS");
    cmd.arg(key).arg("$");
    let response = execute_raw_with_mark(connection, telemetry, cmd).await?;

    let mut names = Vec::new();
    collect_string_values(&response, &mut names, JSON_ATTRIBUTE_NAME_CAP);

    Ok(names.into_iter().take(JSON_ATTRIBUTE_NAME_CAP).map(|name| (name, None)).collect())
}

async fn execute_typed_with_mark<T: FromRedisValue>(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
    cmd: crate::command::Cmd,
) -> ResultEP<T> {
    let result = execute_command(connection, cmd).await;
    mark_endpoint_response(telemetry);
    result
}

async fn execute_raw_with_mark(
    connection: &mut Object<RedisConnectionManager>,
    telemetry: &mut TelemetryWrapper,
    cmd: crate::command::Cmd,
) -> ResultEP<Value> {
    let result = execute_command_raw(connection, cmd).await;
    mark_endpoint_response(telemetry);
    result
}

fn merge_attributes(target: &mut HashMap<String, AttributeAccumulator>, attributes: Vec<(String, Option<String>)>) {
    let mut seen_in_key = HashSet::new();

    for (name, sample_value) in attributes {
        let entry = target.entry(name.clone()).or_default();
        if seen_in_key.insert(name.clone()) {
            entry.presence_count = entry.presence_count.saturating_add(1);
        }

        let Some(sample_value) = sample_value else {
            continue;
        };
        if entry.sample_values.len() >= PER_ATTRIBUTE_VALUE_CAP {
            continue;
        }

        let truncated = truncate_utf8_bytes(&sample_value, VALUE_BYTE_CAP);
        if entry.sample_values.iter().any(|existing| existing == &truncated) {
            continue;
        }
        entry.sample_values.push(truncated);
    }
}

fn dominant_kind(kind_counts: &HashMap<String, u32>) -> String {
    let mut kinds: Vec<(&String, &u32)> = kind_counts.iter().collect();
    kinds.sort_by(|left, right| right.1.cmp(left.1).then_with(|| left.0.cmp(right.0)));
    kinds.first().map(|(kind, _)| (*kind).clone()).unwrap_or_else(|| "other".to_string())
}

fn normalize_value_kind(raw_type: &str) -> String {
    match raw_type.to_ascii_lowercase().as_str() {
        "string" => "string",
        "list" => "list",
        "hash" => "hash",
        "set" => "set",
        "zset" => "zset",
        "stream" => "stream",
        "rejson-rl" | "rejson" | "json" => "rejson",
        "none" => "other",
        _ => "other",
    }
    .to_string()
}

fn value_to_owned_string(value: &Value) -> Option<String> {
    match value {
        Value::BulkString(bytes) => Some(String::from_utf8_lossy(bytes).into_owned()),
        Value::SimpleString(value) => Some(value.clone()),
        Value::Okay => Some("OK".to_string()),
        Value::Int(value) => Some(value.to_string()),
        Value::Double(value) => Some(value.to_string()),
        Value::Boolean(value) => Some(value.to_string()),
        Value::VerbatimString { text, .. } => Some(text.clone()),
        Value::BigNumber(value) => Some(value.to_string()),
        _ => None,
    }
}

fn extract_stream_field_names(response: &Value) -> Option<Vec<String>> {
    let top_level = value_to_map(response)?;
    let last_entry = top_level.get("last-entry")?;

    if let Some(fields) = parse_stream_entry_fields(last_entry) {
        return Some(fields);
    }

    let entry_map = value_to_map(last_entry)?;
    let fields = entry_map.get("fields")?;
    let fields = value_to_vec(fields)?;
    let mut names = Vec::new();

    let mut iter = fields.iter();
    while let Some(field_name) = iter.next() {
        if let Some(name) = value_to_owned_string(field_name) {
            names.push(name);
        }
        let _ = iter.next();
    }

    Some(names)
}

fn parse_stream_entry_fields(entry: &Value) -> Option<Vec<String>> {
    let parts = value_to_vec(entry)?;
    if parts.len() < 2 {
        return None;
    }

    let fields = value_to_vec(&parts[1])?;
    let mut names = Vec::new();
    let mut iter = fields.iter();
    while let Some(field_name) = iter.next() {
        if let Some(name) = value_to_owned_string(field_name) {
            names.push(name);
        }
        let _ = iter.next();
    }

    Some(names)
}

fn collect_string_values(value: &Value, output: &mut Vec<String>, cap: usize) {
    if output.len() >= cap {
        return;
    }

    if let Some(text) = value_to_owned_string(value) {
        output.push(text);
        return;
    }

    match value {
        Value::Array(values) | Value::Set(values) => {
            for item in values {
                collect_string_values(item, output, cap);
                if output.len() >= cap {
                    break;
                }
            }
        }
        Value::Map(entries) => {
            for (key, value) in entries {
                collect_string_values(key, output, cap);
                if output.len() >= cap {
                    break;
                }
                collect_string_values(value, output, cap);
                if output.len() >= cap {
                    break;
                }
            }
        }
        Value::Attribute { data, .. } => collect_string_values(data, output, cap),
        _ => {}
    }
}

fn truncate_utf8_bytes(value: &str, max_bytes: usize) -> String {
    if value.len() <= max_bytes {
        return value.to_string();
    }

    let mut end = max_bytes;
    while !value.is_char_boundary(end) {
        end -= 1;
    }
    value[..end].to_string()
}

fn raw_pattern(key: &str) -> String {
    let mut changed = false;
    let parts: Vec<String> = key
        .split(':')
        .map(|segment| {
            if segment.parse::<u64>().is_ok() || segment_is_hexish(segment) || segment_is_uuid_like(segment) {
                changed = true;
                "*".to_string()
            } else {
                segment.to_string()
            }
        })
        .collect();

    if changed { parts.join(":") } else { key.to_string() }
}

fn segment_is_hexish(segment: &str) -> bool {
    if !matches!(segment.len(), 32 | 36) {
        return false;
    }

    let cleaned: String = segment.chars().filter(|ch| *ch != '-').collect();
    cleaned.len() == 32 && cleaned.chars().all(|ch| ch.is_ascii_hexdigit())
}

fn segment_is_uuid_like(segment: &str) -> bool {
    segment.matches('-').count() == 4 && segment_is_hexish(segment)
}

#[cfg(test)]
mod tests {
    use super::raw_pattern;

    #[test]
    fn raw_pattern_replaces_integer_segments() {
        assert_eq!(raw_pattern("user:42:session"), "user:*:session");
    }

    #[test]
    fn raw_pattern_replaces_uuid_segments() {
        assert_eq!(raw_pattern("order:550e8400-e29b-41d4-a716-446655440000"), "order:*");
    }

    #[test]
    fn raw_pattern_leaves_date_like_segments() {
        assert_eq!(raw_pattern("events:2024-01-01"), "events:2024-01-01");
    }

    #[test]
    fn raw_pattern_leaves_short_hex_like_segments() {
        assert_eq!(raw_pattern("payment:abc123"), "payment:abc123");
    }

    #[test]
    fn raw_pattern_replaces_32_char_hex_segments() {
        assert_eq!(raw_pattern("cache:f47ac10b58cc4372a5670e02b2c3d479"), "cache:*");
    }
}
