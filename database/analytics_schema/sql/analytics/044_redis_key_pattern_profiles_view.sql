-- Query-time view for per-key-pattern profiles (convenience for ad-hoc exploration).
-- Not materialized: computes on every query.

CREATE OR REPLACE VIEW analytics.redis_key_pattern_profiles AS
SELECT
    organization_uuid,
    endpoint_uuid,
    target_pattern,
    sum(request_count)          AS total_requests,
    sum(read_count)             AS total_reads,
    sum(write_count)            AS total_writes,
    sum(error_count)            AS total_errors,
    sum(cost_sum)               AS total_cost,
    sum(latency_sum)            AS total_latency,
    sum(value_bytes_sum)        AS total_value_bytes,
    sum(ttl_present_count)      AS total_ttl_present,
    sum(bandwidth_cost)         AS total_bandwidth_kb,
    min(window_start)           AS first_seen,
    max(window_start)           AS last_seen,
    uniqExact(window_start)     AS observation_windows,
    arraySort(groupUniqArray(command)) AS commands_used,
    -- Derived metrics
    if(sum(read_count) + sum(write_count) > 0,
       sum(write_count) / (sum(read_count) + sum(write_count)),
       0) AS write_ratio,
    if(sum(request_count) > 0,
       sum(value_bytes_sum) / sum(request_count),
       0) AS avg_value_bytes,
    if(sum(write_count) > 0,
       sum(ttl_present_count) / sum(write_count),
       0) AS ttl_coverage,
    if(sum(request_count) > 0,
       sum(latency_sum) / sum(request_count),
       0) AS avg_latency_us,
    if(sum(request_count) > 0,
       sum(error_count) * 1.0 / sum(request_count),
       0) AS error_rate
FROM analytics.target_pattern_rollups
GROUP BY organization_uuid, endpoint_uuid, target_pattern;
