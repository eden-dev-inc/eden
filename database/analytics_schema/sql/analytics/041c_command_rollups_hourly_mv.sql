-- Materialized view: automatically roll up 60-second command rollups into hourly buckets.
-- Uses explicit column list to avoid alias-resolution issues in ClickHouse 23.3.

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.command_rollups_hourly_mv
TO analytics.command_rollups_hourly AS
SELECT
    toStartOfHour(window_start) AS window_start,
    toUInt16(3600) AS window_secs,
    organization_uuid,
    endpoint_uuid,
    protocol,
    service,
    command_id,
    command,
    category,
    sum(request_count) AS request_count,
    sum(success_count) AS success_count,
    sum(error_count) AS error_count,
    sum(slow_count) AS slow_count,
    sum(dangerous_count) AS dangerous_count,
    sum(write_command_count) AS write_command_count,
    sum(latency_sum) AS latency_sum,
    sum(latency_sample_count) AS latency_sample_count,
    sum(latency_sample_sum_us) AS latency_sample_sum_us,
    sum(latency_sample_sumsq_us2) AS latency_sample_sumsq_us2,
    min(latency_min) AS latency_min,
    max(latency_max) AS latency_max,
    sumForEach(latency_histogram) AS latency_histogram,
    sum(request_bytes_sum) AS request_bytes_sum,
    sum(response_bytes_sum) AS response_bytes_sum,
    sumForEach(request_size_histogram) AS request_size_histogram,
    sumForEach(response_size_histogram) AS response_size_histogram,
    sum(target_count_sum) AS target_count_sum,
    sum(cost_sum) AS cost_sum,
    sum(cache_hit_count) AS cache_hit_count,
    sum(cache_miss_count) AS cache_miss_count,
    sum(redirect_count) AS redirect_count,
    sum(server_error_count) AS server_error_count,
    sum(client_error_count) AS client_error_count,
    sum(bandwidth_cost) AS bandwidth_cost
FROM analytics.command_rollups
GROUP BY
    window_start,
    window_secs,
    organization_uuid, endpoint_uuid, protocol, service, command_id, command, category;
