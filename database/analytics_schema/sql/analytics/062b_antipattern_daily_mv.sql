-- Daily anti-pattern summary materialized view

CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.antipattern_daily_mv
TO analytics.antipattern_daily
AS SELECT
    toDate(detected_at) AS day,
    organization_uuid,
    endpoint_uuid,
    pattern_type,
    sum(occurrence_count) AS occurrence_count,
    uniqExactState(connection_id) AS unique_connections
FROM analytics.anti_patterns
GROUP BY day, organization_uuid, endpoint_uuid, pattern_type
