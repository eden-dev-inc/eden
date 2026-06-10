-- Connection metrics snapshots
-- Periodic snapshots of endpoint, proxy, and client connection counts.
--
-- Deployments with older schemas must ALTER TABLE to add new columns
-- manually — ClickHouse's DDL pipeline does not support multi-statement
-- queries via the HTTP client used by this service.

CREATE TABLE IF NOT EXISTS analytics.connection_metrics
(
    snapshot_time DateTime64(3, 'UTC'),
    organization_uuid String CODEC(ZSTD(3)),

    -- Endpoint connections: total open (idle + in-use), from eden.connections gauge.
    endpoint_connections_total Int64,
    endpoint_connections_by_type Map(String, Int64),
    endpoint_connections_by_uuid Map(String, Int64),

    -- Endpoint connections currently checked out (in-use).
    endpoint_connections_in_use Int64,
    endpoint_connections_in_use_by_uuid Map(String, Int64),

    -- Proxy connections (client wire-protocol sessions to Eden).
    proxy_connections_total Int64,
    proxy_connections_by_endpoint Map(String, Int64),
    -- Client-side breakdown: key is `"client_ip|interlay_id"`.
    proxy_connections_by_client Map(String, Int64),

    -- Client connections (active HTTP requests to Eden service).
    active_requests Int64
)
ENGINE = MergeTree()
PARTITION BY toYYYYMM(snapshot_time)
ORDER BY (organization_uuid, snapshot_time)
TTL toDateTime(snapshot_time) + INTERVAL 90 DAY;
