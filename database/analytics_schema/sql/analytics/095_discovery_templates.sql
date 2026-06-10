-- Discovery template table: reverse-engineered access patterns from discovery windows.
-- Stores template results from DocumentExtractor + TemplateGenerator pipeline.

CREATE TABLE IF NOT EXISTS analytics.discovery_templates
(
    -- Time
    discovered_at DateTime64(3, 'UTC'),

    -- Identity
    organization_uuid String CODEC(ZSTD(3)),
    endpoint_uuid String CODEC(ZSTD(3)),

    -- Template info
    template_name String CODEC(ZSTD(3)),
    template_pattern String CODEC(ZSTD(3)),
    sample_count UInt64,
    unique_commands UInt32,

    -- Cluster info
    cluster_id UInt32,
    cluster_size UInt32,

    -- Representative sample
    representative_commands String CODEC(ZSTD(3))
)
ENGINE = ReplacingMergeTree(discovered_at)
PARTITION BY toYYYYMM(discovered_at)
ORDER BY (organization_uuid, endpoint_uuid, template_name)
TTL toDateTime(discovered_at) + INTERVAL 90 DAY;
