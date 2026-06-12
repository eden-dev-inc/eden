ALTER TABLE analytics.llm_operation_events
    ADD COLUMN IF NOT EXISTS route_optimization_mode LowCardinality(String) DEFAULT 'cost' AFTER estimated_cache_savings_micros,
    ADD COLUMN IF NOT EXISTS kv_cache_mode LowCardinality(String) DEFAULT 'disabled' AFTER route_optimization_mode,
    ADD COLUMN IF NOT EXISTS kv_cache_status LowCardinality(String) DEFAULT 'bypass' AFTER kv_cache_mode,
    ADD COLUMN IF NOT EXISTS estimated_kv_cache_savings_micros UInt64 DEFAULT 0 AFTER kv_cache_status,
    ADD COLUMN IF NOT EXISTS route_move_reason LowCardinality(String) DEFAULT '' AFTER estimated_kv_cache_savings_micros,
    ADD COLUMN IF NOT EXISTS conversation_route_key String DEFAULT '' AFTER route_move_reason
