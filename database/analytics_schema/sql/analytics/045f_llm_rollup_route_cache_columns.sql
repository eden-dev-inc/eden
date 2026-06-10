ALTER TABLE analytics.llm_operation_rollups
    ADD COLUMN IF NOT EXISTS estimated_kv_cache_savings_micros_sum SimpleAggregateFunction(sum, UInt64) AFTER estimated_cache_savings_micros_sum,
    ADD COLUMN IF NOT EXISTS kv_cache_hit_count SimpleAggregateFunction(sum, UInt64) AFTER cache_miss_count,
    ADD COLUMN IF NOT EXISTS route_move_count SimpleAggregateFunction(sum, UInt64) AFTER kv_cache_hit_count
