ALTER TABLE analytics.llm_operation_rollups
    ADD COLUMN IF NOT EXISTS estimated_arbitrage_savings_micros_sum SimpleAggregateFunction(sum, UInt64) AFTER estimated_cost_micros_sum,
    ADD COLUMN IF NOT EXISTS estimated_cache_savings_micros_sum SimpleAggregateFunction(sum, UInt64) AFTER estimated_arbitrage_savings_micros_sum,
    ADD COLUMN IF NOT EXISTS cache_hit_count SimpleAggregateFunction(sum, UInt64) AFTER error_count,
    ADD COLUMN IF NOT EXISTS cache_miss_count SimpleAggregateFunction(sum, UInt64) AFTER cache_hit_count,
    ADD COLUMN IF NOT EXISTS arbitrage_switch_count SimpleAggregateFunction(sum, UInt64) AFTER cache_miss_count
