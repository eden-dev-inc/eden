ALTER TABLE analytics.llm_operation_events
    ADD COLUMN IF NOT EXISTS requested_provider LowCardinality(String) DEFAULT '' AFTER estimated_cost_micros,
    ADD COLUMN IF NOT EXISTS requested_model String DEFAULT '' AFTER requested_provider,
    ADD COLUMN IF NOT EXISTS baseline_estimated_cost_micros UInt64 DEFAULT 0 AFTER requested_model,
    ADD COLUMN IF NOT EXISTS selected_estimated_cost_micros UInt64 DEFAULT 0 AFTER baseline_estimated_cost_micros,
    ADD COLUMN IF NOT EXISTS estimated_arbitrage_savings_micros UInt64 DEFAULT 0 AFTER selected_estimated_cost_micros,
    ADD COLUMN IF NOT EXISTS arbitrage_reason LowCardinality(String) DEFAULT '' AFTER estimated_arbitrage_savings_micros,
    ADD COLUMN IF NOT EXISTS price_source LowCardinality(String) DEFAULT '' AFTER arbitrage_reason,
    ADD COLUMN IF NOT EXISTS cache_status LowCardinality(String) DEFAULT 'bypass' AFTER price_source,
    ADD COLUMN IF NOT EXISTS estimated_cache_savings_micros UInt64 DEFAULT 0 AFTER cache_status
