ALTER TABLE execution_runs ADD COLUMN IF NOT EXISTS total_tokens INTEGER;
ALTER TABLE execution_runs ADD COLUMN IF NOT EXISTS total_cost_usd NUMERIC(12, 6);
