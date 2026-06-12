ALTER TABLE execution_runs ADD COLUMN IF NOT EXISTS idempotency_key TEXT;
