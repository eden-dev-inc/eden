-- Note: uuid and id already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX IF NOT EXISTS idx_workflow_updated_at ON workflows (updated_at);
