-- Note: uuid and id already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX IF NOT EXISTS idx_endpoint_updated_at ON endpoints (updated_at);
