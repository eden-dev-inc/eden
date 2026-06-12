-- Note: uuid and id already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX IF NOT EXISTS idx_auth_updated_at ON auths (updated_at);
