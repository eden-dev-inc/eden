-- Note: uuid and username already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX IF NOT EXISTS idx_user_updated_at ON users (updated_at);
