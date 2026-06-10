-- Note: uuid and id already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX idx_apis_updated_at ON apis (updated_at);

