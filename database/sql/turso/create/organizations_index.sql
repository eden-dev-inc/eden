-- Note: uuid and id already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX IF NOT EXISTS idx_organization_updated_at ON organizations (updated_at);
