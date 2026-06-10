-- Note: uuid and id already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX IF NOT EXISTS idx_eden_node_updated_at ON eden_nodes (updated_at);
