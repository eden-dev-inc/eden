-- Note: uuid and id already indexed via PRIMARY KEY and UNIQUE constraints
CREATE INDEX idx_interlays_updated_at ON interlays (updated_at);
CREATE UNIQUE INDEX idx_interlays_port_unique ON interlays (port) WHERE port IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_interlays_listeners_gin ON interlays USING gin (listeners jsonb_path_ops) WHERE listeners IS NOT NULL;
