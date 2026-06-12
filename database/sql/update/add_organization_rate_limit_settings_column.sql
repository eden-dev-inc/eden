ALTER TABLE organizations
    ADD COLUMN IF NOT EXISTS rate_limit_settings JSONB DEFAULT NULL;
