CREATE TABLE IF NOT EXISTS trigger_sources (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    organization_uuid UUID NOT NULL,
    name TEXT NOT NULL,
    source_type TEXT NOT NULL,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    hmac_secret TEXT,
    is_active BOOLEAN NOT NULL DEFAULT true,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
