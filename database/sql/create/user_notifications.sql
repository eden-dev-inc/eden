CREATE TABLE IF NOT EXISTS user_notifications (
    id UUID PRIMARY KEY,
    user_uuid UUID NOT NULL,
    organization_uuid UUID NOT NULL,
    kind TEXT NOT NULL,
    category TEXT NOT NULL,
    severity TEXT NOT NULL DEFAULT 'info',
    title TEXT NOT NULL,
    body TEXT NOT NULL,
    action_url TEXT,
    action_label TEXT,
    read BOOLEAN NOT NULL DEFAULT false,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

-- kind: system_update, new_service, recommendation, security, billing, maintenance, feature
-- category: general, endpoints, analytics, security, billing, features
-- severity: info, warning, critical
