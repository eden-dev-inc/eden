CREATE TABLE IF NOT EXISTS analytics_dashboard_prefs (
    user_uuid TEXT NOT NULL,
    organization_uuid TEXT NOT NULL,
    prefs TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (user_uuid, organization_uuid)
);
