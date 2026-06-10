CREATE TABLE IF NOT EXISTS llm_org_pii_dictionary (
    organization_uuid TEXT PRIMARY KEY REFERENCES organizations(uuid) ON DELETE CASCADE,
    terms TEXT NOT NULL DEFAULT '[]',
    updated_at TEXT NOT NULL DEFAULT (datetime('now'))
);
