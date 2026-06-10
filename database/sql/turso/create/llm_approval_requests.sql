CREATE TABLE IF NOT EXISTS llm_approval_requests (
    id TEXT PRIMARY KEY,
    run_id TEXT NOT NULL,
    organization_uuid TEXT NOT NULL,
    requested_by TEXT NOT NULL,
    plan TEXT NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    expires_at TEXT DEFAULT (datetime('now', '+24 hours')),
    delegated_to TEXT,
    required_approvals INTEGER NOT NULL DEFAULT 1,
    approval_count INTEGER NOT NULL DEFAULT 0,
    change_window_start TEXT,
    change_window_end TEXT,
    justification TEXT,
    decided_by TEXT,
    decided_at TEXT,
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
