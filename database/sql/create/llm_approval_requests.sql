CREATE TABLE IF NOT EXISTS llm_approval_requests (
    id UUID PRIMARY KEY,
    run_id UUID NOT NULL,
    organization_uuid UUID NOT NULL,
    requested_by UUID NOT NULL,
    plan JSONB NOT NULL,
    state TEXT NOT NULL DEFAULT 'pending',
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + INTERVAL '24 hours'),
    delegated_to UUID,
    required_approvals INTEGER NOT NULL DEFAULT 1,
    approval_count INTEGER NOT NULL DEFAULT 0,
    change_window_start TIMESTAMP WITH TIME ZONE,
    change_window_end TIMESTAMP WITH TIME ZONE,
    justification TEXT,
    decided_by UUID,
    decided_at TIMESTAMP WITH TIME ZONE,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);
