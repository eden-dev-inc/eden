CREATE INDEX IF NOT EXISTS idx_llm_approval_requests_org_state_created
    ON llm_approval_requests (organization_uuid, state, created_at DESC);

CREATE UNIQUE INDEX IF NOT EXISTS idx_llm_approval_requests_run
    ON llm_approval_requests (run_id);
