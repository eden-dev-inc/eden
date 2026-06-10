INSERT INTO llm_approval_requests (
    id,
    run_id,
    organization_uuid,
    requested_by,
    plan,
    state,
    expires_at,
    delegated_to,
    required_approvals,
    approval_count,
    change_window_start,
    change_window_end,
    justification,
    decided_by,
    decided_at
)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14, $15)
