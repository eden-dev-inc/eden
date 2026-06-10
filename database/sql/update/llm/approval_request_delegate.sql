UPDATE llm_approval_requests
SET delegated_to = $2
WHERE id = $1
  AND state = 'pending'
RETURNING id, run_id, organization_uuid, requested_by, plan, state, expires_at, delegated_to,
          required_approvals, approval_count, change_window_start, change_window_end,
          justification, decided_by, decided_at, created_at
