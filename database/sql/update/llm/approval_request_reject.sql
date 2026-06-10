UPDATE llm_approval_requests
SET state = 'rejected',
    justification = COALESCE($3, justification),
    decided_by = $2,
    decided_at = CURRENT_TIMESTAMP
WHERE id = $1
  AND state = 'pending'
RETURNING id, run_id, organization_uuid, requested_by, plan, state, expires_at, delegated_to,
          required_approvals, approval_count, change_window_start, change_window_end,
          justification, decided_by, decided_at, created_at
