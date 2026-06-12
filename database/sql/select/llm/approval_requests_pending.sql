SELECT id, run_id, organization_uuid, requested_by, plan, state, expires_at, delegated_to,
       required_approvals, approval_count, change_window_start, change_window_end,
       justification, decided_by, decided_at, created_at
FROM llm_approval_requests
WHERE organization_uuid = $1
  AND state = 'pending'
ORDER BY created_at DESC
LIMIT $2
