UPDATE llm_approval_requests
SET state = $2,
    decided_by = $3,
    decided_at = NOW()
WHERE id = $1
  AND state = 'pending'
RETURNING id, run_id, organization_uuid, requested_by, plan, state, decided_by, decided_at, created_at
