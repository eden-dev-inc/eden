UPDATE execution_runs
SET state = $2,
    updated_at = NOW()
WHERE id = $1
  AND state = ANY($3::text[])
RETURNING id, state
