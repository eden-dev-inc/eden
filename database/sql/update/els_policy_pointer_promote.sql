UPDATE els_policy_pointers
SET active_version = $2, activated_by = $3, activated_at = NOW()
WHERE policy_uuid = $1
  AND (active_version = $4
       OR (active_version IS NULL AND $4 IS NULL))
RETURNING active_version;
