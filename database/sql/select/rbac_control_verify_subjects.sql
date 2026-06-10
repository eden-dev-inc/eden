SELECT subject_uuid
FROM rbac_control
WHERE org_uuid = $1
  AND entity_kind = $2
  AND entity_uuid = $3
  AND subject_kind = $4
  AND subject_uuid = ANY($5)
  AND is_active = TRUE
LIMIT 1;
