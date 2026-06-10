SELECT subject_uuid
FROM rbac_data
WHERE org_uuid = $1
  AND endpoint_uuid = $2
  AND subject_kind = $3
  AND subject_uuid = ANY($4)
  AND is_active = TRUE
LIMIT 1;
