SELECT subject_kind, subject_uuid, perms
FROM rbac_data
WHERE org_uuid = $1
  AND endpoint_uuid = $2
  AND is_active = TRUE;
