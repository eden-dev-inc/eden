SELECT endpoint_uuid, perms
FROM rbac_data
WHERE org_uuid = $1
  AND subject_kind = $2
  AND subject_uuid = $3
  AND is_active = TRUE;
