SELECT subject_kind, subject_uuid, perms
FROM rbac_control
WHERE org_uuid = $1
  AND entity_kind = $2
  AND entity_uuid = $3
  AND is_active = TRUE;
