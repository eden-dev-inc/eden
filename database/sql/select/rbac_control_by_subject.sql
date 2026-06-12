SELECT entity_kind, entity_uuid, perms
FROM rbac_control
WHERE org_uuid = $1
  AND subject_kind = $2
  AND subject_uuid = $3
  AND is_active = TRUE;
