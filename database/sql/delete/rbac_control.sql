INSERT INTO rbac_control (org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid, perms, is_active, version_ms, version_seq, updated_at)
VALUES ($1::UUID, $2::VARCHAR(32), $3::UUID, $4::VARCHAR(32), $5::UUID, '', FALSE, $6::BIGINT, $7::BIGINT, NOW())
ON CONFLICT (org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid)
DO UPDATE SET
    is_active = FALSE,
    version_ms = EXCLUDED.version_ms,
    version_seq = EXCLUDED.version_seq,
    updated_at = NOW()
WHERE (rbac_control.version_ms, rbac_control.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq);
