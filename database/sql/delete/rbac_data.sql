INSERT INTO rbac_data (org_uuid, endpoint_uuid, subject_kind, subject_uuid, perms, is_active, version_ms, version_seq, updated_at)
VALUES ($1::UUID, $2::UUID, $3::VARCHAR(32), $4::UUID, '', FALSE, $5::BIGINT, $6::BIGINT, NOW())
ON CONFLICT (org_uuid, endpoint_uuid, subject_kind, subject_uuid)
DO UPDATE SET
    is_active = FALSE,
    version_ms = EXCLUDED.version_ms,
    version_seq = EXCLUDED.version_seq,
    updated_at = NOW()
WHERE (rbac_data.version_ms, rbac_data.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq);
