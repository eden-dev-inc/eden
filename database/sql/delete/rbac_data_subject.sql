WITH tombstone AS (
    INSERT INTO rbac_data_subject_tombstones (org_uuid, subject_kind, subject_uuid, version_ms, version_seq, updated_at)
    VALUES ($1::UUID, $2::VARCHAR(32), $3::UUID, $4::BIGINT, $5::BIGINT, NOW())
    ON CONFLICT (org_uuid, subject_kind, subject_uuid)
    DO UPDATE SET
        version_ms = EXCLUDED.version_ms,
        version_seq = EXCLUDED.version_seq,
        updated_at = NOW()
    WHERE (rbac_data_subject_tombstones.version_ms, rbac_data_subject_tombstones.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq)
    RETURNING 1
)
UPDATE rbac_data
SET is_active = FALSE,
    version_ms = $4::BIGINT,
    version_seq = $5::BIGINT,
    updated_at = NOW()
WHERE EXISTS (SELECT 1 FROM tombstone)
  AND org_uuid = $1::UUID
  AND subject_kind = $2::VARCHAR(32)
  AND subject_uuid = $3::UUID
  AND (version_ms, version_seq) < ($4::BIGINT, $5::BIGINT);
