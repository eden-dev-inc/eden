WITH tombstone AS (
    INSERT INTO rbac_data_entity_tombstones (org_uuid, endpoint_uuid, version_ms, version_seq, updated_at)
    VALUES ($1::UUID, $2::UUID, $3::BIGINT, $4::BIGINT, NOW())
    ON CONFLICT (org_uuid, endpoint_uuid)
    DO UPDATE SET
        version_ms = EXCLUDED.version_ms,
        version_seq = EXCLUDED.version_seq,
        updated_at = NOW()
    WHERE (rbac_data_entity_tombstones.version_ms, rbac_data_entity_tombstones.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq)
    RETURNING 1
)
UPDATE rbac_data
SET is_active = FALSE,
    version_ms = $3::BIGINT,
    version_seq = $4::BIGINT,
    updated_at = NOW()
WHERE EXISTS (SELECT 1 FROM tombstone)
  AND org_uuid = $1::UUID
  AND endpoint_uuid = $2::UUID
  AND (version_ms, version_seq) < ($3::BIGINT, $4::BIGINT);
