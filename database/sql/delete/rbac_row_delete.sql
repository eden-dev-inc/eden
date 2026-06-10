WITH control_tombstone AS (
    INSERT INTO rbac_control_row_tombstones (org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid, version_ms, version_seq, updated_at)
    VALUES ($1::UUID, $2::VARCHAR(32), $3::UUID, $4::VARCHAR(32), $5::UUID, $6::BIGINT, $7::BIGINT, NOW())
    ON CONFLICT (org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid)
    DO UPDATE SET
        version_ms = EXCLUDED.version_ms,
        version_seq = EXCLUDED.version_seq,
        updated_at = NOW()
    WHERE (rbac_control_row_tombstones.version_ms, rbac_control_row_tombstones.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq)
    RETURNING 1
),
control_delete AS (
    UPDATE rbac_control
    SET is_active = FALSE,
        version_ms = $6::BIGINT,
        version_seq = $7::BIGINT,
        updated_at = NOW()
    WHERE EXISTS (SELECT 1 FROM control_tombstone)
      AND org_uuid = $1::UUID
      AND entity_kind = $2::VARCHAR(32)
      AND entity_uuid = $3::UUID
      AND subject_kind = $4::VARCHAR(32)
      AND subject_uuid = $5::UUID
      AND (version_ms, version_seq) < ($6::BIGINT, $7::BIGINT)
    RETURNING 1
),
data_tombstone AS (
    INSERT INTO rbac_data_row_tombstones (org_uuid, endpoint_uuid, subject_kind, subject_uuid, version_ms, version_seq, updated_at)
    SELECT $1::UUID, $3::UUID, $4::VARCHAR(32), $5::UUID, $6::BIGINT, $7::BIGINT, NOW()
    WHERE $2::VARCHAR(32) = 'endpoint'
    ON CONFLICT (org_uuid, endpoint_uuid, subject_kind, subject_uuid)
    DO UPDATE SET
        version_ms = EXCLUDED.version_ms,
        version_seq = EXCLUDED.version_seq,
        updated_at = NOW()
    WHERE (rbac_data_row_tombstones.version_ms, rbac_data_row_tombstones.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq)
    RETURNING 1
),
data_delete AS (
    UPDATE rbac_data
    SET is_active = FALSE,
        version_ms = $6::BIGINT,
        version_seq = $7::BIGINT,
        updated_at = NOW()
    WHERE EXISTS (SELECT 1 FROM data_tombstone)
      AND org_uuid = $1::UUID
      AND endpoint_uuid = $3::UUID
      AND subject_kind = $4::VARCHAR(32)
      AND subject_uuid = $5::UUID
      AND (version_ms, version_seq) < ($6::BIGINT, $7::BIGINT)
    RETURNING 1
)
SELECT 1;
