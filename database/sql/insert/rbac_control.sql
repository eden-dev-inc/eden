INSERT INTO rbac_control (org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid, perms, is_active, version_ms, version_seq, updated_at)
SELECT $1::UUID, $2::VARCHAR(32), $3::UUID, $4::VARCHAR(32), $5::UUID, $6::VARCHAR(8), TRUE, $7::BIGINT, $8::BIGINT, NOW()
WHERE NOT EXISTS (
        SELECT 1
        FROM rbac_control_row_tombstones rt
        WHERE rt.org_uuid = $1::UUID
          AND rt.entity_kind = $2::VARCHAR(32)
          AND rt.entity_uuid = $3::UUID
          AND rt.subject_kind = $4::VARCHAR(32)
          AND rt.subject_uuid = $5::UUID
          AND (rt.version_ms, rt.version_seq) >= ($7::BIGINT, $8::BIGINT)
    )
  AND NOT EXISTS (
        SELECT 1
        FROM rbac_control_subject_tombstones st
        WHERE st.org_uuid = $1::UUID
          AND st.subject_kind = $4::VARCHAR(32)
          AND st.subject_uuid = $5::UUID
          AND (st.version_ms, st.version_seq) >= ($7::BIGINT, $8::BIGINT)
    )
  AND NOT EXISTS (
        SELECT 1
        FROM rbac_control_entity_tombstones et
        WHERE et.org_uuid = $1::UUID
          AND et.entity_kind = $2::VARCHAR(32)
          AND et.entity_uuid = $3::UUID
          AND (et.version_ms, et.version_seq) >= ($7::BIGINT, $8::BIGINT)
    )
ON CONFLICT (org_uuid, entity_kind, entity_uuid, subject_kind, subject_uuid)
DO UPDATE SET
    perms = EXCLUDED.perms,
    is_active = TRUE,
    version_ms = EXCLUDED.version_ms,
    version_seq = EXCLUDED.version_seq,
    updated_at = NOW()
WHERE (rbac_control.version_ms, rbac_control.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq);
