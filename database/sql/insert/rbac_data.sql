INSERT INTO rbac_data (org_uuid, endpoint_uuid, subject_kind, subject_uuid, perms, is_active, version_ms, version_seq, updated_at)
SELECT $1::UUID, $2::UUID, $3::VARCHAR(32), $4::UUID, $5::VARCHAR(4), TRUE, $6::BIGINT, $7::BIGINT, NOW()
WHERE NOT EXISTS (
        SELECT 1
        FROM rbac_data_row_tombstones rt
        WHERE rt.org_uuid = $1::UUID
          AND rt.endpoint_uuid = $2::UUID
          AND rt.subject_kind = $3::VARCHAR(32)
          AND rt.subject_uuid = $4::UUID
          AND (rt.version_ms, rt.version_seq) >= ($6::BIGINT, $7::BIGINT)
    )
  AND NOT EXISTS (
        SELECT 1
        FROM rbac_data_subject_tombstones st
        WHERE st.org_uuid = $1::UUID
          AND st.subject_kind = $3::VARCHAR(32)
          AND st.subject_uuid = $4::UUID
          AND (st.version_ms, st.version_seq) >= ($6::BIGINT, $7::BIGINT)
    )
  AND NOT EXISTS (
        SELECT 1
        FROM rbac_data_entity_tombstones et
        WHERE et.org_uuid = $1::UUID
          AND et.endpoint_uuid = $2::UUID
          AND (et.version_ms, et.version_seq) >= ($6::BIGINT, $7::BIGINT)
    )
ON CONFLICT (org_uuid, endpoint_uuid, subject_kind, subject_uuid)
DO UPDATE SET
    perms = EXCLUDED.perms,
    is_active = TRUE,
    version_ms = EXCLUDED.version_ms,
    version_seq = EXCLUDED.version_seq,
    updated_at = NOW()
WHERE (rbac_data.version_ms, rbac_data.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq);
