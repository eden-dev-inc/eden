WITH tombstone AS (
    -- Persist entity-wide deletion marker first.
    INSERT INTO rbac_entity_tombstones (org_uuid, entity_kind, entity_uuid, version_ms, version_seq, updated_at)
    VALUES ($1, $2, $3, $4, $5, NOW())
    ON CONFLICT (org_uuid, entity_kind, entity_uuid)
    DO UPDATE SET
        version_ms = EXCLUDED.version_ms,
        version_seq = EXCLUDED.version_seq,
        updated_at = NOW()
    WHERE (rbac_entity_tombstones.version_ms, rbac_entity_tombstones.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq)
    RETURNING 1
)
-- Apply soft-delete to currently existing rows for the same entity scope.
UPDATE rbac
SET is_active = FALSE,
    version_ms = $4,
    version_seq = $5,
    updated_at = NOW()
WHERE EXISTS (SELECT 1 FROM tombstone)
  AND org_uuid = $1
  AND entity_kind = $2
  AND entity_uuid = $3
  AND (version_ms, version_seq) < ($4, $5);
