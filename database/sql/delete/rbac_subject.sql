WITH tombstone AS (
    -- Persist subject-wide deletion marker first.
    INSERT INTO rbac_subject_tombstones (org_uuid, subject_kind, subject_uuid, version_ms, version_seq, updated_at)
    VALUES ($1, $2, $3, $4, $5, NOW())
    ON CONFLICT (org_uuid, subject_kind, subject_uuid)
    DO UPDATE SET
        version_ms = EXCLUDED.version_ms,
        version_seq = EXCLUDED.version_seq,
        updated_at = NOW()
    WHERE (rbac_subject_tombstones.version_ms, rbac_subject_tombstones.version_seq) < (EXCLUDED.version_ms, EXCLUDED.version_seq)
    RETURNING 1
)
-- Apply soft-delete to currently existing rows for the same subject scope.
UPDATE rbac
SET is_active = FALSE,
    version_ms = $4,
    version_seq = $5,
    updated_at = NOW()
WHERE EXISTS (SELECT 1 FROM tombstone)
  AND org_uuid = $1
  AND subject_kind = $2
  AND subject_uuid = $3
  AND (version_ms, version_seq) < ($4, $5);
