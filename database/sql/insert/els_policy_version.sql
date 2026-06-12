-- Compute the next version number and insert atomically.
-- The PK constraint (policy_uuid, version) guarantees uniqueness. If two
-- concurrent transactions compute the same MAX(version)+1, one will fail
-- with a unique constraint violation — the caller should retry.
INSERT INTO els_policy_versions (policy_uuid, version, strategy, config, status, created_by, created_at)
VALUES ($1, (SELECT COALESCE(MAX(version), 0) + 1 FROM els_policy_versions WHERE policy_uuid = $1), $2, $3, 'draft', $4, NOW())
RETURNING version;
