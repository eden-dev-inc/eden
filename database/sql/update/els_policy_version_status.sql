-- Transition a version status atomically with a guard on the expected
-- current status ($4). Returns the updated row so callers can detect
-- when the transition was a no-op (invalid source state / race).
UPDATE els_policy_versions
SET status = $3
WHERE policy_uuid = $1 AND version = $2 AND status = $4
RETURNING version;
