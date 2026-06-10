-- Paginated list of users with optional status and exact permission filters.
-- Parameters:
--   $1: org_uuid        - Organization UUID (required)
--   $2: cursor_created  - Cursor timestamp for pagination
--   $3: cursor_uuid     - Cursor UUID for pagination
--   $4: status_filter   - 'active' | 'deleted' | NULL (no filter)
--   $5: perms_filter    - exact control-plane permission string (for example 'RG') | NULL
--   $6: limit           - Max rows to return

SELECT u.*
FROM users u
LEFT JOIN rbac_control r
    ON r.org_uuid = u.organization_uuid
    AND r.entity_kind = 'org'
    AND r.subject_kind = 'user'
    AND r.subject_uuid = u.uuid
    AND r.is_active = TRUE
WHERE u.organization_uuid = $1
  -- Cursor-based pagination
  AND (u.created_at, u.uuid) < ($2::timestamptz, $3::uuid)
  -- Status filter
  AND (
    $4::text IS NULL
    OR ($4::text = 'active' AND r.perms IS NOT NULL)
    OR ($4::text = 'deleted' AND r.perms IS NULL)
  )
  -- Exact perms filter
  AND (
    $5::text IS NULL
    OR r.perms = $5::text
  )
ORDER BY u.created_at DESC, u.uuid DESC
LIMIT $6;
