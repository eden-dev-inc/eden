SELECT 1
FROM organization_users
WHERE organization_uuid = $1
  AND user_uuid = $2
LIMIT 1
