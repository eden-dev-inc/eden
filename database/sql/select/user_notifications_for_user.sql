SELECT id, user_uuid, organization_uuid, kind, category, severity, title, body, action_url, action_label, read, created_at
FROM user_notifications
WHERE user_uuid = $1
ORDER BY created_at DESC
LIMIT $2
