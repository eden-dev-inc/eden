INSERT INTO user_notifications (id, user_uuid, organization_uuid, kind, category, severity, title, body, action_url, action_label)
VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
