-- Create or update the eden_node
INSERT INTO eden_nodes (id, uuid, description, created_at, updated_at)
VALUES ($1, $2, $3, $4, $5)
ON CONFLICT (id) DO UPDATE
SET description = EXCLUDED.description,
    updated_at = EXCLUDED.updated_at
RETURNING uuid;
