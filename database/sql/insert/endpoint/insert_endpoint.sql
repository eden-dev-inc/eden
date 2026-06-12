INSERT INTO endpoints (id, uuid, kind, config, routing, description, created_by, updated_by, created_at, updated_at)
VALUES ($1, $2, $3, $4::BYTEA, $5::JSONB, $6, $7, $8, $9, $10)
RETURNING uuid;
