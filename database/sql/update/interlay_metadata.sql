UPDATE interlays
SET description = COALESCE($2, description),
    settings    = COALESCE($3, settings),
    updated_by  = $4,
    updated_at  = $5
WHERE uuid = $1;
