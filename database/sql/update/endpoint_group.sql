UPDATE endpoint_groups
SET id = COALESCE($2, id),
    description = COALESCE($3, description),
    default_endpoint = COALESCE($4, default_endpoint),
    updated_by = $5,
    updated_at = $6
WHERE uuid = $1;
