UPDATE users
SET display_name = $2,
    updated_at = $3,
    updated_by = $4
WHERE username = $1;
