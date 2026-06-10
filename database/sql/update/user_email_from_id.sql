UPDATE users
SET email = $2,
    updated_at = $3,
    updated_by = $4
WHERE username = $1;
