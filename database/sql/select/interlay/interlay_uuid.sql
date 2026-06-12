SELECT *
FROM interlays
WHERE uuid = $1
LIMIT 1;