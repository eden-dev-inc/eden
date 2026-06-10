DELETE FROM endpoints WHERE uuid = $1 RETURNING uuid;
