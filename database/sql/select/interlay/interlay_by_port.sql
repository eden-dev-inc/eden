SELECT a.*
FROM interlays a
WHERE a.port = $1
   OR a.listeners @> jsonb_build_array(jsonb_build_object('bind_port', $1))
LIMIT 1;
