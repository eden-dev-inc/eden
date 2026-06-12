SELECT a.*
FROM unnest($1::int[]) AS requested(port)
JOIN interlays a
  ON a.port = requested.port
  OR a.listeners @> jsonb_build_array(jsonb_build_object('bind_port', requested.port))
LIMIT 1;
