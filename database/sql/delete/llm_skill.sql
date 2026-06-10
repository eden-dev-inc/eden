DELETE FROM llm_skills
WHERE id = $1
  AND organization_uuid IS NOT DISTINCT FROM $2
RETURNING id;
