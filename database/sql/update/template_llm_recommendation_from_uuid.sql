UPDATE templates
SET llm_recommendation = $2, updated_at = $3, updated_by = COALESCE($4, updated_by)
WHERE uuid = $1;
