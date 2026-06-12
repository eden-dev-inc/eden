INSERT INTO llm_org_pii_dictionary (
    organization_uuid,
    terms,
    updated_at
) VALUES (
    $1,
    $2,
    NOW()
)
ON CONFLICT (organization_uuid) DO UPDATE
SET
    terms = EXCLUDED.terms,
    updated_at = EXCLUDED.updated_at
RETURNING
    organization_uuid,
    terms,
    updated_at;
