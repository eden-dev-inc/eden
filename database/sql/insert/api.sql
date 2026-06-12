WITH
-- Create or update the api
api_insert AS (
    INSERT INTO apis (
                      id,
                      uuid,
                      "description",
                      fields,
                      bindings,
                      response_logic,
                      created_by,
                      updated_by,
                      created_at,
                      updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
        ON CONFLICT (id) DO UPDATE
            SET fields = EXCLUDED.fields,
                "description" = EXCLUDED.description,
                bindings = EXCLUDED.bindings,
                response_logic = EXCLUDED.response_logic,
                updated_by = EXCLUDED.updated_by,
                updated_at = EXCLUDED.updated_at
        RETURNING uuid),
-- Link api to organization using returned uuid from upsert
org_link AS (
    INSERT INTO organization_apis (organization_uuid, api_uuid)
    SELECT $11, api_insert.uuid
    FROM api_insert
    ON CONFLICT (organization_uuid, api_uuid) DO NOTHING
)
-- Return the actual api UUID from the upsert
SELECT uuid FROM api_insert;
