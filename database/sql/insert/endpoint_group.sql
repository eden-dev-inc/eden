WITH
-- Create or update the endpoint group
endpoint_group_insert AS (
    INSERT INTO endpoint_groups (
                      id,
                      uuid,
                      description,
                      ep_kind,
                      default_endpoint,
                      created_by,
                      updated_by,
                      created_at,
                      updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)
        ON CONFLICT (id) DO UPDATE
            SET description = EXCLUDED.description,
                ep_kind = EXCLUDED.ep_kind,
                default_endpoint = EXCLUDED.default_endpoint,
                updated_by = EXCLUDED.updated_by,
                updated_at = EXCLUDED.updated_at
        RETURNING uuid),
-- Link endpoint group to organization using returned uuid from upsert
org_link AS (
    INSERT INTO organization_endpoint_groups (organization_uuid, endpoint_group_uuid)
    SELECT $10, endpoint_group_insert.uuid
    FROM endpoint_group_insert
    ON CONFLICT (organization_uuid, endpoint_group_uuid) DO NOTHING
)
-- Return the actual endpoint group UUID from the upsert
SELECT uuid FROM endpoint_group_insert;
