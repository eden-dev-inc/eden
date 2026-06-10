WITH
-- Create or update the interlay
interlay_insert AS (
    INSERT INTO interlays (
                           id,
                           uuid,
                           "description",
                           endpoint,
                           port,
                           listeners,
                           advertise_host,
                           tls,
                           settings,
                           created_by,
                           updated_by,
                           created_at,
                           updated_at
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        ON CONFLICT (id) DO UPDATE
            SET endpoint = EXCLUDED.endpoint,
                "description" = EXCLUDED.description,
                port = EXCLUDED.port,
                listeners = EXCLUDED.listeners,
                advertise_host = EXCLUDED.advertise_host,
                tls = EXCLUDED.tls,
                settings = EXCLUDED.settings,
                updated_by = EXCLUDED.updated_by,
                updated_at = EXCLUDED.updated_at
        RETURNING uuid),
-- Link interlay to organization using returned uuid from upsert
org_link AS (
    INSERT INTO organization_interlays (organization_uuid, interlay_uuid)
    SELECT $14, interlay_insert.uuid
    FROM interlay_insert
    ON CONFLICT (organization_uuid, interlay_uuid) DO NOTHING
)
-- Return the actual interlay UUID from the upsert
SELECT uuid FROM interlay_insert;
