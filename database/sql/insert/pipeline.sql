WITH
    org AS (
        SELECT uuid as organization_uuid
        FROM organizations
        WHERE uuid = $13 FOR SHARE
    ),
    pipeline_insert AS (
        INSERT INTO pipelines (
            id, uuid, description, status, source_endpoint, target_endpoint,
            filter, cdc_config, last_lsn, write_template_uuid, read_template_uuid,
            created_by, updated_by, created_at, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9, $10, $11,
            $12, $12, NOW(), NOW()
        ) ON CONFLICT (id) DO UPDATE
        SET
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            source_endpoint = EXCLUDED.source_endpoint,
            target_endpoint = EXCLUDED.target_endpoint,
            filter = EXCLUDED.filter,
            cdc_config = EXCLUDED.cdc_config,
            write_template_uuid = EXCLUDED.write_template_uuid,
            read_template_uuid = EXCLUDED.read_template_uuid,
            updated_by = EXCLUDED.updated_by,
            updated_at = EXCLUDED.updated_at
        RETURNING uuid
    ),
    org_link AS (
        INSERT INTO organization_pipelines (organization_uuid, pipeline_uuid)
        SELECT org.organization_uuid, pipeline_insert.uuid
        FROM org, pipeline_insert
        ON CONFLICT (organization_uuid, pipeline_uuid) DO NOTHING
    )
SELECT uuid FROM pipeline_insert
