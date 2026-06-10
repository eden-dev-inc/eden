WITH
    org AS (
        SELECT uuid as organization_uuid
        FROM organizations
        WHERE uuid = $19 FOR SHARE
    ),
    snapshot_insert AS (
        INSERT INTO snapshots (
            id, uuid, description, status, source_endpoint, target_endpoint,
            data, preserve_ttl, schedule,
            source_mode, filter, cdc_config, last_lsn, write_template_uuid, read_template_uuid,
            created_by, updated_by, created_at, updated_at
        )
        VALUES (
            $1, $2, $3, $4, $5, $6,
            $7, $8, $9,
            $10, $11, $12, $13, $14, $15,
            $16, $17, $18, $18
        ) ON CONFLICT (id) DO UPDATE
        SET
            uuid = EXCLUDED.uuid,
            description = EXCLUDED.description,
            status = EXCLUDED.status,
            source_endpoint = EXCLUDED.source_endpoint,
            target_endpoint = EXCLUDED.target_endpoint,
            data = EXCLUDED.data,
            preserve_ttl = EXCLUDED.preserve_ttl,
            schedule = EXCLUDED.schedule,
            source_mode = EXCLUDED.source_mode,
            filter = EXCLUDED.filter,
            cdc_config = EXCLUDED.cdc_config,
            write_template_uuid = EXCLUDED.write_template_uuid,
            read_template_uuid = EXCLUDED.read_template_uuid,
            updated_by = EXCLUDED.updated_by,
            updated_at = NOW()
        RETURNING uuid
    ),
    org_link AS (
        INSERT INTO organization_snapshots (organization_uuid, snapshot_uuid)
        SELECT org.organization_uuid, snapshot_insert.uuid
        FROM org, snapshot_insert
        ON CONFLICT (organization_uuid, snapshot_uuid) DO NOTHING
    )
SELECT uuid FROM snapshot_insert
