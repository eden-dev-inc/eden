SELECT
    p.id, p.uuid, p.description, p.status,
    p.source_endpoint, p.target_endpoint,
    p.filter, p.cdc_config, p.last_lsn, p.write_template_uuid, p.read_template_uuid,
    p.created_by, p.updated_by, p.created_at, p.updated_at
FROM pipelines p
JOIN organization_pipelines op ON p.uuid = op.pipeline_uuid
WHERE op.organization_uuid = $1;
