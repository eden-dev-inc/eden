SELECT
    s.id, s.uuid, s.description, s.status,
    s.source_endpoint, s.target_endpoint,
    s.data, s.preserve_ttl, s.schedule,
    s.last_run_at, s.next_run_at, s.job_uuid,
    s.source_mode, s.filter, s.cdc_config, s.last_lsn, s.write_template_uuid, s.read_template_uuid,
    s.created_by, s.updated_by, s.created_at, s.updated_at,
    os.organization_uuid
FROM snapshots s
JOIN organization_snapshots os ON s.uuid = os.snapshot_uuid
WHERE s.schedule IS NOT NULL
  AND (s.schedule->>'enabled')::boolean = true
  AND s.next_run_at <= NOW()
  AND s.status != 'Running'
  AND (s.source_mode IS NULL OR s.source_mode != 'cdc');
