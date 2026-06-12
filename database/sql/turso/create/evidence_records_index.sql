CREATE INDEX IF NOT EXISTS idx_evidence_records_run_id
    ON evidence_records (run_id, step_index);

CREATE INDEX IF NOT EXISTS idx_evidence_records_kind
    ON evidence_records (kind, created_at DESC);
