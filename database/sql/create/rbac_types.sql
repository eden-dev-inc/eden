DO $$
BEGIN
    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typname = 'rbac_entity_kind'
          AND n.nspname = current_schema()
    ) THEN
        CREATE DOMAIN rbac_entity_kind AS VARCHAR(32)
            CHECK (VALUE IN ('org', 'endpoint', 'workflow', 'template', 'api', 'robot', 'eden_node'));
    END IF;

    IF NOT EXISTS (
        SELECT 1 FROM pg_type t
        JOIN pg_namespace n ON t.typnamespace = n.oid
        WHERE t.typname = 'rbac_subject_kind'
          AND n.nspname = current_schema()
    ) THEN
        CREATE DOMAIN rbac_subject_kind AS VARCHAR(32)
            CHECK (VALUE IN ('user', 'robot'));
    END IF;
END $$;
