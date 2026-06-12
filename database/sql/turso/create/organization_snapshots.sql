CREATE TABLE IF NOT EXISTS organization_snapshots (
    organization_uuid TEXT,
    snapshot_uuid TEXT,
    PRIMARY KEY (organization_uuid, snapshot_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (snapshot_uuid) REFERENCES snapshots(uuid)
);
