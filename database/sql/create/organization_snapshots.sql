CREATE TABLE IF NOT EXISTS organization_snapshots (
    organization_uuid UUID,
    snapshot_uuid UUID,
    PRIMARY KEY (organization_uuid, snapshot_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations(uuid),
    FOREIGN KEY (snapshot_uuid) REFERENCES snapshots(uuid)
);
