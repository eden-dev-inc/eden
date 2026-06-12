CREATE TABLE IF NOT EXISTS organization_interlays
(
    organization_uuid UUID,
    interlay_uuid     UUID,
    PRIMARY KEY (organization_uuid, interlay_uuid),
    FOREIGN KEY (organization_uuid) REFERENCES organizations (uuid),
    FOREIGN KEY (interlay_uuid) REFERENCES interlays (uuid)
);