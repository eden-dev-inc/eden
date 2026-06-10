SELECT uuid as organization_uuid
FROM organizations
WHERE uuid = $1
    FOR SHARE;