-- Associate the admin with the organization
INSERT INTO organization_admins (organization_uuid, user_uuid)
VALUES ($1, $2);
