SELECT eg.*
FROM endpoint_groups eg
         JOIN organization_endpoint_groups oeg ON eg.uuid = oeg.endpoint_group_uuid
WHERE oeg.organization_uuid = $1;
