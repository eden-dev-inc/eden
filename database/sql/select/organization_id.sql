SELECT o.id,
       o."uuid"::text,
       array_remove(array_agg(DISTINCT u_admin.username), NULL)            as admin_ids,
       array_remove(array_agg(DISTINCT o_adm.user_uuid), NULL)             as admin_uuids,
       array_remove(array_agg(DISTINCT a.id), NULL)                        as api_ids,
       array_remove(array_agg(DISTINCT o_api.api_uuid), NULL)              as api_uuids,
       array_remove(array_agg(DISTINCT en.id), NULL)                       as eden_node_ids,
       array_remove(array_agg(DISTINCT o_eden_nodes.eden_node_uuid), NULL) as eden_node_uuids,
       array_remove(array_agg(DISTINCT ep.id), NULL)                       as endpoint_ids,
       array_remove(array_agg(DISTINCT o_ep.endpoint_uuid), NULL)          as endpoint_uuids,
       array_remove(array_agg(DISTINCT eg.id), NULL)                       as endpoint_group_ids,
       array_remove(array_agg(DISTINCT o_eg.endpoint_group_uuid), NULL)    as endpoint_group_uuids,
       array_remove(array_agg(DISTINCT it.id), NULL)                       as interlay_ids,
       array_remove(array_agg(DISTINCT o_it.interlay_uuid), NULL)          as interlay_uuids,
       array_remove(array_agg(DISTINCT r.username), NULL)                  as robot_ids,
       array_remove(array_agg(DISTINCT o_robots.robot_uuid), NULL)         as robot_uuids,
       array_remove(array_agg(DISTINCT t.id), NULL)                        as template_ids,
       array_remove(array_agg(DISTINCT o_templates.template_uuid), NULL)   as template_uuids,
       array_remove(array_agg(DISTINCT u.username), NULL)                  as user_ids,
       array_remove(array_agg(DISTINCT o_users.user_uuid), NULL)           as user_uuids,
       array_remove(array_agg(DISTINCT w.id), NULL)                        as workflow_ids,
       array_remove(array_agg(DISTINCT o_wf.workflow_uuid), NULL)          as workflow_uuids,
       o."description",
       o.rate_limit_settings,
       o.created_at,
       o.updated_at
FROM organizations o
         LEFT JOIN organization_admins o_adm ON o.uuid = o_adm.organization_uuid
         LEFT JOIN users u_admin ON o_adm.user_uuid = u_admin.uuid
         LEFT JOIN organization_apis o_api ON o.uuid = o_api.organization_uuid
         LEFT JOIN apis a ON o_api.api_uuid = a.uuid
         LEFT JOIN organization_eden_nodes o_eden_nodes ON o.uuid = o_eden_nodes.organization_uuid
         LEFT JOIN eden_nodes en ON o_eden_nodes.eden_node_uuid = en.uuid
         LEFT JOIN organization_endpoints o_ep ON o.uuid = o_ep.organization_uuid
         LEFT JOIN endpoints ep ON o_ep.endpoint_uuid = ep.uuid
         LEFT JOIN organization_endpoint_groups o_eg ON o.uuid = o_eg.organization_uuid
         LEFT JOIN endpoint_groups eg ON o_eg.endpoint_group_uuid = eg.uuid
         LEFT JOIN organization_interlays o_it ON o.uuid = o_it.interlay_uuid
         LEFT JOIN interlays it ON o_it.interlay_uuid = it.uuid
         LEFT JOIN organization_robots o_robots ON o.uuid = o_robots.organization_uuid
         LEFT JOIN robots r ON o_robots.robot_uuid = r.uuid
         LEFT JOIN organization_templates o_templates ON o.uuid = o_templates.organization_uuid
         LEFT JOIN templates t ON o_templates.template_uuid = t.uuid
         LEFT JOIN organization_users o_users ON o.uuid = o_users.organization_uuid
         LEFT JOIN users u ON o_users.user_uuid = u.uuid
         LEFT JOIN organization_workflows o_wf ON o.uuid = o_wf.organization_uuid
         LEFT JOIN workflows w ON o_wf.workflow_uuid = w.uuid
WHERE o.id = $1
GROUP BY o.id, o.uuid, o.description, o.rate_limit_settings, o.created_at, o.updated_at;
