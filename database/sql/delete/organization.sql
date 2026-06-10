WITH entity_uuids AS (
        SELECT
            array_agg(DISTINCT ap.uuid) as api_uuids,
            array_agg(DISTINCT w.uuid) as workflow_uuids,
            array_agg(DISTINCT t.uuid) as template_uuids,
            array_agg(DISTINCT e.uuid) as endpoint_uuids,
            array_agg(DISTINCT it.uuid) as interlay_uuids,
            array_agg(DISTINCT r.uuid) as robot_uuids,
            array_agg(DISTINCT u.uuid) as user_uuids
        FROM organizations o
        LEFT JOIN organization_apis oa_api ON o.uuid = oa_api.organization_uuid
        LEFT JOIN apis ap ON ap.uuid = oa_api.api_uuid
        LEFT JOIN organization_workflows ow ON o.uuid = ow.organization_uuid
        LEFT JOIN workflows w ON w.uuid = ow.workflow_uuid
        LEFT JOIN organization_templates ot ON o.uuid = ot.organization_uuid
        LEFT JOIN templates t ON t.uuid = ot.template_uuid
        LEFT JOIN organization_endpoints oe ON o.uuid = oe.organization_uuid
        LEFT JOIN endpoints e ON e.uuid = oe.endpoint_uuid
        LEFT JOIN organization_interlays o_it ON o.uuid = o_it.organization_uuid
        LEFT JOIN interlays it ON it.uuid = o_it.interlay_uuid
        LEFT JOIN organization_robots o_r ON o.uuid = o_r.organization_uuid
        LEFT JOIN robots r ON r.uuid = o_r.robot_uuid
        LEFT JOIN organization_users ou ON o.uuid = ou.organization_uuid
        LEFT JOIN users u ON u.uuid = ou.user_uuid
        WHERE o.uuid = $1
    ),
    -- Get auth UUIDs that are connected to the endpoints
    auth_uuids AS (
        SELECT array_agg(DISTINCT a.uuid) as auth_uuids
        FROM entity_uuids
        LEFT JOIN auths a ON a.endpoint_uuid = ANY(entity_uuids.endpoint_uuids)
    ),
    -- -- Capture entity info before deletion
    org_info AS (
        SELECT json_agg(json_build_object('id', id, 'uuid', uuid)) as data
        FROM organizations
        WHERE uuid = $1
    ),
    admin_info AS (
        SELECT json_agg(json_build_object('id', u.username, 'uuid', u.uuid)) as data
        FROM organization_admins oa
        JOIN users u ON u.uuid = oa.user_uuid
        WHERE oa.organization_uuid = $1
    ),
    api_info AS (
        SELECT json_agg(json_build_object('id', id, 'uuid', uuid)) as data
        FROM apis
        WHERE uuid = ANY((SELECT unnest(api_uuids) FROM entity_uuids))
    ),
    user_info AS (
        SELECT json_agg(json_build_object('id', u.username, 'uuid', u.uuid)) as data
        FROM users u
        WHERE u.uuid = ANY((SELECT unnest(user_uuids) FROM entity_uuids))
    ),
    workflow_info AS (
        SELECT json_agg(json_build_object('id', id, 'uuid', uuid)) as data
        FROM workflows
        WHERE uuid = ANY((SELECT unnest(workflow_uuids) FROM entity_uuids))
    ),
    template_info AS (
        SELECT json_agg(json_build_object('id', id, 'uuid', uuid)) as data
        FROM templates
        WHERE uuid = ANY((SELECT unnest(template_uuids) FROM entity_uuids))
    ),
    endpoint_info AS (
        SELECT json_agg(json_build_object('id', id, 'uuid', uuid)) as data
        FROM endpoints
        WHERE uuid = ANY((SELECT unnest(endpoint_uuids) FROM entity_uuids))
    ),
    interlay_info AS (
        SELECT json_agg(json_build_object('id', id, 'uuid', uuid)) as data
        FROM interlays
        WHERE uuid = ANY((SELECT unnest(interlay_uuids) FROM entity_uuids))
    ),
    robot_info AS (
        SELECT json_agg(json_build_object('id', username, 'uuid', uuid)) as data
        FROM robots
        WHERE uuid = ANY((SELECT unnest(robot_uuids) FROM entity_uuids))
    ),
    auth_info AS (
        SELECT json_agg(json_build_object('id', id, 'uuid', uuid)) as data
        FROM auths
        WHERE uuid = ANY((SELECT unnest(auth_uuids) FROM auth_uuids))
    ),
    -- Batch deletes using the collected UUIDs
    -- Delete relationships first
    deleted_workflow_templates AS (
        DELETE FROM workflow_templates
        WHERE workflow_uuid = ANY((SELECT unnest(workflow_uuids) FROM entity_uuids))
    ),
    deleted_eden_node_endpoints AS (
        DELETE FROM eden_node_endpoints
        WHERE endpoint_uuid = ANY((SELECT unnest(endpoint_uuids) FROM entity_uuids))
    ),
    -- Delete entities
    deleted_workflows AS (
        DELETE FROM workflows
        WHERE uuid = ANY((SELECT unnest(workflow_uuids) FROM entity_uuids))
    ),
    deleted_templates AS (
        DELETE FROM templates
        WHERE uuid = ANY((SELECT unnest(template_uuids) FROM entity_uuids))
    ),
    deleted_auths AS (
        DELETE FROM auths
        WHERE uuid = ANY((SELECT unnest(auth_uuids) FROM auth_uuids))
    ),
    deleted_endpoints AS (
        DELETE FROM endpoints
        WHERE uuid = ANY((SELECT unnest(endpoint_uuids) FROM entity_uuids))
    ),
    deleted_interlays AS (
        DELETE FROM interlays
        WHERE uuid = ANY((SELECT unnest(interlay_uuids) FROM entity_uuids))
    ),
    deleted_apis AS (
        DELETE FROM apis
        WHERE uuid = ANY((SELECT unnest(api_uuids) FROM entity_uuids))
    ),
    deleted_robots AS (
        DELETE FROM robots
        WHERE uuid = ANY((SELECT unnest(robot_uuids) FROM entity_uuids))
    ),
    -- Delete organization relationships
    deleted_apis_rel AS (DELETE FROM organization_apis WHERE organization_uuid = $1),
    deleted_workflows_rel AS (DELETE FROM organization_workflows WHERE organization_uuid = $1),
    deleted_templates_rel AS (DELETE FROM organization_templates WHERE organization_uuid = $1),
    deleted_endpoints_rel AS (DELETE FROM organization_endpoints WHERE organization_uuid = $1),
    deleted_interlays_rel AS (DELETE FROM organization_interlays WHERE organization_uuid = $1),
    deleted_robots_rel AS (DELETE FROM organization_robots WHERE organization_uuid = $1),
    deleted_eden_nodes_rel AS (DELETE FROM organization_eden_nodes WHERE organization_uuid = $1),
    deleted_admins_rel AS (DELETE FROM organization_admins WHERE organization_uuid = $1),
    deleted_users_rel AS (DELETE FROM organization_users WHERE organization_uuid = $1),
    -- Delete users
    deleted_users AS (
        DELETE FROM users
        WHERE uuid = ANY((SELECT unnest(user_uuids) FROM entity_uuids))
    ),
    -- Finally delete organization
    deleted_org AS (
        DELETE FROM organizations
        WHERE uuid = $1
    )
    -- Return all collected info
SELECT json_build_object(
    'organization', COALESCE((SELECT data FROM org_info), '[]'::json),
    'apis', COALESCE((SELECT data FROM api_info), '[]'::json),
    'users', COALESCE((SELECT data FROM user_info), '[]'::json),
    'workflows', COALESCE((SELECT data FROM workflow_info), '[]'::json),
    'templates', COALESCE((SELECT data FROM template_info), '[]'::json),
    'endpoints', COALESCE((SELECT data FROM endpoint_info), '[]'::json),
    'interlays', COALESCE((SELECT data FROM interlay_info), '[]'::json),
    'robots', COALESCE((SELECT data FROM robot_info), '[]'::json),
    'auths', COALESCE((SELECT data FROM auth_info), '[]'::json)
) as organization_uuids;
