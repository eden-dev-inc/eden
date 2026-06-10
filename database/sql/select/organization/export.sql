-- Export all data belonging to an organization for transfer.
-- Parameter: $1 = organization UUID
-- Returns a single JSON object containing full rows for all org-scoped entities.
WITH entity_uuids AS (
    SELECT
        array_agg(DISTINCT w.uuid) FILTER (WHERE w.uuid IS NOT NULL) as workflow_uuids,
        array_agg(DISTINCT t.uuid) FILTER (WHERE t.uuid IS NOT NULL) as template_uuids,
        array_agg(DISTINCT e.uuid) FILTER (WHERE e.uuid IS NOT NULL) as endpoint_uuids,
        array_agg(DISTINCT u.uuid) FILTER (WHERE u.uuid IS NOT NULL) as user_uuids
    FROM organizations o
    LEFT JOIN organization_workflows ow ON o.uuid = ow.organization_uuid
    LEFT JOIN workflows w ON w.uuid = ow.workflow_uuid
    LEFT JOIN organization_templates ot ON o.uuid = ot.organization_uuid
    LEFT JOIN templates t ON t.uuid = ot.template_uuid
    LEFT JOIN organization_endpoints oe ON o.uuid = oe.organization_uuid
    LEFT JOIN endpoints e ON e.uuid = oe.endpoint_uuid
    LEFT JOIN organization_users ou ON o.uuid = ou.organization_uuid
    LEFT JOIN users u ON u.uuid = ou.user_uuid
    WHERE o.uuid = $1
),
auth_uuids AS (
    SELECT array_agg(DISTINCT a.uuid) FILTER (WHERE a.uuid IS NOT NULL) as auth_uuids
    FROM entity_uuids
    LEFT JOIN auths a ON a.endpoint_uuid = ANY(entity_uuids.endpoint_uuids)
),
-- Full row exports using row_to_json
org_data AS (
    SELECT COALESCE(json_agg(row_to_json(o)), '[]'::json) as data
    FROM organizations o
    WHERE o.uuid = $1
),
user_data AS (
    SELECT COALESCE(json_agg(row_to_json(u)), '[]'::json) as data
    FROM users u, entity_uuids eu
    WHERE u.uuid = ANY(eu.user_uuids)
),
admin_data AS (
    SELECT COALESCE(json_agg(oa.user_uuid), '[]'::json) as data
    FROM organization_admins oa
    WHERE oa.organization_uuid = $1
),
endpoint_data AS (
    SELECT COALESCE(json_agg(row_to_json(e)), '[]'::json) as data
    FROM endpoints e, entity_uuids eu
    WHERE e.uuid = ANY(eu.endpoint_uuids)
),
auth_data AS (
    SELECT COALESCE(json_agg(row_to_json(a)), '[]'::json) as data
    FROM auths a, auth_uuids au
    WHERE a.uuid = ANY(au.auth_uuids)
),
template_data AS (
    SELECT COALESCE(json_agg(row_to_json(t)), '[]'::json) as data
    FROM templates t, entity_uuids eu
    WHERE t.uuid = ANY(eu.template_uuids)
),
workflow_data AS (
    SELECT COALESCE(json_agg(row_to_json(w)), '[]'::json) as data
    FROM workflows w, entity_uuids eu
    WHERE w.uuid = ANY(eu.workflow_uuids)
),
workflow_template_data AS (
    SELECT COALESCE(json_agg(json_build_object(
        'workflow_uuid', wt.workflow_uuid,
        'template_uuid', wt.template_uuid,
        'created_at', wt.created_at,
        'updated_at', wt.updated_at
    )), '[]'::json) as data
    FROM workflow_templates wt, entity_uuids eu
    WHERE wt.workflow_uuid = ANY(eu.workflow_uuids)
),
eden_node_endpoint_data AS (
    SELECT COALESCE(json_agg(json_build_object(
        'eden_node_uuid', ene.eden_node_uuid,
        'endpoint_uuid', ene.endpoint_uuid,
        'created_at', ene.created_at,
        'updated_at', ene.updated_at
    )), '[]'::json) as data
    FROM eden_node_endpoints ene, entity_uuids eu
    WHERE ene.endpoint_uuid = ANY(eu.endpoint_uuids)
),
org_eden_node_data AS (
    SELECT COALESCE(json_agg(json_build_object(
        'organization_uuid', oen.organization_uuid,
        'eden_node_uuid', oen.eden_node_uuid
    )), '[]'::json) as data
    FROM organization_eden_nodes oen
    WHERE oen.organization_uuid = $1
),
org_api_data AS (
    SELECT COALESCE(json_agg(json_build_object(
        'organization_uuid', oa.organization_uuid,
        'api_uuid', oa.api_uuid
    )), '[]'::json) as data
    FROM organization_apis oa
    WHERE oa.organization_uuid = $1
),
org_interlay_data AS (
    SELECT COALESCE(json_agg(json_build_object(
        'organization_uuid', oi.organization_uuid,
        'interlay_uuid', oi.interlay_uuid
    )), '[]'::json) as data
    FROM organization_interlays oi
    WHERE oi.organization_uuid = $1
),
interlay_data AS (
    SELECT COALESCE(json_agg(row_to_json(i)), '[]'::json) as data
    FROM interlays i
    JOIN organization_interlays oi ON oi.interlay_uuid = i.uuid
    WHERE oi.organization_uuid = $1
),
robot_data AS (
    SELECT COALESCE(json_agg(row_to_json(r)), '[]'::json) as data
    FROM robots r
    WHERE r.organization_uuid = $1
)
SELECT json_build_object(
    'organization', (SELECT data FROM org_data),
    'users', (SELECT data FROM user_data),
    'admins', (SELECT data FROM admin_data),
    'endpoints', (SELECT data FROM endpoint_data),
    'auths', (SELECT data FROM auth_data),
    'templates', (SELECT data FROM template_data),
    'workflows', (SELECT data FROM workflow_data),
    'workflow_templates', (SELECT data FROM workflow_template_data),
    'eden_node_endpoints', (SELECT data FROM eden_node_endpoint_data),
    'organization_eden_nodes', (SELECT data FROM org_eden_node_data),
    'organization_apis', (SELECT data FROM org_api_data),
    'organization_interlays', (SELECT data FROM org_interlay_data),
    'interlays', (SELECT data FROM interlay_data),
    'robots', (SELECT data FROM robot_data)
) as export_data;
