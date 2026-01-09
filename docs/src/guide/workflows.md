# Workflows & Templates

Workflows and templates are powerful features in Eden-MDBS for creating reusable, automated database operations.

## Templates Overview

Templates are reusable, parameterized operations. They're the building blocks for consistent, secure database interactions.

### Why Use Templates?

- **Reusability**: Define once, execute many times with different parameters
- **Security**: Parameters are properly escaped, preventing injection attacks
- **Consistency**: Standardize common operations across your team
- **Access Control**: Templates integrate with RBAC

### Quick Template Example

```bash
# Create a template
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "get_active_users",
    "description": "Get users by status with pagination",
    "template": {
      "endpoint_uuid": "YOUR_ENDPOINT_UUID",
      "kind": "Read",
      "template": {
        "query": "SELECT * FROM users WHERE status = {{status}} LIMIT {{limit}} OFFSET {{offset}}",
        "params": ["{{status}}", "{{limit}}", "{{offset}}"]
      },
      "endpoint_kind": "Postgres"
    }
  }'

# Execute the template
curl http://{host}:8000/api/v1/templates/get_active_users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "status": "active",
    "limit": 10,
    "offset": 0
  }'
```

For complete template documentation, see [Templates](../advanced/templates.md).

## Workflows Overview

Workflows orchestrate multiple operations across one or more endpoints, enabling complex data processing pipelines.

### Use Cases for Workflows

- **Multi-step data processing**: Transform data through multiple stages
- **Cross-database operations**: Coordinate operations across different databases
- **Conditional logic**: Execute different paths based on results
- **Data synchronization**: Keep multiple data sources in sync

## Creating Workflows

### Basic Workflow

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "user_onboarding",
    "description": "Complete user onboarding workflow",
    "steps": [
      {
        "id": "create_user",
        "template_id": "insert_user",
        "params": {
          "name": "{{input.name}}",
          "email": "{{input.email}}"
        }
      },
      {
        "id": "create_profile",
        "template_id": "insert_profile",
        "params": {
          "user_id": "{{steps.create_user.result.id}}",
          "preferences": "{{input.preferences}}"
        }
      },
      {
        "id": "send_welcome",
        "template_id": "queue_email",
        "params": {
          "to": "{{input.email}}",
          "template": "welcome"
        }
      }
    ]
  }'
```

### Workflow with Conditional Steps

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "process_order",
    "description": "Process order with inventory check",
    "steps": [
      {
        "id": "check_inventory",
        "template_id": "get_inventory",
        "params": {
          "product_id": "{{input.product_id}}"
        }
      },
      {
        "id": "reserve_inventory",
        "template_id": "decrement_inventory",
        "condition": "{{steps.check_inventory.result.quantity >= input.quantity}}",
        "params": {
          "product_id": "{{input.product_id}}",
          "quantity": "{{input.quantity}}"
        }
      },
      {
        "id": "create_order",
        "template_id": "insert_order",
        "condition": "{{steps.reserve_inventory.success}}",
        "params": {
          "user_id": "{{input.user_id}}",
          "product_id": "{{input.product_id}}",
          "quantity": "{{input.quantity}}"
        }
      }
    ]
  }'
```

## Executing Workflows

### Run a Workflow

```bash
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "name": "John Doe",
    "email": "john@example.com",
    "preferences": {"theme": "dark", "notifications": true}
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "workflow_id": "user_onboarding",
    "execution_id": "550e8400-e29b-41d4-a716-446655440000",
    "steps": {
      "create_user": {
        "status": "completed",
        "result": {"id": 123}
      },
      "create_profile": {
        "status": "completed",
        "result": {"id": 456}
      },
      "send_welcome": {
        "status": "completed",
        "result": {"queued": true}
      }
    }
  }
}
```

## Workflow Components

### Steps

Each step in a workflow has:

| Field         | Required | Description                                    |
| ------------- | -------- | ---------------------------------------------- |
| `id`          | Yes      | Unique identifier for the step                 |
| `template_id` | Yes      | Template to execute                            |
| `params`      | No       | Parameters to pass to the template             |
| `condition`   | No       | Condition that must be true to execute step    |

### Parameter References

Reference values from different sources:

- `{{input.field}}` - Input provided when executing workflow
- `{{steps.step_id.result.field}}` - Result from a previous step
- `{{steps.step_id.success}}` - Boolean indicating if step succeeded

## Managing Workflows

### Get Workflow Details

```bash
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Authorization: Bearer $TOKEN"
```

### Delete Workflow

```bash
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

## Common Patterns

### Data Migration

```json
{
  "id": "migrate_user_data",
  "steps": [
    {
      "id": "fetch_legacy",
      "template_id": "read_legacy_users",
      "params": {"batch_size": 100, "offset": "{{input.offset}}"}
    },
    {
      "id": "transform",
      "template_id": "transform_user_format",
      "params": {"users": "{{steps.fetch_legacy.result.rows}}"}
    },
    {
      "id": "insert_new",
      "template_id": "bulk_insert_users",
      "params": {"users": "{{steps.transform.result}}"}
    }
  ]
}
```

### Audit Trail

```json
{
  "id": "audited_update",
  "steps": [
    {
      "id": "get_current",
      "template_id": "get_record",
      "params": {"id": "{{input.record_id}}"}
    },
    {
      "id": "update_record",
      "template_id": "update_record",
      "params": {"id": "{{input.record_id}}", "data": "{{input.new_data}}"}
    },
    {
      "id": "log_change",
      "template_id": "insert_audit_log",
      "params": {
        "record_id": "{{input.record_id}}",
        "old_value": "{{steps.get_current.result}}",
        "new_value": "{{input.new_data}}",
        "user_id": "{{input.user_id}}"
      }
    }
  ]
}
```

## Access Control

Workflows require appropriate access levels:

| Operation        | Required Access |
| ---------------- | --------------- |
| Create workflow  | Admin           |
| Execute workflow | Based on templates used |
| Delete workflow  | Admin           |

When executing a workflow, you need sufficient access to all templates used in the workflow.

## Error Handling

### Step Failure

If a step fails:

1. The workflow stops at that step
2. Subsequent steps are not executed
3. Response includes error details for the failed step

```json
{
  "status": "error",
  "data": {
    "workflow_id": "user_onboarding",
    "failed_step": "create_profile",
    "error": "Foreign key constraint violation",
    "completed_steps": ["create_user"]
  }
}
```

### Conditional Skip

If a step's condition evaluates to false, it's skipped (not failed):

```json
{
  "steps": {
    "check_inventory": {"status": "completed"},
    "reserve_inventory": {"status": "skipped", "reason": "condition not met"},
    "create_order": {"status": "skipped", "reason": "dependent step skipped"}
  }
}
```

## Best Practices

### Workflow Design

- **Keep workflows focused**: Each workflow should accomplish one logical task
- **Use meaningful IDs**: Step IDs should describe what they do
- **Handle failures**: Design for partial failure scenarios

### Performance

- **Minimize steps**: Combine operations where possible
- **Use appropriate templates**: Ensure templates are optimized
- **Consider parallelization**: Independent steps can run in parallel (when supported)

### Maintenance

- **Document workflows**: Use descriptive names and descriptions
- **Version control**: Track workflow definitions in your codebase
- **Test thoroughly**: Test workflows with various inputs

## Related

- [Templates](../advanced/templates.md) - Detailed template documentation
- [Transactions](./transactions.md) - Atomic operations
- [RBAC](./rbac.md) - Access control
- [API Reference](../api/workflows.md) - Workflow API details
