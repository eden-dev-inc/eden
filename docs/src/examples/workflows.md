# Workflow Examples

This page provides practical examples of using workflows in Eden-MDBS.

## Prerequisites

Before creating workflows, you need templates. Create the required templates first:

```bash
# Create insert_user template
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "insert_user",
    "template": {
      "endpoint_uuid": "YOUR_ENDPOINT_UUID",
      "kind": "Write",
      "template": {
        "query": "INSERT INTO users (email, name, status) VALUES ({{email}}, {{name}}, {{status}}) RETURNING id",
        "params": ["{{email}}", "{{name}}", "{{status}}"]
      },
      "endpoint_kind": "Postgres"
    }
  }'

# Create insert_profile template
curl http://{host}:8000/api/v1/templates \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "insert_profile",
    "template": {
      "endpoint_uuid": "YOUR_ENDPOINT_UUID",
      "kind": "Write",
      "template": {
        "query": "INSERT INTO profiles (user_id, preferences, created_at) VALUES ({{user_id}}, {{preferences}}, NOW())",
        "params": ["{{user_id}}", "{{preferences}}"]
      },
      "endpoint_kind": "Postgres"
    }
  }'
```

## User Onboarding Workflow

Create a complete user with profile and settings:

```bash
# Create the workflow
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "user_onboarding",
    "description": "Create user with profile and default settings",
    "steps": [
      {
        "id": "create_user",
        "template_id": "insert_user",
        "params": {
          "email": "{{input.email}}",
          "name": "{{input.name}}",
          "status": "active"
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
        "id": "create_settings",
        "template_id": "insert_settings",
        "params": {
          "user_id": "{{steps.create_user.result.id}}",
          "notifications": true,
          "theme": "light"
        }
      }
    ]
  }'

# Execute the workflow
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "email": "john@example.com",
    "name": "John Doe",
    "preferences": {"language": "en", "timezone": "UTC"}
  }'
```

**Response:**

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
      "create_settings": {
        "status": "completed",
        "result": {"id": 789}
      }
    }
  }
}
```

## Order Processing Workflow

Process an order with inventory check and notification:

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "process_order",
    "description": "Process order with inventory validation",
    "steps": [
      {
        "id": "check_inventory",
        "template_id": "get_inventory",
        "params": {
          "product_id": "{{input.product_id}}"
        }
      },
      {
        "id": "reserve_stock",
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
        "condition": "{{steps.reserve_stock.success}}",
        "params": {
          "customer_id": "{{input.customer_id}}",
          "product_id": "{{input.product_id}}",
          "quantity": "{{input.quantity}}",
          "status": "confirmed"
        }
      },
      {
        "id": "log_order",
        "template_id": "insert_audit_log",
        "condition": "{{steps.create_order.success}}",
        "params": {
          "action": "ORDER_CREATED",
          "entity_type": "order",
          "entity_id": "{{steps.create_order.result.id}}"
        }
      }
    ]
  }'
```

## Data Migration Workflow

Migrate data between systems in batches:

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "migrate_users",
    "description": "Migrate users from legacy to new system",
    "steps": [
      {
        "id": "fetch_legacy",
        "template_id": "read_legacy_users",
        "params": {
          "batch_size": "{{input.batch_size}}",
          "offset": "{{input.offset}}"
        }
      },
      {
        "id": "transform_data",
        "template_id": "transform_user_format",
        "params": {
          "users": "{{steps.fetch_legacy.result.rows}}"
        }
      },
      {
        "id": "insert_new",
        "template_id": "bulk_insert_users",
        "params": {
          "users": "{{steps.transform_data.result}}"
        }
      },
      {
        "id": "log_migration",
        "template_id": "insert_migration_log",
        "params": {
          "batch_offset": "{{input.offset}}",
          "records_migrated": "{{steps.insert_new.result.rows_affected}}"
        }
      }
    ]
  }'

# Run migration in batches
curl http://{host}:8000/api/v1/workflows/migrate_users \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{"batch_size": 100, "offset": 0}'
```

## Audit Trail Workflow

Update records with automatic audit logging:

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "audited_update",
    "description": "Update record with audit trail",
    "steps": [
      {
        "id": "get_current",
        "template_id": "get_record_by_id",
        "params": {
          "table": "{{input.table}}",
          "id": "{{input.record_id}}"
        }
      },
      {
        "id": "update_record",
        "template_id": "update_record",
        "params": {
          "table": "{{input.table}}",
          "id": "{{input.record_id}}",
          "data": "{{input.new_data}}"
        }
      },
      {
        "id": "create_audit",
        "template_id": "insert_audit_log",
        "params": {
          "action": "UPDATE",
          "entity_type": "{{input.table}}",
          "entity_id": "{{input.record_id}}",
          "old_value": "{{steps.get_current.result}}",
          "new_value": "{{input.new_data}}",
          "user_id": "{{input.user_id}}"
        }
      }
    ]
  }'
```

## Account Deactivation Workflow

Deactivate user with cascading cleanup:

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "deactivate_account",
    "description": "Deactivate user and clean up sessions",
    "steps": [
      {
        "id": "update_status",
        "template_id": "update_user_status",
        "params": {
          "user_id": "{{input.user_id}}",
          "status": "inactive"
        }
      },
      {
        "id": "terminate_sessions",
        "template_id": "terminate_user_sessions",
        "params": {
          "user_id": "{{input.user_id}}"
        }
      },
      {
        "id": "revoke_tokens",
        "template_id": "revoke_user_tokens",
        "params": {
          "user_id": "{{input.user_id}}"
        }
      },
      {
        "id": "log_deactivation",
        "template_id": "insert_audit_log",
        "params": {
          "action": "ACCOUNT_DEACTIVATED",
          "entity_type": "user",
          "entity_id": "{{input.user_id}}",
          "performed_by": "{{input.admin_id}}"
        }
      }
    ]
  }'
```

## Notification Workflow

Send notifications through multiple channels:

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "send_notification",
    "description": "Send notification via multiple channels",
    "steps": [
      {
        "id": "get_user_prefs",
        "template_id": "get_user_preferences",
        "params": {
          "user_id": "{{input.user_id}}"
        }
      },
      {
        "id": "send_email",
        "template_id": "queue_email",
        "condition": "{{steps.get_user_prefs.result.email_enabled}}",
        "params": {
          "to": "{{steps.get_user_prefs.result.email}}",
          "subject": "{{input.subject}}",
          "body": "{{input.message}}"
        }
      },
      {
        "id": "store_notification",
        "template_id": "insert_notification",
        "params": {
          "user_id": "{{input.user_id}}",
          "type": "{{input.type}}",
          "message": "{{input.message}}",
          "read": false
        }
      }
    ]
  }'
```

## Managing Workflows

### List Workflows

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Authorization: Bearer $TOKEN"
```

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

## Error Handling

### Workflow Failure Response

When a step fails, subsequent steps are skipped:

```json
{
  "status": "error",
  "data": {
    "workflow_id": "process_order",
    "failed_step": "reserve_stock",
    "error": "Insufficient inventory",
    "completed_steps": ["check_inventory"],
    "skipped_steps": ["create_order", "log_order"]
  }
}
```

### Conditional Skip Response

When a condition is not met:

```json
{
  "status": "success",
  "data": {
    "workflow_id": "send_notification",
    "steps": {
      "get_user_prefs": {"status": "completed"},
      "send_email": {"status": "skipped", "reason": "condition not met"},
      "store_notification": {"status": "completed"}
    }
  }
}
```

## Best Practices

### Design Principles

- **Keep workflows focused**: One logical operation per workflow
- **Use meaningful IDs**: Step IDs should describe their purpose
- **Validate early**: Put validation steps at the beginning
- **Log important actions**: Include audit steps for tracking

### Error Recovery

- Design for partial failures
- Use conditional steps to handle edge cases
- Consider compensating workflows for rollback scenarios

### Performance

- Minimize the number of steps
- Use efficient templates
- Avoid unnecessary data fetching

## Related

- [Workflows Guide](../guide/workflows.md) - Workflow concepts
- [Templates](../advanced/templates.md) - Creating templates
- [Transactions](./transactions.md) - Atomic operations
