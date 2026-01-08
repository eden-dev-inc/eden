# Workflow APIs

This reference covers the APIs for managing and executing workflows in Eden-MDBS.

## Overview

Workflows orchestrate multiple template executions in sequence, enabling complex multi-step operations with conditional logic and data passing between steps.

## Create Workflow

Create a new workflow definition.

### Request

```http
POST /api/v1/workflows
Content-Type: application/json
Authorization: Bearer <token>
```

### Body Parameters

| Field         | Type   | Required | Description                    |
| ------------- | ------ | -------- | ------------------------------ |
| `id`          | string | Yes      | Unique workflow identifier     |
| `description` | string | No       | Workflow description           |
| `steps`       | array  | Yes      | Array of step definitions      |

### Step Object

| Field         | Type   | Required | Description                              |
| ------------- | ------ | -------- | ---------------------------------------- |
| `id`          | string | Yes      | Unique step identifier                   |
| `template_id` | string | Yes      | Template to execute                      |
| `params`      | object | No       | Parameters for the template              |
| `condition`   | string | No       | Condition expression for execution       |

### Example

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -d '{
    "id": "user_onboarding",
    "description": "Complete user onboarding process",
    "steps": [
      {
        "id": "create_user",
        "template_id": "insert_user",
        "params": {
          "email": "{{input.email}}",
          "name": "{{input.name}}"
        }
      },
      {
        "id": "create_profile",
        "template_id": "insert_profile",
        "params": {
          "user_id": "{{steps.create_user.result.id}}"
        }
      }
    ]
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "user_onboarding",
    "description": "Complete user onboarding process",
    "steps_count": 2,
    "created_at": "2024-01-15T10:30:00Z"
  }
}
```

## List Workflows

Get all workflows in your organization.

### Request

```http
GET /api/v1/workflows
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/workflows \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "workflows": [
      {
        "id": "user_onboarding",
        "description": "Complete user onboarding process",
        "steps_count": 2,
        "created_at": "2024-01-15T10:30:00Z"
      },
      {
        "id": "process_order",
        "description": "Process order with inventory check",
        "steps_count": 4,
        "created_at": "2024-01-16T09:00:00Z"
      }
    ]
  }
}
```

## Get Workflow

Get details of a specific workflow.

### Request

```http
GET /api/v1/workflows/{id}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Authorization: Bearer $TOKEN"
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "user_onboarding",
    "description": "Complete user onboarding process",
    "steps": [
      {
        "id": "create_user",
        "template_id": "insert_user",
        "params": {
          "email": "{{input.email}}",
          "name": "{{input.name}}"
        }
      },
      {
        "id": "create_profile",
        "template_id": "insert_profile",
        "params": {
          "user_id": "{{steps.create_user.result.id}}"
        }
      }
    ],
    "created_at": "2024-01-15T10:30:00Z"
  }
}
```

## Execute Workflow

Run a workflow with input parameters.

### Request

```http
POST /api/v1/workflows/{id}
Content-Type: application/json
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X POST \
  -d '{
    "email": "john@example.com",
    "name": "John Doe"
  }'
```

### Success Response

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
      }
    }
  }
}
```

### Partial Failure Response

```json
{
  "status": "error",
  "data": {
    "workflow_id": "user_onboarding",
    "failed_step": "create_profile",
    "error": "Foreign key constraint violation",
    "completed_steps": ["create_user"],
    "skipped_steps": []
  }
}
```

## Update Workflow

Update an existing workflow.

### Request

```http
PUT /api/v1/workflows/{id}
Content-Type: application/json
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Content-Type: application/json" \
  -H "Authorization: Bearer $TOKEN" \
  -X PUT \
  -d '{
    "description": "Updated onboarding workflow",
    "steps": [
      {
        "id": "create_user",
        "template_id": "insert_user",
        "params": {
          "email": "{{input.email}}",
          "name": "{{input.name}}",
          "status": "active"
        }
      }
    ]
  }'
```

### Response

```json
{
  "status": "success",
  "data": {
    "id": "user_onboarding",
    "description": "Updated onboarding workflow",
    "updated_at": "2024-01-17T10:30:00Z"
  }
}
```

## Delete Workflow

Remove a workflow.

### Request

```http
DELETE /api/v1/workflows/{id}
Authorization: Bearer <token>
```

### Example

```bash
curl http://{host}:8000/api/v1/workflows/user_onboarding \
  -H "Authorization: Bearer $TOKEN" \
  -X DELETE
```

### Response

```json
{
  "status": "success",
  "data": {
    "message": "Workflow deleted successfully"
  }
}
```

## Parameter References

### Input Parameters

Reference values from workflow input:

```json
{
  "params": {
    "email": "{{input.email}}",
    "name": "{{input.name}}"
  }
}
```

### Step Results

Reference results from previous steps:

```json
{
  "params": {
    "user_id": "{{steps.create_user.result.id}}",
    "order_total": "{{steps.calculate_total.result.total}}"
  }
}
```

### Step Success

Check if a previous step succeeded:

```json
{
  "condition": "{{steps.previous_step.success}}"
}
```

## Conditional Execution

### Simple Condition

```json
{
  "id": "send_notification",
  "template_id": "send_email",
  "condition": "{{steps.create_order.success}}",
  "params": {
    "to": "{{input.email}}"
  }
}
```

### Value Comparison

```json
{
  "id": "apply_discount",
  "template_id": "update_order",
  "condition": "{{steps.check_inventory.result.quantity >= input.quantity}}",
  "params": {
    "order_id": "{{steps.create_order.result.id}}"
  }
}
```

## Access Control

| Operation        | Required Access       |
| ---------------- | --------------------- |
| Create workflow  | Admin                 |
| List workflows   | Read                  |
| Get workflow     | Read                  |
| Execute workflow | Based on templates    |
| Update workflow  | Admin                 |
| Delete workflow  | Admin                 |

When executing a workflow, you need sufficient access to all templates used in the workflow.

## Error Responses

### Workflow Not Found

```json
{
  "error": "Not found",
  "message": "Workflow 'user_onboarding' does not exist"
}
```

### Template Not Found

```json
{
  "error": "Bad Request",
  "message": "Template 'insert_user' referenced in step 'create_user' does not exist"
}
```

### Missing Input Parameter

```json
{
  "error": "Bad Request",
  "message": "Missing required input parameter: email"
}
```

### Step Failure

```json
{
  "error": "Workflow failed",
  "message": "Step 'create_profile' failed: duplicate key value violates unique constraint",
  "details": {
    "failed_step": "create_profile",
    "completed_steps": ["create_user"]
  }
}
```

### Circular Dependency

```json
{
  "error": "Bad Request",
  "message": "Circular dependency detected in workflow steps"
}
```

## Workflow Behavior

### Step Execution
Steps execute sequentially in the order defined.

### Conditional Skip
Steps with conditions that evaluate to false are skipped, not failed.

### Failure Handling
When a step fails, subsequent steps are skipped and the workflow returns an error.

### Result Availability
Each step's result is available to subsequent steps via parameter references.

## Best Practices

### Step Design
- Keep steps focused on single operations
- Use meaningful step IDs
- Place validation steps early

### Error Handling
- Design for partial failure scenarios
- Use conditional steps for optional operations
- Consider compensating workflows for cleanup

### Performance
- Minimize the number of steps
- Avoid unnecessary data fetching
- Use efficient templates

## Related

- [Templates](../advanced/templates.md) - Creating templates
- [Workflows Guide](../guide/workflows.md) - Concepts and patterns
- [Workflow Examples](../examples/workflows.md) - Practical examples
