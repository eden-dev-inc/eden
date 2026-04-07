# APIs Implementation Guide

APIs in Eden are composite endpoints that orchestrate multiple templates to execute complex data operations. This guide provides detailed implementation instructions for creating, configuring, and executing APIs in your applications.

## What Are APIs?

APIs are high-level orchestration layers that:
- Combine multiple templates into a single executable unit
- Handle complex data flows with automatic array processing
- Manage database migrations with rollback capabilities
- Provide field mapping between input data and template parameters
- Execute templates in a coordinated, sequential manner

## Implementation Overview

### Core Components You'll Work With

1. **API Schema**: The main configuration defining your API
2. **Field Bindings**: Mappings between input fields and template parameters
3. **Templates**: Individual processing units your API will execute
4. **Migration Configuration**: Optional database migration handling
5. **Response Logic**: Custom response formatting and processing

## Creating an API

### Step 1: Define Your API Schema

```json
{
  "id": "create_customer_order",
  "description": "Creates a customer order with items and inventory updates",
  "fields": [
    {
      "name": "customer",
      "type": "Object", 
      "description": "Customer information",
      "required": true
    },
    {
      "name": "order_items",
      "type": "Array",
      "description": "List of items being ordered", 
      "required": true
    },
    {
      "name": "shipping_info",
      "type": "Object",
      "description": "Shipping and delivery details",
      "required": false
    }
  ],
  "bindings": [...], // Detailed below
  "response_logic": {...}, // Optional
  "migration": {...} // Optional
}
```

### Step 2: Configure Field Bindings

Field bindings map your API input to template parameters using dot notation:

```json
{
  "bindings": [
    {
      "template": "insert_customer_template_uuid",
      "fields": {
        "customer_id": "customer.id",
        "customer_name": "customer.name", 
        "customer_email": "customer.email",
        "registration_date": "customer.created_at"
      }
    },
    {
      "template": "create_order_template_uuid", 
      "fields": {
        "customer_id": "customer.id",
        "order_total": "order_items.*.price", // Will sum all prices
        "shipping_address": "shipping_info.address",
        "delivery_method": "shipping_info.method"
      }
    },
    {
      "template": "insert_order_item_template_uuid",
      "fields": {
        "order_id": "order.id", // Available after order creation
        "product_id": "order_items.product_id", // Array processing
        "quantity": "order_items.quantity",
        "unit_price": "order_items.price",
        "line_total": "order_items.total"
      }
    }
  ]
}
```

### Step 3: HTTP Request to Create API

```http
POST /api/v1/apis
Content-Type: application/json
Authorization: Bearer your_jwt_token

{
  "id": "create_customer_order",
  "description": "Creates a customer order with items and inventory updates",
  "fields": [
    {
      "name": "customer",
      "type": "Object",
      "description": "Customer information object",
      "required": true
    },
    {
      "name": "order_items", 
      "type": "Array",
      "description": "Array of order items",
      "required": true
    }
  ],
  "bindings": [
    {
      "template": "550e8400-e29b-41d4-a716-446655440000",
      "fields": {
        "customer_id": "customer.id",
        "customer_name": "customer.name"
      }
    },
    {
      "template": "550e8400-e29b-41d4-a716-446655440001", 
      "fields": {
        "customer_id": "customer.id",
        "product_id": "order_items.product_id",
        "quantity": "order_items.quantity"
      }
    }
  ]
}
```

**Response:**
```json
{
  "status": "success",
  "message": "API created successfully"
}
```

## Executing APIs

### Basic Execution

```http
POST /api/v1/apis/create_customer_order
Content-Type: application/json  
Authorization: Bearer your_jwt_token

{
  "customer": {
    "id": "CUST-001",
    "name": "John Doe",
    "email": "john@example.com",
    "created_at": "2024-01-15T10:30:00Z"
  },
  "order_items": [
    {
      "product_id": "PROD-123",
      "quantity": 2,
      "price": 25.99,
      "total": 51.98
    },
    {
      "product_id": "PROD-456", 
      "quantity": 1,
      "price": 15.50,
      "total": 15.50
    }
  ],
  "shipping_info": {
    "address": "123 Main St, City, State 12345",
    "method": "standard"
  }
}
```

### How Execution Works

1. **Field Parsing**: Your input is parsed according to field bindings
2. **Array Detection**: System detects `order_items` is an array requiring iteration
3. **Template Execution**: Templates execute in binding order
4. **Array Processing**: For array-bound templates, execution happens once per array item
5. **Response Assembly**: All template outputs are combined into final response

### Execution Response

```json
{
  "status": "success",
  "data": {
    "550e8400-e29b-41d4-a716-446655440000": {
      "template_executed": "insert_customer",
      "fields_received": {
        "customer_id": "CUST-001",
        "customer_name": "John Doe"
      },
      "status": "success"
    },
    "550e8400-e29b-41d4-a716-446655440001": {
      "array_results": [
        {
          "template_executed": "insert_order_item",
          "fields_received": {
            "customer_id": "CUST-001",
            "product_id": "PROD-123", 
            "quantity": 2
          },
          "status": "success"
        },
        {
          "template_executed": "insert_order_item",
          "fields_received": {
            "customer_id": "CUST-001",
            "product_id": "PROD-456",
            "quantity": 1
          },
          "status": "success"
        }
      ],
      "items_processed": 2
    }
  }
}
```

## Array Processing Implementation

### How Array Processing Works

When your field bindings reference array fields without specific indices (e.g., `order_items.product_id` instead of `order_items[0].product_id`), the system automatically:

1. **Detects Array Fields**: Scans bindings for array references
2. **Creates Item Context**: For each array item, creates execution context
3. **Executes Per Item**: Runs template once for each array element
4. **Aggregates Results**: Combines all executions into array response

### Array Processing Example

**Input Data:**
```json
{
  "order": {"id": "ORD-001"},
  "items": [
    {"batch_id": "BATCH-A", "quantity": 10, "price": 5.00},
    {"batch_id": "BATCH-B", "quantity": 20, "price": 3.50},
    {"batch_id": "BATCH-C", "quantity": 15, "price": 4.25}
  ]
}
```

**Binding Configuration:**
```json
{
  "template": "process_inventory_template_uuid",
  "fields": {
    "order_id": "order.id",          // Single value, same for all
    "batch_id": "items.batch_id",    // Array processing
    "quantity": "items.quantity",    // Array processing  
    "unit_price": "items.price"      // Array processing
  }
}
```

**Execution Result:**
- Template executes 3 times (once per item)
- Each execution receives: `order_id=ORD-001` plus one item's data
- Final response contains all 3 execution results

### Mixed Processing Example

```json
{
  "bindings": [
    {
      "template": "create_order_header",
      "fields": {
        "order_id": "order.id",
        "customer_id": "order.customer_id",
        "total_amount": "order.total"
      }
    },
    {
      "template": "process_order_items", 
      "fields": {
        "order_id": "order.id",
        "item_id": "items.id",      // Array processing
        "quantity": "items.qty"     // Array processing
      }
    }
  ]
}
```

This will:
1. Execute `create_order_header` once with order data
2. Execute `process_order_items` once per item in the `items` array

## Advanced Field Mapping

### Nested Object Access

```json
{
  "customer_name": "customer.profile.personal.full_name",
  "billing_address": "customer.billing.address.street",
  "shipping_city": "order.shipping.location.city"
}
```

### Array with Index Access

```json
{
  "first_item_id": "items[0].product_id",
  "second_item_qty": "items[1].quantity"
}
```

### Conditional Field Mapping

```json
{
  "priority_level": "customer.tier", // VIP, Premium, Standard
  "discount_rate": "customer.tier.discount_percentage",
  "shipping_method": "order.urgent ? 'express' : 'standard'"
}
```

## Migration Implementation

### Migration Configuration

```json
{
  "migration": {
    "id": "customer_schema_v2_migration",
    "bindings": [
      {
        "template": "backup_existing_customers",
        "fields": {
          "backup_table": "customers_backup_v1",
          "timestamp": "migration.started_at"
        }
      },
      {
        "template": "transform_customer_data",
        "fields": {
          "source_table": "customers",
          "target_table": "customers_v2", 
          "batch_size": "migration.batch_size"
        }
      },
      {
        "template": "verify_migration_integrity",
        "fields": {
          "source_count": "migration.source_record_count",
          "target_count": "migration.target_record_count"
        }
      }
    ]
  }
}
```

### Migration Execution

Migrations are executed when you run the API, but with special handling:

1. **Migration Lock**: Prevents concurrent migrations
2. **State Tracking**: Tracks completion status
3. **Rollback Capability**: Can reverse if needed
4. **Verification**: Built-in integrity checks

**Example Migration Execution:**
```http
POST /api/v1/apis/migrate_customer_schema
Authorization: Bearer your_jwt_token

{
  "migration": {
    "started_at": "2024-01-15T10:30:00Z",
    "batch_size": 1000,
    "source_record_count": 50000,
    "target_record_count": 0
  }
}
```

## Retrieving API Configuration

### Get API Details

```http
GET /api/v1/apis/create_customer_order
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success",
  "data": {
    "id": "create_customer_order",
    "uuid": "550e8400-e29b-41d4-a716-446655440002",
    "templates": [
      {
        "uuid": "550e8400-e29b-41d4-a716-446655440000",
        "id": "insert_customer", 
        "description": "Inserts customer data",
        "created_at": "2024-01-15T10:30:00Z"
      },
      {
        "uuid": "550e8400-e29b-41d4-a716-446655440001",
        "id": "insert_order_item",
        "description": "Inserts order item data", 
        "created_at": "2024-01-15T10:30:00Z"
      }
    ],
    "response_logic": null,
    "migration": {
      "uuid": "550e8400-e29b-41d4-a716-446655440003",
      "state": false,
      "templates": [...],
      "response_logic": null
    },
    "created_at": "2024-01-15T10:30:00Z",
    "updated_at": "2024-01-15T10:30:00Z"
  }
}
```

## Deleting APIs

### Remove API

```http
DELETE /api/v1/apis/create_customer_order
Authorization: Bearer your_jwt_token
```

**Response:**
```json
{
  "status": "success", 
  "message": "success"
}
```

**Important Notes:**
- Deleting an API doesn't delete the underlying templates
- API is disconnected from your organization
- Ongoing executions may fail after deletion
- Migration state is preserved for rollback purposes

## Error Handling Implementation

### Common Error Scenarios

1. **Missing Required Fields**
```json
{
  "error": "Missing required field: customer.id",
  "field": "customer.id",
  "binding": "insert_customer_template"
}
```

2. **Template Execution Failure**
```json
{
  "error": "Template execution failed",
  "template": "550e8400-e29b-41d4-a716-446655440000",
  "details": "Database connection timeout"
}
```

3. **Array Processing Error**
```json
{
  "error": "Array processing failed at item 2",
  "array_field": "order_items",
  "item_index": 2,
  "details": "Invalid product_id format"
}
```

### Error Recovery Strategies

1. **Partial Success Handling**: Design templates to be idempotent
2. **Retry Logic**: Implement retry mechanisms for transient failures
3. **Rollback Procedures**: Use migration features for data consistency
4. **Validation**: Validate input data before API execution

## Performance Optimization

### Template Caching
Templates are automatically cached for faster execution:
- First execution loads template from database
- Subsequent executions use cached version
- Cache invalidated when template is updated

### Array Processing Optimization
For large arrays:
- Process in batches rather than single items
- Use streaming for memory efficiency
- Consider pagination for very large datasets

### Migration Performance
- Use batch processing for large data migrations
- Monitor migration progress with checkpoints
- Plan migrations during low-traffic periods

## Best Practices for Implementation

### 1. API Design
- Keep APIs focused on single business operations
- Use descriptive IDs and field names
- Design for reusability across applications
- Document expected input formats clearly

### 2. Template Organization
- Create atomic templates for single responsibilities
- Design templates to be composable
- Implement proper error handling in each template
- Use consistent naming conventions

### 3. Field Mapping Strategy
- Use consistent field naming across APIs
- Minimize deep nesting when possible
- Validate field paths before deployment
- Document complex mapping logic

### 4. Error Handling
- Implement comprehensive validation
- Provide detailed error messages
- Log failures for debugging
- Design graceful degradation strategies

### 5. Testing
- Test with various input combinations
- Verify array processing with different array sizes
- Test migration rollback procedures
- Load test with expected production volumes

### 6. Security Considerations
- Validate all input data
- Implement proper access controls
- Audit API usage and modifications
- Secure template configurations

## Troubleshooting Common Issues

### Array Processing Not Working
**Problem**: Templates not executing for array items
**Solution**: Check field bindings don't include array indices (use `items.field` not `items[0].field`)

### Template Not Found
**Problem**: API execution fails with template not found
**Solution**: Verify template UUIDs in bindings match existing templates in your organization

### Migration Lock Timeout
**Problem**: Migration execution hangs or times out
**Solution**: Check for stuck migration locks and clear them if necessary

### Field Mapping Errors
**Problem**: Fields not mapping correctly to templates
**Solution**: Verify dot notation paths match your input data structure exactly

This implementation guide provides the detailed, hands-on information needed to successfully build and deploy APIs in your Eden environment.