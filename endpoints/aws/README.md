# AWS Endpoint

Unified AWS API endpoint for Eden. Provides SigV4-signed HTTP access to **all**
AWS services (EC2, S3, IAM, Lambda, DynamoDB, CloudFormation, etc.) through a
single endpoint, using reqwest + aws-sigv4 rather than per-service SDK crates.

## Architecture

```
endpoints/aws/                   <-- this crate (endpoint plumbing, tool server, metadata)
endpoint-core/aws-core/          <-- core client library (SigV4 signing, credential resolution)
```

### How requests flow

```
LLM / User
    |
    v
Tool: execute_aws_request(service, method, path, query, body)
    |
    v
AwsCommandValidator  -- classifies safety (Safe / Moderate / Dangerous)
    |
    v
Relay (permission-gated)
    |
    v
AwsClient.execute()  -- builds URL, serialises body, signs with SigV4
    |
    v
AWS Service API (e.g. https://ec2.us-east-1.amazonaws.com)
```

## Connection Configuration

```json
{
  "region": "us-east-1",
  "access_key_id": "AKIA...",
  "secret_access_key": "...",
  "session_token": "...",
  "endpoint_url": "http://localhost:4566",
  "role_arn": "arn:aws:iam::123456789012:role/MyRole"
}
```

| Field | Required | Description |
|-------|----------|-------------|
| `region` | Yes | AWS region (e.g. `us-east-1`) |
| `access_key_id` | No | Static IAM access key. Must be paired with `secret_access_key`. |
| `secret_access_key` | No | Static IAM secret key. Must be paired with `access_key_id`. |
| `session_token` | No | STS session token for temporary credentials. |
| `endpoint_url` | No | Override the service endpoint (for LocalStack, MinIO, etc.). |
| `role_arn` | No | IAM role ARN for cross-account or role-based access (future). |

### Credential Resolution Order

1. **Static credentials**: if `access_key_id` + `secret_access_key` are provided, use them directly
   (with optional `session_token`).
2. **Default credential chain**: falls back to the standard AWS chain: environment variables
   (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_SESSION_TOKEN`), shared config
   (`~/.aws/credentials`), ECS/EC2 instance metadata, etc.

## Tools

The endpoint exposes three tools, one per AWS API protocol family.

### Protocol Selection Guide

| Protocol | Services | Tool to use |
|----------|----------|-------------|
| Query (form-encoded) | EC2, IAM, STS, CloudFormation, SQS, SNS, RDS, CloudWatch, AutoScaling, ElastiCache, Redshift, ELB, EMR, Glacier | `execute_aws_query` |
| JSON Target | DynamoDB, Kinesis, Firehose, CloudWatch Logs, Step Functions, CodePipeline, CodeDeploy, WAF | `execute_aws_json_target` |
| REST-JSON | ECS, EKS, Lambda (REST), Secrets Manager, API Gateway, CodeBuild, Batch, Glue, IoT | `execute_aws_request` |
| REST-XML | S3, CloudFront, Route 53 | `execute_aws_request` |

---

### `execute_aws_query`

Execute a query-protocol AWS API request. Automatically builds the `Action=X&Version=Y&...`
form body and handles the XML response. The version defaults to the correct value for the
service if omitted.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `service` | string | Yes | AWS service (`ec2`, `iam`, `sts`, `cloudformation`, `sqs`, `sns`, `rds`, `cloudwatch`, `autoscaling`, `elasticache`, `redshift`, `elb`, `elbv2`, `emr`, `route53`, `glacier`) |
| `action` | string | Yes | API action name (`DescribeInstances`, `ListUsers`, `CreateQueue`, etc.) |
| `version` | string | No | API version string. Omit to use per-service defaults. |
| `params` | object | No | Additional parameters as a flat key-value map. Use AWS dot notation for arrays. |
| `relay_permission` | string | No | Override permission level (`read`, `write`, `admin`) |

**Safety classification** is based on action name prefix:
- `Describe*`, `List*`, `Get*`, `Check*`, `Preview*` → Safe
- `Delete*`, `Terminate*`, `Remove*`, `Detach*`, `Revoke*` → Dangerous
- Everything else → Moderate

**Examples:**

```json
// List EC2 instances
{ "service": "ec2", "action": "DescribeInstances" }

// List running EC2 instances with a filter
{
  "service": "ec2",
  "action": "DescribeInstances",
  "params": {
    "Filter.1.Name": "instance-state-name",
    "Filter.1.Value.1": "running"
  }
}

// List IAM users
{ "service": "iam", "action": "ListUsers" }

// Describe a CloudFormation stack
{
  "service": "cloudformation",
  "action": "DescribeStacks",
  "params": { "StackName": "my-stack" }
}

// List SQS queues
{ "service": "sqs", "action": "ListQueues" }

// Describe RDS DB instances
{ "service": "rds", "action": "DescribeDBInstances" }
```

---

### `execute_aws_json_target`

Execute a JSON Target AWS API request. Automatically sets the `X-Amz-Target` header and
the correct `application/x-amz-json-{version}` content type.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `service` | string | Yes | AWS service (`dynamodb`, `kinesis`, `firehose`, `logs`, `states`, `codepipeline`, `codedeploy`, `emr`, `opsworks`, `storagegateway`, `waf`, `waf-regional`) |
| `target` | string | Yes | Full `X-Amz-Target` value: `"ServiceVersion.OperationName"` |
| `body` | object | No | JSON request body |
| `ct_version` | string | No | `"1.0"` (DynamoDB default) or `"1.1"` (all others default). Omit to use service default. |
| `relay_permission` | string | No | Override permission level (`read`, `write`, `admin`) |

**Target format examples:**

| Service | Target prefix | Example |
|---------|--------------|---------|
| DynamoDB | `DynamoDB_20120810` | `DynamoDB_20120810.ListTables` |
| Kinesis | `Kinesis_20131202` | `Kinesis_20131202.ListStreams` |
| CloudWatch Logs | `Logs_20140328` | `Logs_20140328.DescribeLogGroups` |
| Step Functions | `AmazonStates` | `AmazonStates.ListStateMachines` |
| CodePipeline | `CodePipeline_20150709` | `CodePipeline_20150709.ListPipelines` |

**Examples:**

```json
// List DynamoDB tables
{ "service": "dynamodb", "target": "DynamoDB_20120810.ListTables" }

// DynamoDB PutItem
{
  "service": "dynamodb",
  "target": "DynamoDB_20120810.PutItem",
  "body": {
    "TableName": "Users",
    "Item": { "UserId": { "S": "u1" }, "Name": { "S": "Alice" } }
  }
}

// List Kinesis streams
{ "service": "kinesis", "target": "Kinesis_20131202.ListStreams" }

// List CloudWatch Log groups
{ "service": "logs", "target": "Logs_20140328.DescribeLogGroups" }

// List Step Functions state machines
{ "service": "states", "target": "AmazonStates.ListStateMachines" }
```

---

### `execute_aws_request`

Execute a raw REST API request. Use this for REST-JSON services (ECS, EKS, Lambda REST API,
Secrets Manager, API Gateway, CodeBuild, etc.) and REST-XML services (S3, CloudFront, Route 53).
Non-JSON responses (XML) are returned as raw text.

**Parameters:**

| Parameter | Type | Required | Description |
|-----------|------|----------|-------------|
| `service` | string | Yes | AWS service name |
| `method` | string | Yes | HTTP method (`GET`, `POST`, `PUT`, `DELETE`, `PATCH`, `HEAD`) |
| `path` | string | Yes | Request path (e.g., `/v1/clusters` for ECS, `/my-bucket/key` for S3) |
| `query` | string | No | Raw query string |
| `body` | object | No | JSON request body |
| `relay_permission` | string | No | Override permission level (`read`, `write`, `admin`) |

**Examples:**

```json
// List ECS clusters
{ "service": "ecs", "method": "GET", "path": "/v1/clusters" }

// List S3 buckets
{ "service": "s3", "method": "GET", "path": "/" }

// Get S3 object
{ "service": "s3", "method": "GET", "path": "/my-bucket/my-key" }

// Invoke Lambda function
{
  "service": "lambda",
  "method": "POST",
  "path": "/2015-03-31/functions/my-function/invocations",
  "body": { "key": "value" }
}

// Get a secret from Secrets Manager
{
  "service": "secretsmanager",
  "method": "POST",
  "path": "/",
  "body": { "SecretId": "my-secret" }
}
```

## Safety Classification

Requests are classified by HTTP method for permission gating:

| Method | Safety | Rationale |
|--------|--------|-----------|
| `GET`, `HEAD` | Safe | Read-only operations |
| `POST` | Moderate | Used for both reads (Describe*, List*) and writes (Create*, Run*) |
| `PUT`, `PATCH`, `DELETE` | Dangerous | Modify or delete resources |

## Metadata Collectors

The endpoint collects AWS account metadata at three frequencies:

### HIGH frequency: `aws.identity`
Calls **STS GetCallerIdentity** to capture and validate the active credentials.

| Field | Description |
|-------|-------------|
| `account_id` | 12-digit AWS account number |
| `arn` | Full ARN of the authenticated principal |
| `user_id` | Unique identifier of the IAM entity |

### MEDIUM frequency: `aws.iam_summary`
Calls **IAM GetAccountSummary** to provide IAM resource counts.

| Field | Description |
|-------|-------------|
| `users` | Number of IAM users |
| `roles` | Number of IAM roles |
| `groups` | Number of IAM groups |
| `policies` | Number of customer-managed policies |
| `mfa_devices` | Total MFA devices provisioned |
| `mfa_devices_in_use` | MFA devices actively assigned to users |
| `access_keys_per_user_quota` | Account quota for access keys per user |

### LOW frequency: `aws.account_aliases`
Calls **IAM ListAccountAliases** to fetch human-readable account names.

| Field | Description |
|-------|-------------|
| `aliases` | List of account alias strings (e.g. `["prod-main"]`) |

## AWS API Protocol Notes

AWS services use different request/response protocols:

| Protocol | Services | Request Format | Response Format |
|----------|----------|----------------|-----------------|
| Query (form-encoded) | EC2, IAM, STS, CloudFormation, SQS, SNS, AutoScaling | `POST` with URL-encoded `Action=X&Param=Y` body | XML |
| JSON | DynamoDB, Lambda, CloudWatch Logs, Step Functions | `POST` with JSON body + `X-Amz-Target` header | JSON |
| REST-XML | S3, CloudFront, Route 53 | Standard REST verbs on resource paths | XML |
| REST-JSON | API Gateway, ECS, EKS, Secrets Manager | Standard REST verbs with JSON bodies | JSON |

Each protocol is handled by a dedicated method on `AwsClient`:

- `execute_form(service, body)`: used by the Query protocol operation; returns raw XML/text
- `execute_json_target(service, target, body, ct_version)`: used by the JSON Target operation; sets `X-Amz-Target` header and returns JSON
- `execute(service, method, path, query, body, content_type)`: used by the Custom operation; returns JSON or raw text for non-JSON responses (e.g. S3 XML)

## Feature Flag

The endpoint is gated behind the `aws` Cargo feature. It is included in the
`full` feature set. The feature propagates through:

```
eden_service  -->  endpoints  -->  endpoint  -->  aws
```

## Health Check

Uses **STS GetCallerIdentity** as a lightweight health check. This is the
recommended AWS approach because:
- Available in all regions and partitions
- Requires only minimal IAM permissions (no explicit policy needed)
- Validates that credentials are active and properly signed
- Returns the caller's identity, confirming the connection is functional

## File Structure

```
endpoints/aws/
  src/
    lib.rs                  -- module declarations
    ep.rs                   -- EP trait impl (AwsEp)
    api/
      mod.rs                -- API module + aws_endpoint! macro
      macros.rs             -- macro definitions
      lib.rs                -- AwsApi enum (Custom, Query, JsonTarget)
      lib/custom.rs         -- Custom (raw HTTP) API operation
      lib/query.rs          -- Query-protocol operation (EC2, IAM, STS, etc.)
      lib/json_target.rs    -- JSON Target operation (DynamoDB, Kinesis, etc.)
      wrapper/              -- output wrapper
    catalog.rs              -- command catalog (10 commands)
    tools/
      mod.rs                -- tool server registration
      tool.rs               -- execute_aws_request, execute_aws_query, execute_aws_json_target tools
      validator.rs          -- safety classification
    metadata/
      mod.rs                -- AwsMetadata, sync jobs, SyncCollector impls
      sync.rs               -- AwsLastSyncTimestamps
      stc/
        mod.rs
        identity.rs         -- STS GetCallerIdentity collector
        iam_summary.rs      -- IAM GetAccountSummary collector
        account_aliases.rs  -- IAM ListAccountAliases collector
    output.rs               -- AwsJsonOutput, AwsEmptyOutput
    request.rs              -- request type (define_request! macro)
    serde.rs                -- operation types + registry

endpoint-core/aws-core/
  src/
    lib.rs                  -- AwsAsync, AwsTx type aliases
    connection.rs           -- AwsConnection struct
    config.rs               -- AwsConfig (read/write/admin/system connections)
    comm.rs                 -- AwsClient (SigV4 signing, execute, execute_form)
```
