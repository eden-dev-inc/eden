# S3 Endpoint

This endpoint adds typed S3-compatible object storage support to Eden.

Supported backends:

- AWS S3
- LocalStack S3
- RustFS and other S3-compatible servers via custom `endpoint_url`

Connection fields:

- `provider`
- `region`
- `endpoint_url`
- `access_key_id`
- `secret_access_key`
- `session_token`
- `force_path_style`
- `default_bucket`

Initial operations:

- `put_object`
- `get_object`
- `head_object`
- `delete_object`
- `list_objects`
- `create_bucket`
- `delete_bucket`
- `list_buckets`

Example config for AWS S3:

```json
{
  "kind": "s3",
  "config": {
    "read_conn": {
      "provider": "aws_s3",
      "region": "us-east-1",
      "default_bucket": "eden-demo"
    },
    "write_conn": {
      "provider": "aws_s3",
      "region": "us-east-1",
      "default_bucket": "eden-demo"
    }
  }
}
```

Example config for LocalStack:

```json
{
  "kind": "s3",
  "config": {
    "read_conn": {
      "provider": "localstack",
      "region": "us-east-1",
      "endpoint_url": "http://localhost:4566",
      "access_key_id": "test",
      "secret_access_key": "test",
      "force_path_style": true,
      "default_bucket": "eden-local"
    },
    "write_conn": {
      "provider": "localstack",
      "region": "us-east-1",
      "endpoint_url": "http://localhost:4566",
      "access_key_id": "test",
      "secret_access_key": "test",
      "force_path_style": true,
      "default_bucket": "eden-local"
    }
  }
}
```

RustFS compatibility notes:

- Set `provider` to `rustfs` or `generic_s3`
- Set `endpoint_url` to the RustFS S3 API base URL
- Keep `force_path_style` enabled unless your deployment explicitly supports virtual-hosted buckets
