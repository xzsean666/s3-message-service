# External Documentation Links

Last checked: 2026-06-01

This file records official documentation for systems and standards that Async
Messaging Service may integrate with. Future AI sessions should check these
links before changing provider adapters or storage behavior.

## Object Storage Providers

### Amazon S3

- User Guide: https://docs.aws.amazon.com/AmazonS3/latest/userguide/
- Object key naming: https://docs.aws.amazon.com/AmazonS3/latest/userguide/object-keys.html
- Data consistency overview: https://aws.amazon.com/s3/consistency/
- Conditional writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html
- Enforce conditional writes: https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes-enforce.html
- Performance guidelines: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance.html
- Performance design patterns: https://docs.aws.amazon.com/AmazonS3/latest/userguide/optimizing-performance-design-patterns.html
- Multipart upload overview: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpuoverview.html
- Multipart upload object guide: https://docs.aws.amazon.com/AmazonS3/latest/userguide/mpu-upload-object.html

### Cloudflare R2

- R2 docs: https://developers.cloudflare.com/r2/
- S3 API compatibility: https://developers.cloudflare.com/r2/api/s3/api/
- S3-compatible setup: https://developers.cloudflare.com/r2/get-started/s3/
- Consistency model: https://developers.cloudflare.com/r2/reference/consistency/
- Upload and multipart objects: https://developers.cloudflare.com/r2/objects/multipart-objects/
- Public buckets and custom domains: https://developers.cloudflare.com/r2/data-access/public-buckets/
- R2 cache behavior: https://developers.cloudflare.com/cache/interaction-cloudflare-products/r2/
- R2 docs for AI agents: https://developers.cloudflare.com/r2/llms.txt

### Backblaze B2

- Developer docs: https://www.backblaze.com/docs
- B2 authorize account: https://www.backblaze.com/apidocs/b2-authorize-account
- S3-compatible API: https://www.backblaze.com/docs/en/cloud-storage-call-the-s3-compatible-api
- Connect to Backblaze B2: https://www.backblaze.com/docs/cloud-storage-connect-to-backblaze-b2

### MinIO / Self-Hosted S3-Compatible Storage

- MinIO docs: https://docs.min.io/
- S3 API compatibility: https://docs.min.io/community/minio-object-store/reference/s3-api-compatibility.html
- MinIO Go client quickstart: https://docs.min.io/community/minio-object-store/developers/go/quickstart.html

## Relevant Standards

- RFC 9562 UUIDs: https://www.ietf.org/rfc/rfc9562

## Current Optimization Notes From Docs Review

- Prefer `ListObjectsV2` over older list APIs when using S3-compatible APIs.
- Use object keys and prefixes as the primary query model.
- Use create-if-absent or conditional write behavior for immutable message and
  reference objects when the selected provider supports it.
- Keep provider compatibility checks in adapters because S3-compatible providers
  do not implement every S3 feature identically.
- Use SDK-managed multipart upload for large attachments when possible.
- Do not assume ETag is a content MD5 for multipart uploads.
- Treat CDN or custom-domain cache behavior separately from object storage
  consistency. Cached reads can return stale results even when bucket operations
  are strongly consistent.

## Future Integration Project Docs

No consuming business project has been named yet.

When this service is integrated with another project, add a section here with:

- Project name.
- Official documentation URL.
- API contract URL.
- Authentication expectations.
- Actor identifier format.
- Tag membership source.
- Message event ownership.
