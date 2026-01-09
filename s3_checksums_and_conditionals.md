# Amazon S3 Checksums and Conditional Uploads (Modern API Summary)

## Overview

Amazon S3 now supports **explicit checksum headers** that allow clients to verify the integrity of entire objects (including multipart uploads) using modern hash and CRC algorithms. These features are independent of ETags and avoid the historical pitfalls of multipart ETag semantics.

This document summarizes:
- Checksum headers and their semantics
- How to detect overwrites
- How to prevent object creation if an object already exists
- How to combine conditionals with checksums

---

## Supported Checksum Algorithms

Amazon S3 supports whole-object checksums using the following algorithms:

- `CRC32`
- `CRC32C`
- `CRC64NVME`
- `SHA1`
- `SHA256`
- `MD5`

These checksums apply to the **fully assembled object**, even when uploaded via multipart upload.

---

## Uploading with Explicit Checksums

### Required Headers

When uploading an object via HTTP `PUT`, include:

- `x-amz-checksum-algorithm`
- One algorithm-specific checksum header

### Example: SHA-256

```http
PUT /my-bucket/my-object HTTP/1.1
Host: my-bucket.s3.amazonaws.com
x-amz-checksum-algorithm: SHA256
x-amz-checksum-sha256: <base64-sha256>
Content-Type: application/octet-stream
Content-Length: 123456
```

If the checksum does not match, S3 rejects the upload with `400 BadDigest`.

### Example: Legacy MD5

```http
x-amz-checksum-algorithm: MD5
x-amz-checksum-md5: <base64-md5>
```

This provides deterministic, whole-object MD5 validation without relying on ETag semantics.

---

## Multipart Upload Semantics

- **ETag** for multipart uploads is a *hash-of-hashes* and is **not a content hash**
- **Checksum headers** validate the **final assembled object**
- S3 validates per-part checksums internally and computes the final checksum server-side

Result: You get a single, deterministic checksum for large objects.

---

## Conditional Requests (Overwrite Control)

### Prevent Creation If Object Exists

```http
If-None-Match: *
```

- Upload succeeds **only if the object does not exist**
- If the object exists → `412 Precondition Failed`

This is the canonical way to express *create-only* semantics.

---

### Conditional Overwrite (ETag-Based)

```http
If-Match: "<etag>"
```

- Upload succeeds **only if the current ETag matches**
- Used for optimistic concurrency control

⚠️ Note: Conditionals **only operate on ETags**, not checksum headers.

---

## Important Limitation

> **Checksum headers cannot be used with `If-Match` or `If-None-Match`.**

- HTTP conditionals are defined **only in terms of ETag**
- Checksum headers are for **integrity verification**, not conditional logic
- To combine both:
  - Use checksum headers for integrity
  - Use `If-None-Match: *` for create-only
  - Use `If-Match` with ETag for overwrite protection

---

## Recommended Patterns

### Create-Only with Integrity Verification

```http
If-None-Match: *
x-amz-checksum-algorithm: SHA256
x-amz-checksum-sha256: <base64>
```

### Safe Overwrite with Integrity Verification

1. `HEAD` object → obtain ETag
2. `PUT` with:
   ```http
   If-Match: "<etag>"
   x-amz-checksum-algorithm: SHA256
   x-amz-checksum-sha256: <base64>
   ```

---

## Summary Table

| Feature | Mechanism |
|------|----------|
| Whole-object integrity | `x-amz-checksum-*` |
| Multipart-safe hashing | Yes |
| Detect overwrite | `If-Match` (ETag only) |
| Prevent creation if exists | `If-None-Match: *` |
| Conditional by checksum | ❌ Not supported |
| Legacy MD5 support | ✅ via checksum headers |

---

## Bottom Line

- **ETags are concurrency tokens**
- **Checksum headers are integrity guarantees**
- Stop reverse-engineering multipart ETags
- Use checksums for correctness, ETags for coordination
