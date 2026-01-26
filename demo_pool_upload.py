#!/usr/bin/env python3
"""
Demo: Reusing S3 connections to upload multiple objects using ConnectionPool.

This script demonstrates how to efficiently upload multiple objects to S3
by reusing a single connection through the ConnectionPool/lease mechanism.

Usage:
    python demo_pool_upload.py <bucket_name> [--prefix PREFIX]

Example:
    python demo_pool_upload.py my-bucket --prefix demo/uploads/
"""

import argparse
import sys
from s3lib import ConnectionPool
from s3lib.ui import load_creds


def main():
    parser = argparse.ArgumentParser(
        description='Demo: Upload multiple objects using a pooled connection'
    )
    parser.add_argument('bucket', help='S3 bucket name')
    parser.add_argument('--prefix', default='demo_upload_',
                        help='Key prefix for uploaded objects (default: demo_upload_)')
    parser.add_argument('--count', type=int, default=5,
                        help='Number of objects to upload (default: 5)')
    parser.add_argument('--cleanup', action='store_true',
                        help='Delete uploaded objects after verification')
    args = parser.parse_args()

    # Load AWS credentials
    try:
        access_id, secret_key = load_creds(None)
    except Exception as e:
        print(f"Error loading credentials: {e}", file=sys.stderr)
        print("Ensure AWS credentials are configured.", file=sys.stderr)
        sys.exit(1)

    print(f"Uploading {args.count} objects to s3://{args.bucket}/{args.prefix}*")
    print()

    # Create a connection pool
    # The pool manages connections and enables efficient reuse
    with ConnectionPool(access_id, secret_key) as pool:
        # Lease a single connection from the pool
        # This connection will be reused for all operations within the context
        with pool.lease() as conn:
            # Upload multiple objects on the same connection
            print("Uploading objects...")
            for i in range(args.count):
                key = f'{args.prefix}{i}'
                data = f'Demo upload data for object {i}\nTimestamp will vary per run.'.encode('utf-8')
                conn.put_object(args.bucket, key, data)
                print(f"  Uploaded: s3://{args.bucket}/{key} ({len(data)} bytes)")

            print()

            # Verify uploads using the same connection
            print("Verifying uploads...")
            for i in range(args.count):
                key = f'{args.prefix}{i}'
                resp = conn.get_object(args.bucket, key)
                data = resp.read()  # Must read response to allow connection reuse
                print(f"  Verified: s3://{args.bucket}/{key} ({len(data)} bytes)")

            # Optional: List objects with the prefix
            print()
            print("Listing objects with prefix:")
            for key in conn.list_bucket(args.bucket, prefix=args.prefix):
                print(f"  {key}")

            # Optional cleanup
            if args.cleanup:
                print()
                print("Cleaning up (deleting uploaded objects)...")
                for i in range(args.count):
                    key = f'{args.prefix}{i}'
                    conn.delete_object(args.bucket, key)
                    print(f"  Deleted: s3://{args.bucket}/{key}")

        # Show pool statistics after operations complete
        stats = pool.stats()
        print()
        print("Pool Statistics:")
        print(f"  Total connections created: {stats['total_connections']}")
        print(f"  Connections available: {stats['available']}")
        print(f"  Connections in use: {stats['in_use']}")

    print()
    print("Done. Connection pool closed.")


if __name__ == '__main__':
    main()
