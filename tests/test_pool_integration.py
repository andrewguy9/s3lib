"""
Integration tests for ConnectionPool with real S3.

These tests validate that the pool infrastructure works correctly
for reusing connections to upload/download multiple objects.

Tests run against multiple buckets to ensure compatibility.
"""

import pytest
import uuid
from s3lib import ConnectionPool

pytestmark = pytest.mark.timeout(30)


TEST_BUCKETS = ['s3libtestbucket', 's3libtestbucket2']


@pytest.fixture
def credentials():
    """Load AWS credentials or skip test if unavailable."""
    try:
        from s3lib.ui import load_creds
        return load_creds(None)
    except Exception:
        pytest.skip("No AWS credentials available")


@pytest.fixture(params=TEST_BUCKETS)
def bucket(request):
    """Parametrized fixture to run tests against multiple buckets."""
    return request.param


@pytest.fixture
def testprefix():
    """Generate a unique prefix for test objects using UUID."""
    return str(uuid.uuid1()) + '_'


def test_pool_multiple_puts_same_lease(credentials, bucket, testprefix):
    """Test uploading multiple objects with a single leased connection."""
    access_id, secret_key = credentials

    with ConnectionPool(access_id, secret_key) as pool:
        # Lease a single connection and upload multiple objects
        with pool.lease() as conn:
            for i in range(5):
                key = f'{testprefix}{i}'
                data = f'pool put test data {i}'.encode('utf-8')
                conn.put_object(bucket, key, data)

        # Verify pool stats - should have created only 1 connection
        stats = pool.stats()
        assert stats['total_connections'] == 1
        assert stats['available'] == 1
        assert stats['in_use'] == 0

        # Verify all objects were uploaded correctly
        with pool.lease() as conn:
            for i in range(5):
                key = f'{testprefix}{i}'
                expected = f'pool put test data {i}'.encode('utf-8')
                resp = conn.get_object(bucket, key)
                actual = resp.read()
                assert actual == expected, f"Data mismatch for {key}"

            # Clean up
            for i in range(5):
                conn.delete_object(bucket, f'{testprefix}{i}')


def test_pool_multiple_leases_reuse_connection(credentials, bucket, testprefix):
    """Test that multiple leases reuse the same connection (MRU strategy)."""
    access_id, secret_key = credentials

    with ConnectionPool(access_id, secret_key, max_connections=5) as pool:
        # Do multiple lease cycles - should reuse same connection
        for i in range(5):
            with pool.lease() as conn:
                key = f'{testprefix}{i}'
                data = f'reuse test {i}'.encode('utf-8')
                conn.put_object(bucket, key, data)

        # Should have created only 1 connection (MRU reuse)
        stats = pool.stats()
        assert stats['total_connections'] == 1
        assert stats['available'] == 1

        # Verify uploads and clean up
        with pool.lease() as conn:
            for i in range(5):
                key = f'{testprefix}{i}'
                expected = f'reuse test {i}'.encode('utf-8')
                resp = conn.get_object(bucket, key)
                actual = resp.read()
                assert actual == expected

            for i in range(5):
                conn.delete_object(bucket, f'{testprefix}{i}')


def test_pool_interleaved_put_get(credentials, bucket, testprefix):
    """Test interleaved PUT and GET operations through pool."""
    access_id, secret_key = credentials

    with ConnectionPool(access_id, secret_key) as pool:
        with pool.lease() as conn:
            for i in range(3):
                key = f'{testprefix}{i}'
                data = f'interleave data {i}'.encode('utf-8')

                # PUT
                conn.put_object(bucket, key, data)

                # Immediate GET to verify
                resp = conn.get_object(bucket, key)
                actual = resp.read()
                assert actual == data, f"Immediate GET failed for {key}"

            # Clean up
            for i in range(3):
                conn.delete_object(bucket, f'{testprefix}{i}')


def test_pool_list_operations(credentials, bucket, testprefix):
    """Test LIST operations through pool."""
    access_id, secret_key = credentials

    with ConnectionPool(access_id, secret_key) as pool:
        with pool.lease() as conn:
            # Upload several objects
            for i in range(5):
                key = f'{testprefix}{i}'
                data = f'list test {i}'.encode('utf-8')
                conn.put_object(bucket, key, data)

            # List objects with prefix
            keys = list(conn.list_bucket(bucket, prefix=testprefix))

            # Verify all keys are present
            for i in range(5):
                expected_key = f'{testprefix}{i}'
                assert expected_key in keys, f"Key {expected_key} not found in listing"

            # Clean up
            for i in range(5):
                conn.delete_object(bucket, f'{testprefix}{i}')


def test_pool_large_objects(credentials, bucket, testprefix):
    """Test uploading larger objects through pool."""
    access_id, secret_key = credentials

    with ConnectionPool(access_id, secret_key) as pool:
        with pool.lease() as conn:
            # Upload several larger objects (100KB each)
            for i in range(3):
                key = f'{testprefix}{i}'
                data = bytes([i % 256] * 100000)  # 100KB
                conn.put_object(bucket, key, data)

            # Verify all uploads
            for i in range(3):
                key = f'{testprefix}{i}'
                expected = bytes([i % 256] * 100000)
                resp = conn.get_object(bucket, key)
                actual = resp.read()
                assert actual == expected, f"Data mismatch for {key}"

            # Clean up
            for i in range(3):
                conn.delete_object(bucket, f'{testprefix}{i}')

        # Still just 1 connection
        assert pool.stats()['total_connections'] == 1


def test_pool_connection_survives_operations(credentials, bucket, testprefix):
    """Test that connection remains valid across many operations."""
    access_id, secret_key = credentials

    with ConnectionPool(access_id, secret_key) as pool:
        with pool.lease() as conn:
            # Do many operations on the same connection
            for i in range(20):
                key = f'{testprefix}{i}'
                data = f'survive test {i}'.encode('utf-8')
                conn.put_object(bucket, key, data)

            # Verify a sample
            resp = conn.get_object(bucket, f'{testprefix}10')
            actual = resp.read()
            assert actual == b'survive test 10'

            # Clean up
            for i in range(20):
                conn.delete_object(bucket, f'{testprefix}{i}')

        # Connection should still be valid and returned to pool
        stats = pool.stats()
        assert stats['available'] == 1
        assert stats['total_connections'] == 1


def test_connection_stats(credentials, bucket, testprefix):
    """Test that connection statistics are tracked correctly."""
    access_id, secret_key = credentials

    with ConnectionPool(access_id, secret_key) as pool:
        with pool.lease() as conn:
            # Do a warmup request to trigger any initial region discovery
            key = f'{testprefix}warmup'
            conn.put_object(bucket, key, b'warmup')
            conn.delete_object(bucket, key)

            # Now check stats after warmup
            stats = conn.stats()
            warmup_connects = stats['connects']
            warmup_requests = stats['requests']
            warmup_redirects = stats['redirects']

            # Do some operations
            for i in range(3):
                key = f'{testprefix}{i}'
                data = f'stats test {i}'.encode('utf-8')
                conn.put_object(bucket, key, data)

            # Verify and clean up
            for i in range(3):
                key = f'{testprefix}{i}'
                resp = conn.get_object(bucket, key)
                resp.read()
                conn.delete_object(bucket, key)

            # Check stats after operations
            stats = conn.stats()
            # 3 PUTs + 3 GETs + 3 DELETEs = 9 requests
            assert stats['requests'] == warmup_requests + 9
            # Should not have any new redirects after warmup
            assert stats['redirects'] == warmup_redirects
            # Should reuse the same connection (no new connects)
            assert stats['connects'] == warmup_connects
