"""
Test connection reuse for multiple GET/PUT operations.
This test helps identify connection pool issues.
"""

import pytest
import s3lib
import io


def test_multiple_gets_same_connection():
    """Test multiple GET operations on same connection."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    # Create test objects
    test_data = {
        'test_key_1': b'test data 1',
        'test_key_2': b'test data 2',
        'test_key_3': b'test data 3',
    }

    with s3lib.Connection(access_id, secret_key) as conn:
        # First, put some test objects
        for key, data in test_data.items():
            conn.put_object(bucket, key, data)

        # Now do multiple GETs on the same connection
        # This tests connection reuse after PUT operations
        for i in range(5):
            for key, expected_data in test_data.items():
                resp = conn.get_object(bucket, key)
                actual_data = resp.read()
                assert actual_data == expected_data, f"GET {i}: Data mismatch for {key}"

        # Clean up
        for key in test_data.keys():
            conn.delete_object(bucket, key)


def test_multiple_puts_same_connection():
    """Test multiple PUT operations on same connection."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        # Do multiple PUTs in rapid succession
        for i in range(10):
            key = f'test_put_reuse_{i}'
            data = f'test data {i}'.encode('utf-8')
            conn.put_object(bucket, key, data)

        # Verify all were uploaded
        for i in range(10):
            key = f'test_put_reuse_{i}'
            expected_data = f'test data {i}'.encode('utf-8')
            resp = conn.get_object(bucket, key)
            actual_data = resp.read()
            assert actual_data == expected_data, f"Data mismatch for {key}"

        # Clean up
        for i in range(10):
            conn.delete_object(bucket, f'test_put_reuse_{i}')


def test_interleaved_puts_and_gets():
    """Test interleaved PUT and GET operations on same connection."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        # Interleave PUT and GET operations
        for i in range(5):
            key = f'test_interleave_{i}'
            data = f'interleaved data {i}'.encode('utf-8')

            # PUT
            conn.put_object(bucket, key, data)

            # Immediate GET of what we just PUT
            resp = conn.get_object(bucket, key)
            actual_data = resp.read()
            assert actual_data == data, f"Immediate GET failed for {key}"

            # Another GET to test double-GET scenario
            resp2 = conn.get_object(bucket, key)
            actual_data2 = resp2.read()
            assert actual_data2 == data, f"Second GET failed for {key}"

        # Clean up
        for i in range(5):
            conn.delete_object(bucket, f'test_interleave_{i}')


def test_partial_read_detected_and_prevented():
    """Test that partial reads are detected and raise ConnectionLifecycleError."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        # PUT some test data
        large_data = b'x' * 10000  # 10KB of data
        conn.put_object(bucket, 'test_partial_read', large_data)

        # GET but only read partial data (not the full response)
        resp = conn.get_object(bucket, 'test_partial_read')
        partial_data = resp.read(100)  # Only read 100 bytes, not all 10KB
        assert len(partial_data) == 100
        # NOTE: We intentionally don't read the rest of the response!

        # Now try another operation - should raise ConnectionLifecycleError
        with pytest.raises(s3lib.ConnectionLifecycleError, match="Previous response not fully consumed"):
            conn.get_object(bucket, 'test_partial_read')

        # Now consume the rest of the first response to recover
        remaining = resp.read()
        assert len(remaining) == 9900

        # After consuming, connection should work again
        resp2 = conn.get_object(bucket, 'test_partial_read')
        full_data = resp2.read()
        assert len(full_data) == 10000

        # Clean up
        conn.delete_object(bucket, 'test_partial_read')


def test_partial_read_multiple_recovery():
    """Test that we can recover from partial reads by consuming responses."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        # PUT test object
        data = b'x' * 5000  # 5KB
        conn.put_object(bucket, 'test_partial_multi', data)

        # Do a partial read
        resp1 = conn.get_object(bucket, 'test_partial_multi')
        partial1 = resp1.read(50)  # Only read 50 bytes
        assert len(partial1) == 50

        # Trying another request should fail
        with pytest.raises(s3lib.ConnectionLifecycleError):
            conn.get_object(bucket, 'test_partial_multi')

        # Consume the rest of the first response
        remaining1 = resp1.read()
        assert len(remaining1) == 4950

        # Now connection should work - do another partial read
        resp2 = conn.get_object(bucket, 'test_partial_multi')
        partial2 = resp2.read(100)
        assert len(partial2) == 100

        # Should fail again
        with pytest.raises(s3lib.ConnectionLifecycleError):
            conn.get_object(bucket, 'test_partial_multi')

        # Consume and try again
        resp2.read()

        # Should work now
        resp3 = conn.get_object(bucket, 'test_partial_multi')
        full_data = resp3.read()
        assert len(full_data) == 5000

        # Clean up
        conn.delete_object(bucket, 'test_partial_multi')


def test_no_read_detected():
    """Test that not reading response at all is detected."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        # PUT test data
        data = b'test data for no read'
        conn.put_object(bucket, 'test_no_read', data)

        # GET but don't read anything at all
        resp = conn.get_object(bucket, 'test_no_read')
        # Don't call resp.read() at all!

        # Try another operation - should raise ConnectionLifecycleError
        with pytest.raises(s3lib.ConnectionLifecycleError, match="Previous response not fully consumed"):
            conn.get_object(bucket, 'test_no_read')

        # Consume the response to recover
        resp.read()

        # Now it should work
        resp2 = conn.get_object(bucket, 'test_no_read')
        data2 = resp2.read()
        assert data2 == data

        # Clean up
        conn.delete_object(bucket, 'test_no_read')


def test_list_after_puts():
    """Test LIST operations after multiple PUTs."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'
    prefix = 'test_list_reuse_'

    with s3lib.Connection(access_id, secret_key) as conn:
        # PUT several objects
        for i in range(5):
            key = f'{prefix}{i}'
            data = f'list test data {i}'.encode('utf-8')
            conn.put_object(bucket, key, data)

        # Now LIST to find them - this uses GET internally
        keys = list(conn.list_bucket(bucket, prefix=prefix))

        # Verify all were listed
        for i in range(5):
            expected_key = f'{prefix}{i}'
            assert expected_key in keys, f"Key {expected_key} not found in listing"

        # Clean up
        for i in range(5):
            conn.delete_object(bucket, f'{prefix}{i}')
