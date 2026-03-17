"""
Simple test to isolate the hang issue.
"""

import pytest
import s3lib


def test_simple_put_get_delete():
    """Simplest possible test: PUT, GET, DELETE."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except Exception:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        print("PUT object...")
        conn.put_object(bucket, 'test_simple', b'test data')

        print("GET object...")
        resp = conn.get_object(bucket, 'test_simple')
        data = resp.read()
        print(f"Got data: {data}")
        assert data == b'test data'

        print("DELETE object...")
        conn.delete_object(bucket, 'test_simple')
        print("Done!")


def test_two_puts_then_deletes():
    """Two PUTs followed by two DELETEs."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except Exception:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        print("\nPUT 1...")
        conn.put_object(bucket, 'test_two_1', b'data 1')

        print("PUT 2...")
        conn.put_object(bucket, 'test_two_2', b'data 2')

        print("DELETE 1...")
        conn.delete_object(bucket, 'test_two_1')

        print("DELETE 2...")
        conn.delete_object(bucket, 'test_two_2')

        print("Done!")


def test_ten_puts_then_deletes():
    """Ten PUTs followed by ten DELETEs - reproduces the hang."""
    try:
        from s3lib.ui import load_creds
        (access_id, secret_key) = load_creds(None)
    except Exception:
        pytest.skip("No AWS credentials available")

    bucket = 's3libtestbucket'

    with s3lib.Connection(access_id, secret_key) as conn:
        # Do 10 PUTs
        for i in range(10):
            print(f"PUT {i}...")
            data = f'test data {i}'.encode('utf-8')
            conn.put_object(bucket, f'test_ten_{i}', data)

        # Do 10 DELETEs
        for i in range(10):
            print(f"DELETE {i}...")
            conn.delete_object(bucket, f'test_ten_{i}')

        print("All done!")
