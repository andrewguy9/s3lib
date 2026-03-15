"""
Tests for ConnectionPool and ConnectionLease.
"""

import pytest
import threading
import time
from s3lib import ConnectionPool, ConnectionLease, Connection


def test_pool_basic_creation():
    """Test basic pool creation and properties."""
    pool = ConnectionPool(
        access_id="test_key",
        secret=b"test_secret",
        max_connections=5
    )

    assert pool.max_connections == 5
    assert pool.closed == False
    assert pool.host == "s3.amazonaws.com"
    assert pool.port == 443  # HTTPS is now the default

    pool.close()
    assert pool.closed == True


def test_pool_context_manager():
    """Test pool as context manager."""
    with ConnectionPool(access_id="test", secret=b"secret") as pool:
        assert pool.closed == False
        stats = pool.stats()
        assert stats['total_connections'] == 0
        assert stats['closed'] == False

    # After exiting, pool should be closed
    assert pool.closed == True


def test_pool_stats():
    """Test pool statistics."""
    pool = ConnectionPool(access_id="test", secret=b"secret", max_connections=3)

    stats = pool.stats()
    assert stats['total_connections'] == 0
    assert stats['available'] == 0
    assert stats['in_use'] == 0
    assert stats['max_connections'] == 3
    assert stats['closed'] == False

    pool.close()


def test_lease_context_manager():
    """Test ConnectionLease context manager."""
    pool = ConnectionPool(access_id="test", secret=b"secret")

    # Mock connection since we're not connecting to real S3
    import unittest.mock as mock

    with mock.patch('s3lib.Connection') as MockConnection:
        mock_conn = mock.Mock()
        mock_conn.conn = mock.Mock()
        mock_conn.conn.sock = mock.Mock()
        MockConnection.return_value = mock_conn

        # Lease should return connection in __enter__
        with pool.lease() as conn:
            assert conn == mock_conn

        # After lease, connection should be returned to pool
        stats = pool.stats()
        assert stats['available'] == 1
        assert stats['in_use'] == 0

    pool.close()


def test_pool_reuse_connection():
    """Test that pool reuses connections (MRU strategy)."""
    pool = ConnectionPool(access_id="test", secret=b"secret", max_connections=5)

    import unittest.mock as mock

    with mock.patch('s3lib.Connection') as MockConnection:
        # Create mock connection
        mock_conn = mock.Mock()
        mock_conn.conn = mock.Mock()
        mock_conn.conn.sock = mock.Mock()
        MockConnection.return_value = mock_conn

        # First lease - should create connection
        with pool.lease() as conn1:
            connection_id_1 = id(conn1)

        # Second lease - should reuse same connection (MRU)
        with pool.lease() as conn2:
            connection_id_2 = id(conn2)

        # Should be same connection
        assert connection_id_1 == connection_id_2

        # Only one connection should have been created
        assert MockConnection.call_count == 1
        assert pool.stats()['total_connections'] == 1

    pool.close()


def test_pool_max_connections():
    """Test pool respects max_connections limit."""
    pool = ConnectionPool(
        access_id="test",
        secret=b"secret",
        max_connections=2,
        wait_timeout=1  # Short timeout for test
    )

    import unittest.mock as mock

    with mock.patch('s3lib.Connection') as MockConnection:
        # Create mock connections
        def create_mock_conn():
            mock_conn = mock.Mock()
            mock_conn.conn = mock.Mock()
            mock_conn.conn.sock = mock.Mock()
            return mock_conn

        MockConnection.side_effect = lambda *args, **kwargs: create_mock_conn()

        # Acquire 2 connections (max limit)
        lease1 = pool.lease()
        conn1 = lease1.__enter__()

        lease2 = pool.lease()
        conn2 = lease2.__enter__()

        # Stats should show 2 in use
        stats = pool.stats()
        assert stats['total_connections'] == 2
        assert stats['in_use'] == 2
        assert stats['available'] == 0

        # Try to acquire 3rd connection - should timeout
        with pytest.raises(TimeoutError):
            with pool.lease() as conn3:
                pass

        # Clean up
        lease1.__exit__(None, None, None)
        lease2.__exit__(None, None, None)

    pool.close()


def test_pool_thread_safety():
    """Test pool is thread-safe for concurrent access."""
    pool = ConnectionPool(
        access_id="test",
        secret=b"secret",
        max_connections=5
    )

    import unittest.mock as mock

    with mock.patch('s3lib.Connection') as MockConnection:
        def create_mock_conn():
            mock_conn = mock.Mock()
            mock_conn.conn = mock.Mock()
            mock_conn.conn.sock = mock.Mock()
            return mock_conn

        MockConnection.side_effect = lambda *args, **kwargs: create_mock_conn()

        # Function to run in thread
        results = []
        def worker(worker_id):
            for i in range(3):
                with pool.lease() as conn:
                    time.sleep(0.01)  # Simulate work
                    results.append((worker_id, i))

        # Start 3 threads
        threads = []
        for i in range(3):
            t = threading.Thread(target=worker, args=(i,))
            threads.append(t)
            t.start()

        # Wait for all threads
        for t in threads:
            t.join()

        # All operations should complete
        assert len(results) == 9  # 3 threads * 3 operations

        # Pool should have created some connections
        stats = pool.stats()
        assert stats['total_connections'] > 0
        assert stats['in_use'] == 0  # All returned

    pool.close()


def test_pool_closed_raises_error():
    """Test that using closed pool raises error."""
    pool = ConnectionPool(access_id="test", secret=b"secret")
    pool.close()

    # Attempting to lease from closed pool should raise
    with pytest.raises(RuntimeError, match="closed"):
        with pool.lease() as conn:
            pass


def test_pool_close_idempotent():
    """Test that close() can be called multiple times safely."""
    pool = ConnectionPool(access_id="test", secret=b"secret")

    # Close multiple times
    pool.close()
    pool.close()
    pool.close()

    # Should still be closed
    assert pool.closed == True


def test_pool_invalid_connection_discarded():
    """Test that connections with unexhausted responses are discarded on return."""
    pool = ConnectionPool(access_id="test", secret=b"secret")

    import unittest.mock as mock

    with mock.patch('s3lib.Connection') as MockConnection:
        # First connection: not ready (outstanding unexhausted response)
        mock_conn = mock.Mock()
        mock_conn.is_ready.return_value = False
        MockConnection.return_value = mock_conn

        with pool.lease() as conn:
            pass

        # Connection returned but not ready — should be discarded.
        # Next lease should create a new connection.
        mock_conn2 = mock.Mock()
        mock_conn2.is_ready.return_value = True
        MockConnection.return_value = mock_conn2

        with pool.lease() as conn:
            pass

        assert MockConnection.call_count == 2

    pool.close()


def test_pool_validation():
    """Test pool input validation."""
    # Invalid secret (must be bytes)
    with pytest.raises(TypeError):
        ConnectionPool(access_id="test", secret="not_bytes")

    # Invalid max_connections
    with pytest.raises(ValueError):
        ConnectionPool(access_id="test", secret=b"secret", max_connections=0)

    # Invalid conn_timeout
    with pytest.raises(ValueError):
        ConnectionPool(access_id="test", secret=b"secret", conn_timeout=-1)

    # Invalid wait_timeout
    with pytest.raises(ValueError):
        ConnectionPool(access_id="test", secret=b"secret", wait_timeout=-1)


def test_lease_returns_on_exception():
    """Test that lease returns connection even on exception."""
    pool = ConnectionPool(access_id="test", secret=b"secret")

    import unittest.mock as mock

    with mock.patch('s3lib.Connection') as MockConnection:
        mock_conn = mock.Mock()
        mock_conn.conn = mock.Mock()
        mock_conn.conn.sock = mock.Mock()
        MockConnection.return_value = mock_conn

        # Lease with exception
        try:
            with pool.lease() as conn:
                raise ValueError("Test exception")
        except ValueError:
            pass

        # Connection should still be returned to pool
        stats = pool.stats()
        assert stats['available'] == 1
        assert stats['in_use'] == 0

    pool.close()


def test_pool_repr():
    """Test pool string representation."""
    pool = ConnectionPool(
        access_id="test",
        secret=b"secret",
        max_connections=10
    )

    repr_str = repr(pool)
    assert "ConnectionPool" in repr_str
    assert "max_connections=10" in repr_str
    assert "closed=False" in repr_str

    pool.close()
