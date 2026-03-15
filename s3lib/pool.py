"""
Connection pooling for s3lib.

Provides thread-safe connection pooling with MRU (Most Recently Used) strategy
for efficient S3 operations.
"""

import threading
import time
from collections import deque


class ConnectionLease:
    """
    Context manager for leased connections.

    Automatically returns connection to pool on exit.
    Ensures connections are always returned, even on exceptions.

    Usage:
        with pool.lease() as conn:
            conn.put_object(bucket, key, data)
        # Connection automatically returned to pool
    """

    def __init__(self, connection, pool):
        """
        Create a connection lease.

        Args:
            connection: The Connection object to wrap
            pool: The ConnectionPool to return to
        """
        self._connection = connection
        self._pool = pool
        self._entered = False

    def __enter__(self):
        """
        Enter context: return the wrapped connection.

        Returns:
            Connection: The leased connection for use
        """
        self._entered = True
        return self._connection

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit context: return connection to pool.

        Always returns connection, even on exception.
        Does not suppress exceptions.

        Returns:
            False: Don't suppress exceptions
        """
        if self._entered:
            # Return connection to pool (thread-safe)
            self._pool._return_connection(self._connection)

        return False  # Don't suppress exceptions


class ConnectionPool:
    """
    Thread-safe pool of S3 Connection objects.

    Manages a set of reusable Connection objects, handing them out to callers
    via leases and returning them to the pool when the lease exits. Callers
    never interact with the pool directly beyond acquiring and releasing leases.

    The pool uses Connection.is_ready() to decide whether a returned connection
    can be recycled. If a caller returns a lease while an S3ByteStream is still
    open and unexhausted, the connection is not ready and is discarded rather
    than recycled. A fresh connection will be created on the next lease.

    Connections are allocated using an MRU (Most Recently Used) strategy —
    the most recently returned connection is leased first, keeping hot
    connections warm and minimising reconnects.

    Usage:
        with ConnectionPool(access_id, secret, max_connections=10) as pool:
            with pool.lease() as conn:
                result = conn.put_object2(bucket, key, data)
            with pool.lease() as conn:
                stream, headers = conn.get_object2(bucket, key)
                with stream:
                    data = stream.read()
    """

    def __init__(self, access_id, secret, host=None, port=None,
                 max_connections=10, conn_timeout=60, wait_timeout=30, use_ssl=True):
        """
        Initialize thread-safe connection pool.

        Args:
            access_id (str): AWS access key ID
            secret (bytes): AWS secret access key
            host (str, optional): S3 hostname. Defaults to "s3.amazonaws.com"
            port (int, optional): S3 port. Defaults to 443 (HTTPS) or 80 (HTTP)
            max_connections (int): Maximum total connections (available + in-use).
                Defaults to 10
            conn_timeout (int): Timeout for individual connections in seconds.
                Defaults to 60
            wait_timeout (int): Maximum time to wait for available connection
                in seconds. Defaults to 30
            use_ssl (bool): Use HTTPS if True (default), HTTP if False
        """
        # Validate inputs
        if not isinstance(secret, bytes):
            raise TypeError("secret must be bytes")
        if max_connections < 1:
            raise ValueError("max_connections must be at least 1")
        if conn_timeout < 0:
            raise ValueError("conn_timeout must be non-negative")
        if wait_timeout < 0:
            raise ValueError("wait_timeout must be non-negative")

        # Connection configuration
        self.access_id = access_id
        self.secret = secret
        self.use_ssl = use_ssl
        self.host = host or "s3.amazonaws.com"
        self.port = port or (443 if use_ssl else 80)
        self.max_connections = max_connections
        self.conn_timeout = conn_timeout
        self.wait_timeout = wait_timeout

        # Thread-safe data structures
        # Use deque for O(1) append/pop (LIFO = MRU strategy)
        self._available = deque()  # Available connections (MRU stack)
        self._in_use = set()       # Connections currently leased
        self._all_connections = set()  # All connections ever created

        # Thread synchronization primitives
        # RLock allows same thread to acquire lock multiple times (reentrant)
        self._lock = threading.RLock()
        # Condition variable for efficient waiting
        self._condition = threading.Condition(self._lock)

        # Pool state
        self._closed = False

    def lease(self):
        """
        Lease a connection from the pool.

        Thread-safe. Blocks if pool is exhausted, up to wait_timeout.
        Returns most recently used connection (MRU strategy) for best
        performance (hot connections, warm TCP state).

        Returns:
            ConnectionLease: Context manager wrapping a connection

        Raises:
            RuntimeError: If pool is closed
            TimeoutError: If no connection available within wait_timeout

        Example:
            with pool.lease() as conn:
                conn.put_object(bucket, key, data)
        """
        with self._lock:  # Acquire lock for entire operation
            # Check pool state
            if self._closed:
                raise RuntimeError("Cannot lease from closed ConnectionPool")

            # Calculate deadline for timeout
            deadline = time.time() + self.wait_timeout

            while True:
                # Try to get existing available connection
                if self._available:
                    conn = self._get_available_connection()
                    if conn:
                        # Got valid connection
                        return ConnectionLease(conn, self)
                    # Connection was invalid, try again
                    continue

                # Try to create new connection if under limit
                if len(self._all_connections) < self.max_connections:
                    conn = self._create_new_connection()
                    return ConnectionLease(conn, self)

                # Pool exhausted, wait for a connection to be returned
                remaining = deadline - time.time()
                if remaining <= 0:
                    raise TimeoutError(
                        f"No connection available within {self.wait_timeout}s "
                        f"(pool size: {len(self._all_connections)}, "
                        f"in-use: {len(self._in_use)}, "
                        f"available: {len(self._available)})"
                    )

                # Wait for notification (released when connection returned)
                # This releases lock, sleeps, and reacquires lock when notified
                self._condition.wait(timeout=remaining)

                # Check again if pool was closed while waiting
                if self._closed:
                    raise RuntimeError("Pool was closed while waiting for connection")

    def _get_available_connection(self):
        """
        Get connection from available pool (MRU - most recently used).

        Must be called with lock held.
        Validates connection before returning.

        Returns:
            Connection or None: Valid connection, or None if connection is invalid
        """
        # Pop from right end (LIFO = MRU)
        conn = self._available.pop()

        # Validate connection is still alive
        if self._is_connection_valid(conn):
            # Move to in-use set
            self._in_use.add(conn)
            return conn
        else:
            # Connection is dead, discard it
            self._all_connections.discard(conn)
            try:
                conn._disconnect()
            except Exception:
                pass  # Best effort cleanup
            return None

    def _create_new_connection(self):
        """
        Create a new Connection and add it to the pool as in-use.

        Must be called with lock held.

        Connection objects connect lazily — no socket is opened here.
        The socket will be established on the first request made against
        the connection.

        Returns:
            Connection: Newly created connection, ready for use

        Raises:
            Any exception from Connection construction
        """
        # Import here to avoid circular dependency
        from . import Connection

        conn = Connection(
            self.access_id,
            self.secret,
            self.host,
            self.port,
            self.conn_timeout,
            use_ssl=self.use_ssl
        )

        # TODO: remove conn._connect() once lazy connect is implemented.
        # The pool should not need to call this — the connection will
        # establish its socket on the first request automatically.
        conn._connect()

        self._all_connections.add(conn)
        self._in_use.add(conn)

        return conn

    def _is_connection_valid(self, conn):
        """
        Check if connection is ready to be recycled back into the pool.

        Delegates to conn.is_ready() — the pool does not inspect Connection
        internals directly. Connection owns the definition of readiness.

        Args:
            conn: Connection to check

        Returns:
            bool: True if connection is ready for a new request
        """
        try:
            # Check if connection has valid HTTPConnection with open socket
            # TODO: replace with conn.is_ready() once implemented
            return (conn.conn is not None and
                    hasattr(conn.conn, 'sock') and
                    conn.conn.sock is not None)
        except Exception:
            return False

    def _return_connection(self, conn):
        """
        Return connection to pool.

        Called by ConnectionLease on exit.
        Thread-safe.

        Validates connection before returning to pool.
        Invalid connections are discarded.
        Notifies waiting threads that a connection is available.

        Args:
            conn: Connection to return
        """
        with self._lock:
            # Remove from in-use set
            self._in_use.discard(conn)

            # If pool is closed, discard connection
            if self._closed:
                self._all_connections.discard(conn)
                try:
                    conn._disconnect()
                except Exception:
                    pass  # Best effort
                return

            # Validate before returning to pool
            if self._is_connection_valid(conn):
                # Connection is valid, return to available pool
                # Append to right (MRU - most recently used goes on top)
                self._available.append(conn)
            else:
                # Connection is invalid, discard it
                self._all_connections.discard(conn)
                try:
                    conn._disconnect()
                except Exception:
                    pass  # Best effort

            # Notify one waiting thread that a connection is available
            self._condition.notify()

    def close(self):
        """
        Close all connections and shut down pool.

        Thread-safe and idempotent (safe to call multiple times).
        Closes all connections (available + in-use).
        Notifies all waiting threads.

        After close(), lease() will raise RuntimeError.
        """
        with self._lock:
            if self._closed:
                return  # Already closed

            # Mark as closed
            self._closed = True

            # Close all connections (available + in-use)
            all_conns = list(self._all_connections)

            for conn in all_conns:
                try:
                    conn._disconnect()
                except Exception:
                    pass  # Best effort cleanup

            # Clear all tracking structures
            self._available.clear()
            self._in_use.clear()
            self._all_connections.clear()

            # Notify all waiting threads (they will get RuntimeError)
            self._condition.notify_all()

    @property
    def closed(self):
        """
        Check if pool is closed.

        Thread-safe.

        Returns:
            bool: True if pool is closed
        """
        with self._lock:
            return self._closed

    def stats(self):
        """
        Get pool statistics.

        Thread-safe. Useful for monitoring and debugging.

        Returns:
            dict: Pool statistics with keys:
                - total_connections: Total connections created
                - available: Connections available for lease
                - in_use: Connections currently leased
                - max_connections: Configured maximum
                - closed: Pool closed state (bool)

        Example:
            stats = pool.stats()
            print(f"Pool: {stats['in_use']}/{stats['total_connections']} in use")
        """
        with self._lock:
            return {
                'total_connections': len(self._all_connections),
                'available': len(self._available),
                'in_use': len(self._in_use),
                'max_connections': self.max_connections,
                'closed': self._closed
            }

    def __enter__(self):
        """
        Context manager entry.

        Returns:
            ConnectionPool: self

        Example:
            with ConnectionPool(access_id, secret) as pool:
                with pool.lease() as conn:
                    conn.put_object(bucket, key, data)
        """
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Context manager exit.

        Closes pool automatically.
        Does not suppress exceptions.

        Returns:
            False: Don't suppress exceptions
        """
        self.close()
        return False

    def __repr__(self):
        """String representation for debugging."""
        with self._lock:
            return (f"ConnectionPool(host={self.host}, "
                    f"max_connections={self.max_connections}, "
                    f"total={len(self._all_connections)}, "
                    f"available={len(self._available)}, "
                    f"in_use={len(self._in_use)}, "
                    f"closed={self._closed})")
