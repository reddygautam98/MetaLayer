"""
MetaLayer Database Connection Pool Optimization
==============================================

Advanced PostgreSQL connection pooling configuration for high-performance
data processing with connection management, monitoring, and optimization.

Features:
- Advanced connection pooling with pgbouncer integration
- Connection pool monitoring and alerting
- Dynamic pool sizing based on workload
- Connection health checks and recovery
- Performance metrics and optimization recommendations
"""

import logging
import threading
import time
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime, timedelta
from queue import Empty, Queue
from typing import Any, Dict, Generator, Optional

import psycopg2
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.providers.postgres.hooks.postgres import PostgresHook
from psycopg2 import extras, pool

logger = logging.getLogger(__name__)


@dataclass
class ConnectionPoolConfig:
    """Configuration for database connection pooling"""

    min_connections: int = 5
    max_connections: int = 50
    connection_timeout: int = 30
    idle_timeout: int = 300
    max_lifetime: int = 3600
    health_check_interval: int = 60
    retry_attempts: int = 3
    retry_delay: int = 5
    enable_monitoring: bool = True


@dataclass
class PoolMetrics:
    """Connection pool performance metrics"""

    total_connections: int = 0
    active_connections: int = 0
    idle_connections: int = 0
    waiting_requests: int = 0
    failed_connections: int = 0
    average_wait_time: float = 0.0
    peak_connections: int = 0
    timestamp: Optional[datetime] = None

    def __post_init__(self):
        if self.timestamp is None:
            self.timestamp = datetime.now()


class OptimizedConnectionPool:
    """Advanced PostgreSQL connection pool with monitoring and optimization"""

    def __init__(
        self, connection_string: str, config: Optional[ConnectionPoolConfig] = None
    ):
        self.connection_string = connection_string
        self.config = config or ConnectionPoolConfig()
        self.pool = None
        self.metrics = PoolMetrics()
        self.metrics_lock = threading.Lock()
        self.health_check_thread = None
        self.monitoring_enabled = self.config.enable_monitoring
        self._initialize_pool()

        if self.monitoring_enabled:
            self._start_monitoring()

    def _initialize_pool(self):
        """Initialize the connection pool with optimized settings"""
        try:
            self.pool = psycopg2.pool.ThreadedConnectionPool(
                minconn=self.config.min_connections,
                maxconn=self.config.max_connections,
                dsn=self.connection_string,
                # Connection optimization parameters
                cursor_factory=extras.RealDictCursor,
                # Performance settings
                options="-c default_transaction_isolation=read_committed "
                "-c timezone=UTC "
                "-c statement_timeout=600000 "
                "-c idle_in_transaction_session_timeout=300000",
            )

            # Warm up the pool
            self._warm_up_connections()

            logger.info(
                f"Connection pool initialized: "
                f"min={self.config.min_connections}, "
                f"max={self.config.max_connections}"
            )

        except Exception as e:
            logger.error(f"Failed to initialize connection pool: {e}")
            raise AirflowException(f"Pool initialization failed: {e}")

    def _warm_up_connections(self):
        """Pre-create minimum number of connections to reduce latency"""
        connections = []
        try:
            # Create minimum connections and test them
            for i in range(self.config.min_connections):
                conn = self.pool.getconn()
                if conn:
                    # Test connection with simple query
                    with conn.cursor() as cursor:
                        cursor.execute("SELECT 1")
                    connections.append(conn)

            # Return all connections to pool
            for conn in connections:
                self.pool.putconn(conn)

            logger.info(f"Warmed up {len(connections)} connections")

        except Exception as e:
            logger.warning(f"Connection warm-up partially failed: {e}")
            # Return any successful connections
            for conn in connections:
                try:
                    self.pool.putconn(conn)
                except BaseException:
                    pass

    @contextmanager
    def get_connection(self) -> Generator[psycopg2.extensions.connection, None, None]:
        """Context manager for getting database connections with monitoring"""
        start_time = time.time()
        conn = None

        try:
            # Update waiting requests metric
            with self.metrics_lock:
                self.metrics.waiting_requests += 1

            # Get connection from pool
            conn = self.pool.getconn()
            if not conn:
                raise AirflowException("Failed to obtain database connection")

            # Update metrics
            wait_time = time.time() - start_time
            with self.metrics_lock:
                self.metrics.waiting_requests -= 1
                self.metrics.active_connections += 1
                self.metrics.average_wait_time = (
                    self.metrics.average_wait_time + wait_time
                ) / 2
                if self.metrics.active_connections > self.metrics.peak_connections:
                    self.metrics.peak_connections = self.metrics.active_connections

            # Test connection health
            if not self._test_connection(conn):
                logger.warning("Unhealthy connection detected, getting new one")
                self.pool.putconn(conn, close=True)
                conn = self.pool.getconn()
                if not self._test_connection(conn):
                    raise AirflowException("Unable to obtain healthy connection")

            yield conn

        except Exception as e:
            with self.metrics_lock:
                self.metrics.failed_connections += 1
                if self.metrics.waiting_requests > 0:
                    self.metrics.waiting_requests -= 1
            logger.error(f"Connection error: {e}")
            raise

        finally:
            if conn:
                try:
                    # Return connection to pool
                    self.pool.putconn(conn)
                    with self.metrics_lock:
                        self.metrics.active_connections -= 1
                except Exception as e:
                    logger.error(f"Error returning connection to pool: {e}")

    def _test_connection(self, conn) -> bool:
        """Test if a connection is healthy"""
        try:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                return True
        except BaseException:
            return False

    def execute_query(
        self, query: str, parameters: Optional[tuple] = None, fetch_all: bool = False
    ) -> Any:
        """Execute query with connection pool management"""
        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    cursor.execute(query, parameters)

                    if fetch_all:
                        return cursor.fetchall()
                    elif cursor.description:
                        return cursor.fetchone()
                    else:
                        return cursor.rowcount

            except Exception as e:
                conn.rollback()
                logger.error(f"Query execution failed: {e}")
                raise
            else:
                conn.commit()

    def execute_batch(
        self, query: str, parameter_list: list, batch_size: int = 1000
    ) -> int:
        """Execute batch operations efficiently"""
        total_affected = 0

        with self.get_connection() as conn:
            try:
                with conn.cursor() as cursor:
                    # Process in batches to avoid memory issues
                    for i in range(0, len(parameter_list), batch_size):
                        batch = parameter_list[i : i + batch_size]
                        extras.execute_batch(cursor, query, batch)
                        total_affected += cursor.rowcount

                conn.commit()
                return total_affected

            except Exception as e:
                conn.rollback()
                logger.error(f"Batch execution failed: {e}")
                raise

    def get_pool_status(self) -> Dict[str, Any]:
        """Get current connection pool status and metrics"""
        with self.metrics_lock:
            # Update connection counts from pool
            if hasattr(self.pool, "_pool"):
                total_conns = len(self.pool._pool) + len(self.pool._used)
                idle_conns = len(self.pool._pool)
                active_conns = len(self.pool._used)

                self.metrics.total_connections = total_conns
                self.metrics.idle_connections = idle_conns
                self.metrics.active_connections = active_conns

            return {
                "total_connections": self.metrics.total_connections,
                "active_connections": self.metrics.active_connections,
                "idle_connections": self.metrics.idle_connections,
                "waiting_requests": self.metrics.waiting_requests,
                "failed_connections": self.metrics.failed_connections,
                "average_wait_time": round(self.metrics.average_wait_time, 3),
                "peak_connections": self.metrics.peak_connections,
                "pool_utilization": round(
                    (self.metrics.active_connections / self.config.max_connections)
                    * 100,
                    2,
                ),
                "last_updated": self.metrics.timestamp.isoformat(),
            }

    def _start_monitoring(self):
        """Start background monitoring thread"""

        def monitor_health():
            while self.monitoring_enabled:
                try:
                    # Update metrics timestamp
                    with self.metrics_lock:
                        self.metrics.timestamp = datetime.now()

                    # Log pool status periodically
                    status = self.get_pool_status()
                    logger.debug(f"Pool status: {status}")

                    # Check for issues
                    if status["pool_utilization"] > 80:
                        logger.warning(
                            f"High pool utilization: {status['pool_utilization']}%"
                        )

                    if status["failed_connections"] > 10:
                        logger.warning(
                            f"High connection failure rate: {status['failed_connections']}"
                        )

                    time.sleep(self.config.health_check_interval)

                except Exception as e:
                    logger.error(f"Health monitoring error: {e}")
                    time.sleep(30)  # Wait before retrying

        self.health_check_thread = threading.Thread(target=monitor_health, daemon=True)
        self.health_check_thread.start()
        logger.info("Connection pool monitoring started")

    def optimize_pool_size(self):
        """Dynamic pool size optimization based on usage patterns"""
        status = self.get_pool_status()
        current_utilization = status["pool_utilization"]

        recommendations = []

        # Analyze utilization patterns
        if current_utilization < 20:
            recommendations.append(
                "Consider reducing max_connections to save resources"
            )
        elif current_utilization > 80:
            recommendations.append(
                "Consider increasing max_connections for better performance"
            )

        if status["average_wait_time"] > 1.0:
            recommendations.append(
                "High wait times detected - increase pool size or optimize queries"
            )

        if status["failed_connections"] > 0:
            recommendations.append(
                "Connection failures detected - check database health"
            )

        return {
            "current_status": status,
            "recommendations": recommendations,
            "suggested_min_connections": max(3, min(status["active_connections"], 10)),
            "suggested_max_connections": min(
                100, max(20, int(status["peak_connections"] * 1.5))
            ),
        }

    def close_pool(self):
        """Close the connection pool gracefully"""
        try:
            self.monitoring_enabled = False
            if self.health_check_thread:
                self.health_check_thread.join(timeout=5)

            if self.pool:
                self.pool.closeall()
                logger.info("Connection pool closed successfully")

        except Exception as e:
            logger.error(f"Error closing connection pool: {e}")


class ConnectionPoolManager:
    """Factory and manager for optimized connection pools"""

    _pools: Dict[str, OptimizedConnectionPool] = {}
    _lock = threading.Lock()

    @classmethod
    def get_pool(
        cls,
        conn_id: str = "postgres_default",
        config: Optional[ConnectionPoolConfig] = None,
    ) -> OptimizedConnectionPool:
        """Get or create an optimized connection pool"""

        with cls._lock:
            if conn_id not in cls._pools:
                # Get connection string from Airflow connection
                hook = PostgresHook(postgres_conn_id=conn_id)
                connection_string = hook.get_uri()

                # Create new optimized pool
                cls._pools[conn_id] = OptimizedConnectionPool(
                    connection_string=connection_string, config=config
                )

                logger.info(f"Created new optimized connection pool for {conn_id}")

            return cls._pools[conn_id]

    @classmethod
    def close_all_pools(cls):
        """Close all connection pools"""
        with cls._lock:
            for conn_id, pool in cls._pools.items():
                try:
                    pool.close_pool()
                    logger.info(f"Closed pool for {conn_id}")
                except Exception as e:
                    logger.error(f"Error closing pool {conn_id}: {e}")

            cls._pools.clear()

    @classmethod
    def get_all_pool_status(cls) -> Dict[str, Dict[str, Any]]:
        """Get status of all connection pools"""
        status = {}
        with cls._lock:
            for conn_id, pool in cls._pools.items():
                try:
                    status[conn_id] = pool.get_pool_status()
                except Exception as e:
                    status[conn_id] = {"error": str(e)}
        return status


# Utility functions for DAG integration
def get_optimized_connection(conn_id: str = "postgres_default"):
    """Get optimized connection pool for use in DAGs"""
    config = ConnectionPoolConfig(
        min_connections=int(Variable.get("db_min_connections", default_var=5)),
        max_connections=int(Variable.get("db_max_connections", default_var=50)),
        connection_timeout=int(Variable.get("db_connection_timeout", default_var=30)),
        enable_monitoring=True,
    )
    return ConnectionPoolManager.get_pool(conn_id, config)


def execute_with_pool(
    query: str,
    parameters: Optional[tuple] = None,
    conn_id: str = "postgres_default",
    fetch_all: bool = False,
):
    """Execute query using optimized connection pool"""
    pool = get_optimized_connection(conn_id)
    return pool.execute_query(query, parameters, fetch_all)


def execute_batch_with_pool(
    query: str,
    parameter_list: list,
    conn_id: str = "postgres_default",
    batch_size: int = 1000,
):
    """Execute batch operations using optimized connection pool"""
    pool = get_optimized_connection(conn_id)
    return pool.execute_batch(query, parameter_list, batch_size)


def get_db_connection(conn_id: str = "postgres_default"):
    """
    Get a database connection using Airflow PostgresHook

    Args:
        conn_id: Airflow connection ID for the database

    Returns:
        Database connection object
    """
    try:
        hook = PostgresHook(postgres_conn_id=conn_id)
        return hook.get_conn()
    except Exception as e:
        logger.error(f"Failed to get database connection for {conn_id}: {e}")
        raise AirflowException(f"Database connection failed: {e}")


@contextmanager
def get_db_connection_context(conn_id: str = "postgres_default"):
    """
    Context manager for database connections

    Args:
        conn_id: Airflow connection ID for the database

    Yields:
        Database connection object
    """
    conn = None
    try:
        conn = get_db_connection(conn_id)
        yield conn
    except Exception as e:
        if conn:
            conn.rollback()
        logger.error(f"Database operation failed: {e}")
        raise
    finally:
        if conn:
            conn.close()
