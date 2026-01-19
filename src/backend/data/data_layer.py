"""
Unified Data Layer
==================
Provides transparent access to data via Lakebase (preferred) or SQL Warehouse (fallback).
Implements circuit breaker pattern for automatic failover.
"""
import os
import time
from typing import Any, Dict, List, Optional, Tuple
from dataclasses import dataclass
from enum import Enum

# Databricks SDK
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState
from databricks.sdk.config import Config


class DataSource(Enum):
    LAKEBASE = "lakebase"
    WAREHOUSE = "warehouse"


@dataclass
class QueryResult:
    """Result of a query execution."""
    columns: List[str]
    data: List[List[Any]]
    source: DataSource
    execution_time_ms: float
    from_cache: bool = False


class UnifiedDataLayer:
    """
    Unified data access layer with Lakebase and SQL Warehouse support.

    Features:
    - Automatic failover from Lakebase to SQL Warehouse
    - Connection pooling for Lakebase (PostgreSQL)
    - Query caching with configurable TTL
    - Circuit breaker pattern for fault tolerance
    - Table name translation between sources
    """

    # Table mappings: Unity Catalog -> Lakebase synced tables
    TABLE_MAPPINGS = {
        "system.lakeflow.jobs": "jobs_monitor.synced.jobs",
        "system.lakeflow.job_run_timeline": "jobs_monitor.synced.job_run_timeline",
        "system.lakeflow.job_task_run_timeline": "jobs_monitor.synced.job_task_run_timeline",
        "system.billing.usage": "jobs_monitor.synced.billing_usage",
        "system.billing.list_prices": "jobs_monitor.synced.list_prices",
        "system.compute.clusters": "jobs_monitor.synced.clusters",
    }

    def __init__(
        self,
        host: str,
        warehouse_id: str,
        catalog: str = "hls_amer_catalog",
        schema: str = "cost_management",
        lakebase_instance_id: Optional[str] = None,
        cache_ttl: int = 300,
    ):
        self.host = host
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.lakebase_instance_id = lakebase_instance_id
        self.cache_ttl = cache_ttl

        # Initialize clients
        self._workspace_client = None
        self._lakebase_pool = None
        self._lakebase_available = False

        # Cache
        self._cache: Dict[str, Tuple[Any, float]] = {}

        # Circuit breaker state
        self._lakebase_failures = 0
        self._lakebase_last_failure = 0
        self._circuit_open = False
        self._circuit_open_time = 0
        self._failure_threshold = 3
        self._circuit_reset_timeout = 60  # seconds

        # Initialize connections
        self._init_workspace_client()
        if lakebase_instance_id:
            self._init_lakebase()

    def _init_workspace_client(self):
        """Initialize Databricks WorkspaceClient."""
        try:
            config = Config(
                host=f"https://{self.host}",
                http_timeout_seconds=120,
            )
            self._workspace_client = WorkspaceClient(config=config)
            print(f"WorkspaceClient initialized for {self.host}")
        except Exception as e:
            print(f"Failed to initialize WorkspaceClient: {e}")
            raise

    def _init_lakebase(self):
        """Initialize Lakebase PostgreSQL connection pool."""
        try:
            import psycopg2
            from psycopg2 import pool

            # Get Lakebase connection info from Databricks API
            lakebase_info = self._get_lakebase_connection_info()

            if not lakebase_info:
                print("Could not retrieve Lakebase connection info")
                return

            # Get token for authentication
            token = os.getenv("DATABRICKS_TOKEN")
            if not token:
                # Try to get token from SDK config
                try:
                    token = self._workspace_client.config.token
                except Exception:
                    pass

            if not token:
                print("No Databricks token available for Lakebase auth")
                return

            # Create connection pool
            self._lakebase_pool = pool.ThreadedConnectionPool(
                minconn=2,
                maxconn=10,
                host=lakebase_info.get("host"),
                port=lakebase_info.get("port", 5432),
                database=lakebase_info.get("database", "postgres"),
                user="token",
                password=token,
                sslmode="require",
                connect_timeout=10,
                options="-c statement_timeout=30000",
            )

            self._lakebase_available = True
            print(f"Lakebase connection pool initialized: {lakebase_info.get('host')}")

        except ImportError:
            print("psycopg2 not installed - Lakebase disabled")
        except Exception as e:
            print(f"Failed to initialize Lakebase: {e}")

    def _get_lakebase_connection_info(self) -> Optional[Dict[str, Any]]:
        """Get Lakebase instance connection details from Databricks API."""
        try:
            # Use database API to get instance info
            response = self._workspace_client.api_client.do(
                "GET",
                f"/api/2.0/database/instances/{self.lakebase_instance_id}",
            )

            if isinstance(response, dict):
                return {
                    "host": response.get("read_write_dns") or response.get("dns"),
                    "port": 5432,
                    "database": "postgres",
                    "read_only_host": response.get("read_only_dns"),
                }
        except Exception as e:
            print(f"Failed to get Lakebase info: {e}")

        return None

    @property
    def lakebase_available(self) -> bool:
        """Check if Lakebase is available and circuit is closed."""
        if not self._lakebase_available:
            return False

        # Check circuit breaker
        if self._circuit_open:
            # Check if we should try to reset
            if time.time() - self._circuit_open_time > self._circuit_reset_timeout:
                self._circuit_open = False
                self._lakebase_failures = 0
                print("Lakebase circuit breaker reset - attempting reconnection")
            else:
                return False

        return True

    @property
    def current_source(self) -> str:
        """Get current active data source."""
        return "lakebase" if self.lakebase_available else "warehouse"

    def _record_lakebase_failure(self):
        """Record a Lakebase failure for circuit breaker."""
        self._lakebase_failures += 1
        self._lakebase_last_failure = time.time()

        if self._lakebase_failures >= self._failure_threshold:
            self._circuit_open = True
            self._circuit_open_time = time.time()
            print(f"Lakebase circuit breaker OPEN after {self._lakebase_failures} failures")

    def _translate_query_for_lakebase(self, query: str) -> str:
        """Translate Unity Catalog table names to Lakebase synced table names."""
        translated = query
        for uc_table, lb_table in self.TABLE_MAPPINGS.items():
            translated = translated.replace(uc_table, lb_table)

        # Also translate custom catalog.schema tables
        translated = translated.replace(
            f"{self.catalog}.{self.schema}.",
            "jobs_monitor.cost_management."
        )

        return translated

    def _get_cache_key(self, query: str, params: Optional[Dict] = None) -> str:
        """Generate cache key for a query."""
        import hashlib
        key_str = query + str(params or "")
        return hashlib.md5(key_str.encode()).hexdigest()

    def _get_cached(self, cache_key: str) -> Optional[QueryResult]:
        """Get result from cache if valid."""
        if cache_key in self._cache:
            result, timestamp = self._cache[cache_key]
            if time.time() - timestamp < self.cache_ttl:
                result.from_cache = True
                return result
            else:
                del self._cache[cache_key]
        return None

    def _set_cache(self, cache_key: str, result: QueryResult):
        """Store result in cache."""
        self._cache[cache_key] = (result, time.time())

    def clear_cache(self):
        """Clear all cached results."""
        self._cache.clear()

    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        use_cache: bool = True,
        prefer_lakebase: bool = True,
    ) -> QueryResult:
        """
        Execute a query using the best available data source.

        Args:
            query: SQL query to execute
            params: Optional query parameters
            use_cache: Whether to use caching
            prefer_lakebase: Whether to prefer Lakebase over SQL Warehouse

        Returns:
            QueryResult with columns, data, and metadata
        """
        # Check cache first
        if use_cache:
            cache_key = self._get_cache_key(query, params)
            cached = self._get_cached(cache_key)
            if cached:
                return cached

        # Try Lakebase first if available and preferred
        if prefer_lakebase and self.lakebase_available:
            try:
                result = self._execute_lakebase(query, params)
                if use_cache:
                    self._set_cache(cache_key, result)
                return result
            except Exception as e:
                print(f"Lakebase query failed, falling back to warehouse: {e}")
                self._record_lakebase_failure()

        # Fall back to SQL Warehouse
        result = self._execute_warehouse(query, params)
        if use_cache:
            self._set_cache(cache_key, result)
        return result

    def _execute_lakebase(self, query: str, params: Optional[Dict] = None) -> QueryResult:
        """Execute query on Lakebase PostgreSQL."""
        if not self._lakebase_pool:
            raise RuntimeError("Lakebase pool not initialized")

        start_time = time.time()
        conn = self._lakebase_pool.getconn()

        try:
            translated_query = self._translate_query_for_lakebase(query)

            with conn.cursor() as cursor:
                cursor.execute(translated_query, params)
                columns = [desc[0] for desc in cursor.description] if cursor.description else []
                data = cursor.fetchall() if cursor.description else []

            execution_time = (time.time() - start_time) * 1000

            return QueryResult(
                columns=columns,
                data=[list(row) for row in data],
                source=DataSource.LAKEBASE,
                execution_time_ms=execution_time,
            )

        finally:
            self._lakebase_pool.putconn(conn)

    def _execute_warehouse(self, query: str, params: Optional[Dict] = None) -> QueryResult:
        """Execute query on SQL Warehouse."""
        start_time = time.time()

        # Parameterize query if needed
        if params:
            for key, value in params.items():
                if isinstance(value, str):
                    query = query.replace(f":{key}", f"'{value}'")
                else:
                    query = query.replace(f":{key}", str(value))

        response = self._workspace_client.statement_execution.execute_statement(
            warehouse_id=self.warehouse_id,
            statement=query,
            wait_timeout="50s",
        )

        execution_time = (time.time() - start_time) * 1000

        if response.status.state != StatementState.SUCCEEDED:
            error_msg = response.status.error.message if response.status.error else "Unknown error"
            raise RuntimeError(f"Query failed: {error_msg}")

        columns = []
        data = []

        if response.manifest and response.manifest.schema and response.manifest.schema.columns:
            columns = [col.name for col in response.manifest.schema.columns]

        if response.result and response.result.data_array:
            data = response.result.data_array

        return QueryResult(
            columns=columns,
            data=data,
            source=DataSource.WAREHOUSE,
            execution_time_ms=execution_time,
        )

    def check_table_access(self) -> List[Dict[str, Any]]:
        """Check access to required tables."""
        tables_to_check = [
            ("system.lakeflow.job_run_timeline", "Job run history"),
            ("system.lakeflow.jobs", "Job definitions"),
            ("system.billing.usage", "Billing data"),
            (f"{self.catalog}.{self.schema}.serverless_tag_correlation", "Tag correlation"),
        ]

        results = []
        for table, description in tables_to_check:
            try:
                self.execute_query(f"SELECT 1 FROM {table} LIMIT 1", use_cache=False)
                results.append({
                    "table": table,
                    "description": description,
                    "accessible": True,
                })
            except Exception as e:
                results.append({
                    "table": table,
                    "description": description,
                    "accessible": False,
                    "error": str(e),
                })

        return results

    def get_performance_comparison(self) -> Dict[str, Any]:
        """Compare query performance between Lakebase and SQL Warehouse."""
        test_query = "SELECT COUNT(*) as cnt FROM system.lakeflow.jobs WHERE delete_time IS NULL"

        results = {"lakebase": None, "warehouse": None, "speedup_factor": None}

        # Test warehouse
        try:
            wh_result = self._execute_warehouse(test_query)
            results["warehouse"] = {
                "execution_time_ms": wh_result.execution_time_ms,
                "status": "success",
            }
        except Exception as e:
            results["warehouse"] = {"status": "error", "error": str(e)}

        # Test Lakebase
        if self.lakebase_available:
            try:
                lb_result = self._execute_lakebase(test_query)
                results["lakebase"] = {
                    "execution_time_ms": lb_result.execution_time_ms,
                    "status": "success",
                }
            except Exception as e:
                results["lakebase"] = {"status": "error", "error": str(e)}

        # Calculate speedup
        if (results["lakebase"] and results["warehouse"] and
            results["lakebase"].get("status") == "success" and
            results["warehouse"].get("status") == "success"):
            lb_time = results["lakebase"]["execution_time_ms"]
            wh_time = results["warehouse"]["execution_time_ms"]
            if lb_time > 0:
                results["speedup_factor"] = round(wh_time / lb_time, 2)

        return results

    def close(self):
        """Close all connections."""
        if self._lakebase_pool:
            self._lakebase_pool.closeall()
            print("Lakebase connection pool closed")
