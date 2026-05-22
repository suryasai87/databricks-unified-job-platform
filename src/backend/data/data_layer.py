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
    # Synced tables live in database "jobs_monitor", schema "cost_management"
    TABLE_MAPPINGS = {
        "system.lakeflow.jobs": "cost_management.lb_jobs_latest",
        "system.lakeflow.job_run_timeline": "cost_management.lb_job_runs_latest",
    }

    def __init__(
        self,
        host: str,
        warehouse_id: str,
        catalog: str = "main",
        schema: str = "cost_management",
        lakebase_instance_name: Optional[str] = None,
        cache_ttl: int = 300,
    ):
        self.host = host
        self.warehouse_id = warehouse_id
        self.catalog = catalog
        self.schema = schema
        self.lakebase_instance_name = lakebase_instance_name
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
        if lakebase_instance_name:
            self._init_lakebase()

    def _init_workspace_client(self):
        """Initialize Databricks WorkspaceClient."""
        try:
            # Use default WorkspaceClient() to auto-discover auth from
            # Databricks App runtime environment (M2M service principal).
            # Only override host if DATABRICKS_HOST is not already set.
            if os.getenv("DATABRICKS_HOST"):
                self._workspace_client = WorkspaceClient()
            else:
                config = Config(
                    host=f"https://{self.host}",
                    http_timeout_seconds=120,
                )
                self._workspace_client = WorkspaceClient(config=config)
            host = self._workspace_client.config.host
            print(f"WorkspaceClient initialized for {host}")
        except Exception as e:
            print(f"Failed to initialize WorkspaceClient: {e}")
            raise

    def _init_lakebase(self):
        """Initialize Lakebase PostgreSQL connection pool with automatic token refresh.

        Uses psycopg3's ConnectionPool with a custom connection class that
        generates a fresh OAuth token on every new connection, avoiding the
        stale-token problem (tokens expire after 1 hour).
        """
        try:
            import psycopg
            from psycopg_pool import ConnectionPool

            # Get Lakebase connection info from Databricks API
            lakebase_info = self._get_lakebase_connection_info()

            if not lakebase_info:
                print("Could not retrieve Lakebase connection info")
                return

            # Determine the PostgreSQL user identity
            pg_user = os.getenv("LAKEBASE_USER", "")
            if not pg_user:
                try:
                    me = self._workspace_client.current_user.me()
                    pg_user = me.user_name or "token"
                except Exception:
                    pg_user = "token"

            host = lakebase_info.get("host")
            port = lakebase_info.get("port", 5432)
            database = lakebase_info.get("database", "postgres")

            # Check for static password (non-expiring native Postgres password)
            static_password = os.getenv("LAKEBASE_PASSWORD", "")

            if static_password:
                # Static credentials — no token refresh needed
                conninfo = (
                    f"host={host} port={port} dbname={database} "
                    f"user={pg_user} password={static_password} "
                    f"sslmode=require connect_timeout=10 "
                    f"options='-c statement_timeout=10000'"
                )
                self._lakebase_pool = ConnectionPool(
                    conninfo=conninfo,
                    min_size=2,
                    max_size=10,
                    open=True,
                )
            else:
                # OAuth token auth — generate a fresh token per connection
                workspace_client = self._workspace_client
                instance_name = self.lakebase_instance_name

                class OAuthConnection(psycopg.Connection):
                    """Connection subclass that fetches a fresh OAuth token on connect."""
                    @classmethod
                    def connect(cls, conninfo='', **kwargs):
                        import uuid
                        try:
                            cred = workspace_client.api_client.do(
                                "POST",
                                "/api/2.0/database/credentials",
                                body={
                                    "instance_names": [instance_name],
                                    "request_id": str(uuid.uuid4()),
                                },
                            )
                            token = cred.get("token") if isinstance(cred, dict) else None
                            if token:
                                print("Lakebase: obtained fresh credential via database API")
                            else:
                                print("Lakebase: credential API returned no token")
                        except Exception as e:
                            print(f"Lakebase: credential API failed: {e}")
                            token = None

                        if not token:
                            print("Lakebase: falling back to SDK config token")
                            token = workspace_client.config.token

                        kwargs["password"] = token
                        return super().connect(conninfo, **kwargs)

                conninfo = (
                    f"host={host} port={port} dbname={database} "
                    f"user={pg_user} sslmode=require connect_timeout=10 "
                    f"options='-c statement_timeout=10000'"
                )
                self._lakebase_pool = ConnectionPool(
                    conninfo=conninfo,
                    connection_class=OAuthConnection,
                    min_size=1,
                    max_size=10,
                    max_idle=600,
                    open=True,
                )

            self._lakebase_available = True
            print(f"Lakebase connection pool initialized: {host}")

        except ImportError:
            print("psycopg[pool] not installed - Lakebase disabled")
        except Exception as e:
            print(f"Failed to initialize Lakebase: {e}")

    def _get_lakebase_connection_info(self) -> Optional[Dict[str, Any]]:
        """Get Lakebase instance connection details from Databricks API."""
        try:
            response = self._workspace_client.api_client.do(
                "GET",
                f"/api/2.0/database/instances/{self.lakebase_instance_name}",
            )

            if isinstance(response, dict):
                return {
                    "host": response.get("read_write_dns") or response.get("dns"),
                    "port": 5432,
                    "database": os.getenv("LAKEBASE_DATABASE", self.catalog),
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
        """Translate Databricks SQL syntax to PostgreSQL for Lakebase."""
        import re

        translated = query
        for uc_table, lb_table in self.TABLE_MAPPINGS.items():
            translated = translated.replace(uc_table, lb_table)

        # Also translate custom catalog.schema tables
        # In Lakebase, we're connected to jobs_monitor DB, so just use schema.table
        translated = translated.replace(
            f"{self.catalog}.{self.schema}.",
            f"{self.schema}."
        )

        # Translate INTERVAL syntax: INTERVAL N DAY/HOUR/WEEK -> INTERVAL 'N days/hours/weeks'
        def _fix_interval(match):
            value = match.group(1)
            unit = match.group(2).lower() + "s"  # DAY -> days, HOUR -> hours
            return f"INTERVAL '{value} {unit}'"

        translated = re.sub(
            r"INTERVAL\s+(\d+)\s+(DAY|HOUR|WEEK|MONTH|YEAR|MINUTE|SECOND)",
            _fix_interval,
            translated,
            flags=re.IGNORECASE,
        )

        # Translate Databricks struct access to PostgreSQL JSONB syntax.
        # Lakebase syncs Delta structs as JSONB columns.
        _numeric_fields = {"default"}
        _boolean_fields = {"is_serverless"}
        _struct_columns = r"pricing|usage_metadata|identity_metadata|product_features"

        def _cast_jsonb_field(accessor: str, field: str) -> str:
            if field in _numeric_fields:
                return f"({accessor})::numeric"
            if field in _boolean_fields:
                return f"({accessor})::boolean"
            return accessor

        def _struct_to_jsonb(match):
            prefix = match.group(1)
            struct = match.group(2)
            field = match.group(3)
            accessor = f"{prefix}.{struct}->>'{field}'"
            return _cast_jsonb_field(accessor, field)

        # alias.struct_col.field -> alias.struct_col->>'field'
        translated = re.sub(
            rf"\b(\w+)\.({_struct_columns})\.(\w+)\b",
            _struct_to_jsonb,
            translated,
        )

        # Bare struct_col.field (no alias) -> struct_col->>'field'
        def _bare_struct_to_jsonb(match):
            struct = match.group(1)
            field = match.group(2)
            accessor = f"{struct}->>'{field}'"
            return _cast_jsonb_field(accessor, field)

        translated = re.sub(
            rf"(?<!\.)(?<!\w)\b({_struct_columns})\.(\w+)\b",
            _bare_struct_to_jsonb,
            translated,
        )

        # Translate FIRST_VALUE() to MIN() for PostgreSQL compatibility.
        # Databricks allows FIRST_VALUE as aggregate; PG requires OVER clause.
        translated = re.sub(
            r"\bFIRST_VALUE\s*\(",
            "MIN(",
            translated,
            flags=re.IGNORECASE,
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

    def _is_lakebase_eligible(self, query: str) -> bool:
        """Check if a query only uses tables that are synced to Lakebase."""
        import re

        # Skip queries referencing custom catalog.schema tables (not synced)
        catalog_schema = f"{self.catalog}.{self.schema}."
        if catalog_schema in query:
            return False

        # Skip queries referencing system tables that aren't in TABLE_MAPPINGS
        system_tables = re.findall(r"system\.\w+\.\w+", query)
        if system_tables:
            for table in system_tables:
                if table not in self.TABLE_MAPPINGS:
                    return False

        # Must reference at least one mapped table
        return any(t in query for t in self.TABLE_MAPPINGS)

    def execute_query(
        self,
        query: str,
        params: Optional[Dict] = None,
        use_cache: bool = True,
        prefer_lakebase: bool = True,
        lakebase_query: Optional[str] = None,
    ) -> QueryResult:
        """
        Execute a query using the best available data source.

        Args:
            query: SQL query (Databricks SQL syntax) — used for warehouse
            params: Optional query parameters
            use_cache: Whether to use caching
            prefer_lakebase: Whether to prefer Lakebase over SQL Warehouse
            lakebase_query: Optional PostgreSQL query for Lakebase materialized views.
                           When provided, this runs on Lakebase instead of translating `query`.

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
        use_lakebase = prefer_lakebase and self.lakebase_available
        if use_lakebase and (lakebase_query or self._is_lakebase_eligible(query)):
            try:
                result = self._execute_lakebase(
                    lakebase_query or query, params,
                    skip_translate=bool(lakebase_query),
                )
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

    def _execute_lakebase(self, query: str, params: Optional[Dict] = None, skip_translate: bool = False) -> QueryResult:
        """Execute query on Lakebase PostgreSQL."""
        if not self._lakebase_pool:
            raise RuntimeError("Lakebase pool not initialized")

        start_time = time.time()
        translated_query = query if skip_translate else self._translate_query_for_lakebase(query)

        with self._lakebase_pool.connection() as conn:
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
            row_limit=10000,
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
            (f"{self.catalog}.{self.schema}.job_runs_latest", "Job run history"),
            (f"{self.catalog}.{self.schema}.jobs_latest", "Job definitions"),
            (f"{self.catalog}.{self.schema}.billing_usage_enriched", "Billing cost data"),
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
        test_query = f"SELECT COUNT(*) as cnt FROM {self.catalog}.{self.schema}.jobs_latest"

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
            self._lakebase_pool.close()
            print("Lakebase connection pool closed")
