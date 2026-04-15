"""
Unified Job Platform - FastAPI Backend
=======================================
Job monitoring platform with Lakebase integration for sub-100ms query performance.
"""
import os
import time
import json
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional
from contextlib import asynccontextmanager

from fastapi import FastAPI, HTTPException, Request, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse, JSONResponse
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Import routers
from routers import jobs, health, genie
from data.data_layer import UnifiedDataLayer

# Configuration
DATABRICKS_HOST = os.getenv("DATABRICKS_HOST", "")
CATALOG = os.getenv("CATALOG", "main")
SCHEMA = os.getenv("SCHEMA", "cost_management")
WAREHOUSE_ID = os.getenv("WAREHOUSE_ID", "")
LAKEBASE_INSTANCE_NAME = os.getenv("LAKEBASE_INSTANCE_NAME", "")
LAKEBASE_ENABLED = os.getenv("LAKEBASE_ENABLED", "true").lower() == "true"
CACHE_TTL = int(os.getenv("CACHE_TTL", "300"))

# Initialize data layer
data_layer: Optional[UnifiedDataLayer] = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Initialize and cleanup resources."""
    global data_layer
    print("Initializing Unified Job Platform...")

    # Initialize data layer with Lakebase support
    data_layer = UnifiedDataLayer(
        host=DATABRICKS_HOST,
        warehouse_id=WAREHOUSE_ID,
        catalog=CATALOG,
        schema=SCHEMA,
        lakebase_instance_name=LAKEBASE_INSTANCE_NAME if LAKEBASE_ENABLED else None,
        cache_ttl=CACHE_TTL,
    )

    print(f"Data layer initialized (Lakebase: {LAKEBASE_ENABLED})")
    yield

    # Cleanup
    if data_layer:
        data_layer.close()
    print("Unified Job Platform shutdown complete")


# Create FastAPI app
app = FastAPI(
    title="Unified Job Platform",
    description="Enterprise Databricks Job Monitoring Platform",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# Pydantic models
class User(BaseModel):
    email: str
    name: Optional[str] = None
    source: str = "unknown"
    authenticated: bool = False


class HealthResponse(BaseModel):
    status: str
    data_source: str
    lakebase_enabled: bool
    cache_ttl: int
    timestamp: str


class DataAccessError(BaseModel):
    error: str
    error_code: str
    message: str
    resolution: str
    tables_affected: List[str]


# Helper function to get current user
def get_current_user(request: Request) -> User:
    """Extract user info from request headers (Databricks Apps SSO)."""
    # Try different auth methods
    email = request.headers.get("x-forwarded-email")
    name = request.headers.get("x-forwarded-name")

    if email:
        return User(email=email, name=name, source="OBO", authenticated=True)

    # Try JWT token
    token = request.headers.get("x-forwarded-access-token")
    if token:
        try:
            import jwt
            decoded = jwt.decode(token, options={"verify_signature": False})
            return User(
                email=decoded.get("email", decoded.get("sub", "unknown")),
                name=decoded.get("name"),
                source="U2M",
                authenticated=True,
            )
        except Exception:
            pass

    # Try cookie auth
    auth_cookie = request.cookies.get("_databricks_auth")
    if auth_cookie:
        try:
            import jwt
            decoded = jwt.decode(auth_cookie, options={"verify_signature": False})
            return User(
                email=decoded.get("email", "unknown"),
                name=decoded.get("name"),
                source="Cookie",
                authenticated=True,
            )
        except Exception:
            pass

    return User(email="anonymous", source="M2M", authenticated=False)


# Health check endpoint
@app.get("/api/health", response_model=HealthResponse)
async def health_check():
    """Health check with data source status."""
    return HealthResponse(
        status="healthy",
        data_source="lakebase" if LAKEBASE_ENABLED and data_layer and data_layer.lakebase_available else "warehouse",
        lakebase_enabled=LAKEBASE_ENABLED,
        cache_ttl=CACHE_TTL,
        timestamp=datetime.now().isoformat(),
    )


# Auth status endpoint
@app.get("/api/auth/status")
async def auth_status(request: Request):
    """Get current authentication status."""
    user = get_current_user(request)
    return {
        "authenticated": user.authenticated,
        "user": user.model_dump() if user.authenticated else None,
        "auth_method": user.source,
    }


# Data access check endpoint
@app.get("/api/data/access-check")
async def check_data_access():
    """
    Check if the app has access to required data sources.
    Returns detailed error info if access is denied.
    """
    if not data_layer:
        return JSONResponse(
            status_code=503,
            content={
                "accessible": False,
                "error_code": "DATA_LAYER_NOT_INITIALIZED",
                "message": "Data layer is not initialized",
                "resolution": "Please wait for the app to fully initialize or contact support",
                "tables_affected": [],
            },
        )

    try:
        # Test access to key tables
        access_results = data_layer.check_table_access()
        all_accessible = all(r["accessible"] for r in access_results)
        any_accessible = any(r["accessible"] for r in access_results)

        if all_accessible:
            return {
                "accessible": True,
                "data_source": data_layer.current_source,
                "tables": access_results,
            }
        elif any_accessible:
            # Partial access — allow the app to load with available tables
            inaccessible = [r for r in access_results if not r["accessible"]]
            return {
                "accessible": True,
                "partial": True,
                "data_source": data_layer.current_source,
                "tables": access_results,
                "tables_unavailable": [r["table"] for r in inaccessible],
            }
        else:
            inaccessible = [r for r in access_results if not r["accessible"]]
            return JSONResponse(
                status_code=403,
                content={
                    "accessible": False,
                    "error_code": "PERMISSION_DENIED",
                    "message": "App does not have access to required data tables",
                    "resolution": "Grant SELECT permission on the affected tables to the app's service principal. See documentation for commands.",
                    "tables_affected": [r["table"] for r in inaccessible],
                    "details": inaccessible,
                },
            )

    except Exception as e:
        return JSONResponse(
            status_code=500,
            content={
                "accessible": False,
                "error_code": "ACCESS_CHECK_FAILED",
                "message": str(e),
                "resolution": "Check app logs for more details or verify service principal permissions",
                "tables_affected": [],
            },
        )


# Performance comparison endpoint
@app.get("/api/data/performance")
async def get_performance_stats():
    """Compare Lakebase vs SQL Warehouse query performance."""
    if not data_layer:
        raise HTTPException(status_code=503, detail="Data layer not initialized")

    return data_layer.get_performance_comparison()


# Workspace URL lookup (cached)
_workspace_urls: Dict[str, str] = {}
_workspace_urls_loaded = False


@app.get("/api/workspaces")
async def get_workspace_urls():
    """Get workspace_id -> workspace_url mapping from system.access.workspaces_latest."""
    global _workspace_urls, _workspace_urls_loaded

    if _workspace_urls_loaded:
        return _workspace_urls

    if not data_layer:
        raise HTTPException(status_code=503, detail="Data layer not initialized")

    try:
        result = data_layer.execute_query(
            f"SELECT workspace_id, workspace_name, workspace_url FROM {CATALOG}.{SCHEMA}.synced_workspaces WHERE status = 'RUNNING'",
            use_cache=True,
            lakebase_query=f"SELECT workspace_id::text, workspace_name, workspace_url FROM {SCHEMA}.lb_synced_workspaces WHERE status = 'RUNNING'",
        )
        _workspace_urls = {str(row[0]): {"name": row[1], "url": row[2]} for row in result.data}
        _workspace_urls_loaded = True
        return _workspace_urls
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Failed to load workspace URLs: {e}")


# Include routers
app.include_router(jobs.router, prefix="/api/jobs", tags=["Jobs"])
app.include_router(health.router, prefix="/api/health-metrics", tags=["Health"])
app.include_router(genie.router, prefix="/api/genie", tags=["AI Assistant"])


# Serve static files (React frontend)
static_path = os.path.join(os.path.dirname(__file__), "static")
if os.path.exists(static_path):
    app.mount("/assets", StaticFiles(directory=os.path.join(static_path, "assets")), name="assets")

    @app.get("/{path:path}")
    async def serve_frontend(path: str):
        """Serve React frontend for all non-API routes."""
        # Check if it's a static file
        file_path = os.path.join(static_path, path)
        if os.path.isfile(file_path):
            return FileResponse(file_path)
        # Otherwise serve index.html for SPA routing
        return FileResponse(os.path.join(static_path, "index.html"))


# Root endpoint
@app.get("/")
async def root():
    """Redirect to frontend or return API info."""
    if os.path.exists(static_path):
        return FileResponse(os.path.join(static_path, "index.html"))
    return {
        "name": "Unified Job Platform API",
        "version": "1.0.0",
        "docs": "/docs",
        "health": "/api/health",
    }


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
