#!/usr/bin/env python3
"""
Build Script for Unified Job Platform
======================================
Builds the React frontend and packages with the FastAPI backend.
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path


def run_command(cmd: list, cwd: str = None):
    """Run a command and handle errors."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        sys.exit(1)
    return result.stdout


def main():
    project_root = Path(__file__).parent.absolute()
    frontend_dir = project_root / "src" / "frontend"
    backend_dir = project_root / "src" / "backend"
    build_dir = project_root / "build" / "app"

    print("=" * 60)
    print("Unified Job Platform - Build Script")
    print("=" * 60)

    # Step 1: Clean build directory
    print("\n[1/5] Cleaning build directory...")
    if build_dir.exists():
        shutil.rmtree(build_dir)
    build_dir.mkdir(parents=True)

    # Step 2: Build React frontend
    print("\n[2/5] Building React frontend...")
    if not (frontend_dir / "node_modules").exists():
        run_command(["npm", "install"], cwd=str(frontend_dir))
    run_command(["npm", "run", "build"], cwd=str(frontend_dir))

    # Step 3: Copy backend files
    print("\n[3/5] Copying backend files...")
    shutil.copy(backend_dir / "app.py", build_dir / "app.py")
    shutil.copy(backend_dir / "requirements.txt", build_dir / "requirements.txt")

    # Copy routers
    routers_dest = build_dir / "routers"
    routers_dest.mkdir(exist_ok=True)
    for file in (backend_dir / "routers").glob("*.py"):
        shutil.copy(file, routers_dest / file.name)

    # Copy data layer
    data_dest = build_dir / "data"
    data_dest.mkdir(exist_ok=True)
    for file in (backend_dir / "data").glob("*.py"):
        shutil.copy(file, data_dest / file.name)

    # Step 4: Copy frontend dist to static
    print("\n[4/5] Copying frontend to static directory...")
    static_dest = build_dir / "static"
    shutil.copytree(frontend_dir / "dist", static_dest)

    # Step 5: Create app.yaml
    print("\n[5/5] Creating app.yaml...")
    app_yaml = """command:
  - uvicorn
  - app:app
  - --host
  - "0.0.0.0"
  - --port
  - "8000"

env:
  - name: DATABRICKS_HOST
    value: fe-vm-hls-amer.cloud.databricks.com
  - name: CATALOG
    value: hls_amer_catalog
  - name: SCHEMA
    value: cost_management
  - name: WAREHOUSE_ID
    value: "4b28691c780d9875"
  - name: LAKEBASE_INSTANCE_ID
    value: "6b59171b-cee8-4acc-9209-6c848ffbfbfe"
  - name: LAKEBASE_ENABLED
    value: "true"
  - name: CACHE_TTL
    value: "300"
"""
    (build_dir / "app.yaml").write_text(app_yaml)

    print("\n" + "=" * 60)
    print("Build completed successfully!")
    print(f"Output: {build_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
