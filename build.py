#!/usr/bin/env python3
"""
Build Script for Unified Job Platform
======================================
Builds the React frontend and packages with the FastAPI backend.
Reads configuration from databricks.yml (single source of truth).
"""
import os
import shutil
import subprocess
import sys
from pathlib import Path

import yaml


def run_command(cmd: list, cwd: str = None):
    """Run a command and handle errors."""
    print(f"Running: {' '.join(cmd)}")
    result = subprocess.run(cmd, cwd=cwd, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error: {result.stderr}")
        sys.exit(1)
    return result.stdout


def load_config(project_root: Path, target: str = "dev") -> dict:
    """Load variable values from databricks.yml for the given target."""
    config_path = project_root / "databricks.yml"
    with open(config_path) as f:
        config = yaml.safe_load(f)

    # Start with top-level variable defaults
    variables = {}
    for key, val in config.get("variables", {}).items():
        variables[key] = val.get("default", "") if isinstance(val, dict) else val

    # Override with target-specific variables
    target_config = config.get("targets", {}).get(target, {})
    for key, val in target_config.get("variables", {}).items():
        variables[key] = val

    return variables


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Build Unified Job Platform")
    parser.add_argument("--target", default="dev", help="Target from databricks.yml (default: dev)")
    args = parser.parse_args()

    project_root = Path(__file__).parent.absolute()
    frontend_dir = project_root / "src" / "frontend"
    backend_dir = project_root / "src" / "backend"
    build_dir = project_root / "build" / "app"

    # Load config from databricks.yml
    config = load_config(project_root, args.target)
    print(f"Using target: {args.target}")
    print(f"  catalog: {config.get('catalog')}")
    print(f"  schema: {config.get('schema')}")
    print(f"  warehouse_id: {config.get('warehouse_id')}")
    print(f"  lakebase_instance_name: {config.get('lakebase_instance_name') or '(not set)'}")

    if not config.get("warehouse_id"):
        print("\nError: warehouse_id is not set in databricks.yml. Set it under variables or targets.")
        sys.exit(1)

    print("\n" + "=" * 60)
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
    lakebase_name = config.get("lakebase_instance_name", "")
    env_vars = [
        ("CATALOG", config.get("catalog", "main")),
        ("SCHEMA", config.get("schema", "cost_management")),
        ("WAREHOUSE_ID", config.get("warehouse_id", "")),
        ("LAKEBASE_INSTANCE_NAME", lakebase_name),
        ("LAKEBASE_ENABLED", "true" if lakebase_name else "false"),
        ("CACHE_TTL", "300"),
    ]
    env_lines = "\n".join(
        f"  - name: {name}\n    value: \"{value}\"" for name, value in env_vars if value
    )
    app_yaml = f"""command:
  - uvicorn
  - app:app
  - --host
  - "0.0.0.0"
  - --port
  - "8000"

env:
{env_lines}
"""
    (build_dir / "app.yaml").write_text(app_yaml)

    print("\n" + "=" * 60)
    print("Build completed successfully!")
    print(f"Output: {build_dir}")
    print("=" * 60)


if __name__ == "__main__":
    main()
