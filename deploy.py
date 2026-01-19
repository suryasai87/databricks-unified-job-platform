#!/usr/bin/env python3
"""
Automated Deployment Script for Unified Job Platform
=====================================================
One-command deployment to Databricks Apps with automatic permission setup.

Usage:
    python deploy.py dev        # Deploy to development
    python deploy.py staging    # Deploy to staging
    python deploy.py prod       # Deploy to production
    python deploy.py --setup    # Setup infrastructure only
"""
import argparse
import json
import os
import subprocess
import sys
from pathlib import Path


class Colors:
    HEADER = '\033[95m'
    BLUE = '\033[94m'
    GREEN = '\033[92m'
    YELLOW = '\033[93m'
    RED = '\033[91m'
    END = '\033[0m'
    BOLD = '\033[1m'


def print_header(msg):
    print(f"\n{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{msg}{Colors.END}")
    print(f"{Colors.HEADER}{Colors.BOLD}{'=' * 60}{Colors.END}\n")


def print_step(step, msg):
    print(f"{Colors.BLUE}[{step}]{Colors.END} {msg}")


def print_success(msg):
    print(f"{Colors.GREEN}✓ {msg}{Colors.END}")


def print_warning(msg):
    print(f"{Colors.YELLOW}⚠ {msg}{Colors.END}")


def print_error(msg):
    print(f"{Colors.RED}✗ {msg}{Colors.END}")


def run_command(cmd, capture=True, check=True):
    """Run a shell command."""
    if isinstance(cmd, str):
        cmd = cmd.split()

    result = subprocess.run(
        cmd,
        capture_output=capture,
        text=True,
    )

    if check and result.returncode != 0:
        print_error(f"Command failed: {' '.join(cmd)}")
        if result.stderr:
            print(result.stderr)
        return None

    return result.stdout.strip() if capture else result.returncode


def get_databricks_profile():
    """Get the Databricks CLI profile to use."""
    return os.environ.get("DATABRICKS_PROFILE", "DEFAULT")


class Deployer:
    def __init__(self, target: str):
        self.target = target
        self.project_root = Path(__file__).parent.absolute()
        self.app_name = "unified-job-platform"
        self.profile = get_databricks_profile()
        self.workspace_path = None
        self.app_client_id = None

    def deploy(self):
        """Run full deployment pipeline."""
        print_header(f"Deploying Unified Job Platform to {self.target}")

        steps = [
            ("1/7", "Building application", self.build),
            ("2/7", "Validating bundle", self.validate_bundle),
            ("3/7", "Deploying bundle", self.deploy_bundle),
            ("4/7", "Getting workspace path", self.get_workspace_path),
            ("5/7", "Uploading app files", self.upload_app),
            ("6/7", "Deploying Databricks App", self.deploy_app),
            ("7/7", "Setting up permissions", self.setup_permissions),
        ]

        for step, description, func in steps:
            print_step(step, description)
            if not func():
                print_error(f"Failed at step: {description}")
                return False
            print_success(description)

        self.print_summary()
        return True

    def build(self):
        """Build the application."""
        result = run_command(
            [sys.executable, str(self.project_root / "build.py")],
            capture=False,
        )
        return result == 0

    def validate_bundle(self):
        """Validate the Databricks Asset Bundle."""
        result = run_command([
            "databricks", "bundle", "validate",
            "--profile", self.profile,
            "--target", self.target,
        ])
        return result is not None

    def deploy_bundle(self):
        """Deploy the Databricks Asset Bundle."""
        result = run_command([
            "databricks", "bundle", "deploy",
            "--profile", self.profile,
            "--target", self.target,
            "--force",
        ])
        return result is not None

    def get_workspace_path(self):
        """Get the workspace path for the app."""
        result = run_command([
            "databricks", "current-user", "me",
            "--profile", self.profile,
            "--output", "json",
        ])

        if not result:
            return False

        try:
            user_info = json.loads(result)
            username = user_info.get("userName", "")
            self.workspace_path = f"/Workspace/Users/{username}/.bundle/{self.app_name}/{self.target}/app"
            return True
        except json.JSONDecodeError:
            return False

    def upload_app(self):
        """Upload app files to workspace."""
        build_dir = self.project_root / "build" / "app"

        result = run_command([
            "databricks", "workspace", "import-dir",
            str(build_dir),
            self.workspace_path,
            "--profile", self.profile,
            "--overwrite",
        ])
        return result is not None

    def deploy_app(self):
        """Deploy the Databricks App."""
        # Check if app exists
        check_result = run_command([
            "databricks", "apps", "get", self.app_name,
            "--profile", self.profile,
        ], check=False)

        if check_result is None or "NOT_FOUND" in str(check_result):
            # Create app
            print("  Creating new app...")
            run_command([
                "databricks", "apps", "create", self.app_name,
                "--profile", self.profile,
            ], check=False)

        # Deploy app
        result = run_command([
            "databricks", "apps", "deploy", self.app_name,
            "--source-code-path", self.workspace_path,
            "--profile", self.profile,
        ])

        if result:
            # Get app info to extract service principal
            app_info = run_command([
                "databricks", "apps", "get", self.app_name,
                "--profile", self.profile,
                "--output", "json",
            ])

            if app_info:
                try:
                    info = json.loads(app_info)
                    self.app_client_id = info.get("service_principal_client_id")
                except json.JSONDecodeError:
                    pass

        return result is not None

    def setup_permissions(self):
        """Setup required permissions for the app."""
        if not self.app_client_id:
            print_warning("Could not get app client ID. Manual permission setup required.")
            return True

        print(f"  App Service Principal: {self.app_client_id}")

        # Grant warehouse access
        warehouse_id = "4b28691c780d9875"
        run_command([
            "databricks", "permissions", "update",
            f"sql/warehouses/{warehouse_id}",
            "--profile", self.profile,
            "--json", json.dumps({
                "access_control_list": [{
                    "service_principal_name": self.app_client_id,
                    "permission_level": "CAN_USE"
                }]
            }),
        ], check=False)

        print_warning("Unity Catalog permissions must be granted manually via SQL:")
        print(f"  GRANT USE CATALOG ON CATALOG hls_amer_catalog TO `{self.app_client_id}`;")
        print(f"  GRANT USE SCHEMA ON SCHEMA hls_amer_catalog.cost_management TO `{self.app_client_id}`;")
        print(f"  GRANT SELECT ON SCHEMA hls_amer_catalog.cost_management TO `{self.app_client_id}`;")

        return True

    def print_summary(self):
        """Print deployment summary."""
        print_header("Deployment Complete!")

        # Get app URL
        app_info = run_command([
            "databricks", "apps", "get", self.app_name,
            "--profile", self.profile,
            "--output", "json",
        ])

        if app_info:
            try:
                info = json.loads(app_info)
                url = info.get("url")
                if url:
                    print(f"App URL: {Colors.GREEN}{url}{Colors.END}")
            except json.JSONDecodeError:
                pass

        print(f"\nApp Name: {self.app_name}")
        print(f"Target: {self.target}")
        print(f"Workspace Path: {self.workspace_path}")

        if self.app_client_id:
            print(f"Service Principal: {self.app_client_id}")


def setup_infrastructure():
    """Run infrastructure setup (create tables and views)."""
    print_header("Setting up Infrastructure")

    project_root = Path(__file__).parent.absolute()
    sql_file = project_root / "sql" / "01_create_infrastructure.sql"

    print("To set up the infrastructure, run the following in a Databricks notebook:")
    print(f"\n%sql\n-- Execute the contents of: {sql_file}\n")
    print("Or use the Databricks SQL editor to run the SQL script.")


def main():
    parser = argparse.ArgumentParser(
        description="Deploy Unified Job Platform to Databricks"
    )
    parser.add_argument(
        "target",
        nargs="?",
        default="dev",
        choices=["dev", "staging", "prod"],
        help="Deployment target (default: dev)",
    )
    parser.add_argument(
        "--setup",
        action="store_true",
        help="Setup infrastructure only (create tables/views)",
    )
    parser.add_argument(
        "--profile",
        default="DEFAULT",
        help="Databricks CLI profile to use",
    )

    args = parser.parse_args()

    if args.profile:
        os.environ["DATABRICKS_PROFILE"] = args.profile

    if args.setup:
        setup_infrastructure()
        return 0

    deployer = Deployer(args.target)
    success = deployer.deploy()

    return 0 if success else 1


if __name__ == "__main__":
    sys.exit(main())
