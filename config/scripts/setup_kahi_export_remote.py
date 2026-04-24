#!/usr/bin/env python3
"""Prepare the remote Kahi export runtime used by Airflow.

Run this script on the remote server. It creates/validates the directory
structure, virtual environment, Python dependencies, local env file, token
location, and export script needed by the Airflow SSH DAG.
"""

from __future__ import annotations

import argparse
import json
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any


DEFAULT_BASE_DIR = "/srv/kahi_exports"
DEFAULT_LEGACY_TOKEN = ""
DEFAULT_MONGO_URI = "mongodb://localhost:27017/"
DEFAULT_DB = "kahi"

REQUIREMENTS = [
    "pandas",
    "pymongo",
    "google-api-python-client",
    "google-auth",
    "google-auth-oauthlib",
]


def run(command: list[str], *, dry_run: bool) -> None:
    printable = " ".join(command)
    if dry_run:
        print(f"DRY_RUN command: {printable}")
        return
    subprocess.run(command, check=True)


def shell_quote_env_value(value: str) -> str:
    return "'" + value.replace("'", "'\"'\"'") + "'"


def write_text_file(path: Path, content: str, *, mode: int, overwrite: bool, dry_run: bool) -> bool:
    if path.exists() and not overwrite:
        return False
    if dry_run:
        print(f"DRY_RUN write: {path}")
        return True
    path.write_text(content, encoding="utf-8")
    path.chmod(mode)
    return True


def copy_file(source: Path, destination: Path, *, mode: int, overwrite: bool, dry_run: bool) -> bool:
    if not source.exists():
        return False
    if destination.exists() and not overwrite:
        return False
    if dry_run:
        print(f"DRY_RUN copy: {source} -> {destination}")
        return True
    shutil.copy2(source, destination)
    destination.chmod(mode)
    return True


def ensure_runtime(args: argparse.Namespace) -> dict[str, Any]:
    base_dir = Path(args.base_dir).expanduser().resolve()
    venv_dir = Path(args.venv_dir).expanduser().resolve() if args.venv_dir else base_dir / "venv"
    secrets_dir = Path(args.secrets_dir).expanduser().resolve() if args.secrets_dir else base_dir / "secrets"
    token_path = Path(args.token_path).expanduser().resolve() if args.token_path else secrets_dir / "token.pickle"
    env_path = Path(args.env_path).expanduser().resolve() if args.env_path else base_dir / ".env"
    export_script = (
        Path(args.export_script).expanduser().resolve()
        if args.export_script
        else base_dir / "export_kahi_looker.py"
    )
    setup_script = base_dir / "setup_kahi_export.py"
    requirements_path = base_dir / "requirements.txt"
    legacy_token = (
        Path(args.legacy_token_path).expanduser().resolve()
        if args.legacy_token_path
        else None
    )

    summary: dict[str, Any] = {
        "base_dir": str(base_dir),
        "venv_dir": str(venv_dir),
        "env_path": str(env_path),
        "token_path": str(token_path),
        "export_script": str(export_script),
        "actions": [],
    }

    if args.dry_run:
        print(f"DRY_RUN mkdir: {base_dir}")
        print(f"DRY_RUN mkdir: {secrets_dir}")
    else:
        base_dir.mkdir(parents=True, exist_ok=True)
        secrets_dir.mkdir(parents=True, exist_ok=True)
    summary["actions"].append("ensured_directories")

    env_content = (
        f"MONGO_URI={shell_quote_env_value(args.mongo_uri)}\n"
        f"KAHI_DB={args.db}\n"
    )
    if args.spreadsheet_id:
        env_content += (
            "KAHI_LOOKER_SPREADSHEET_ID="
            f"{shell_quote_env_value(args.spreadsheet_id)}\n"
        )
    if write_text_file(
        env_path,
        env_content,
        mode=0o600,
        overwrite=args.force_env,
        dry_run=args.dry_run,
    ):
        summary["actions"].append("wrote_env")
    else:
        if not args.dry_run:
            env_path.chmod(0o600)
        summary["actions"].append("kept_existing_env")

    if write_text_file(
        requirements_path,
        "\n".join(REQUIREMENTS) + "\n",
        mode=0o644,
        overwrite=True,
        dry_run=args.dry_run,
    ):
        summary["actions"].append("wrote_requirements")

    if legacy_token and copy_file(
        legacy_token,
        token_path,
        mode=0o600,
        overwrite=args.force_token,
        dry_run=args.dry_run,
    ):
        summary["actions"].append("copied_token")
    elif token_path.exists():
        if not args.dry_run:
            token_path.chmod(0o600)
        summary["actions"].append("kept_existing_token")
    else:
        summary["actions"].append("missing_token")

    if args.export_script_source:
        source = Path(args.export_script_source).expanduser().resolve()
        if copy_file(
            source,
            export_script,
            mode=0o750,
            overwrite=True,
            dry_run=args.dry_run,
        ):
            summary["actions"].append("copied_export_script")
        else:
            summary["actions"].append("export_script_source_missing")

    if Path(__file__).resolve() != setup_script:
        copied_setup = copy_file(
            Path(__file__).resolve(),
            setup_script,
            mode=0o750,
            overwrite=True,
            dry_run=args.dry_run,
        )
        if copied_setup:
            summary["actions"].append("copied_setup_script")

    if not venv_dir.exists():
        run([sys.executable, "-m", "venv", str(venv_dir)], dry_run=args.dry_run)
        summary["actions"].append("created_venv")
    else:
        summary["actions"].append("kept_existing_venv")

    venv_python = venv_dir / "bin" / "python"
    venv_pip = venv_dir / "bin" / "pip"
    if not args.skip_pip:
        run([str(venv_python), "-m", "pip", "install", "--upgrade", "pip"], dry_run=args.dry_run)
        run([str(venv_pip), "install", "-r", str(requirements_path)], dry_run=args.dry_run)
        summary["actions"].append("installed_requirements")

    if args.validate and not args.dry_run:
        run(
            [
                str(venv_python),
                "-c",
                "import pandas, pymongo, googleapiclient, google.auth; print('dependency_check=ok')",
            ],
            dry_run=False,
        )
        if not export_script.exists():
            raise FileNotFoundError(f"Missing export script: {export_script}")
        if not token_path.exists():
            raise FileNotFoundError(f"Missing Google token: {token_path}")
        summary["actions"].append("validated_runtime")

    summary["ready"] = bool(export_script.exists() and token_path.exists() and venv_python.exists())
    return summary


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Prepare remote runtime for Kahi Looker export.")
    parser.add_argument("--base-dir", default=os.getenv("KAHI_EXPORT_BASE_DIR", DEFAULT_BASE_DIR))
    parser.add_argument("--venv-dir", default="")
    parser.add_argument("--secrets-dir", default="")
    parser.add_argument("--env-path", default="")
    parser.add_argument("--token-path", default="")
    parser.add_argument("--legacy-token-path", default=DEFAULT_LEGACY_TOKEN)
    parser.add_argument("--export-script", default="")
    parser.add_argument("--export-script-source", default="")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", DEFAULT_MONGO_URI))
    parser.add_argument("--db", default=os.getenv("KAHI_DB", DEFAULT_DB))
    parser.add_argument("--spreadsheet-id", default=os.getenv("KAHI_LOOKER_SPREADSHEET_ID", ""))
    parser.add_argument("--force-env", action="store_true")
    parser.add_argument("--force-token", action="store_true")
    parser.add_argument("--skip-pip", action="store_true")
    parser.add_argument("--no-validate", dest="validate", action="store_false")
    parser.add_argument("--dry-run", action="store_true")
    parser.set_defaults(validate=True)
    return parser.parse_args()


def main() -> None:
    summary = ensure_runtime(parse_args())
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
