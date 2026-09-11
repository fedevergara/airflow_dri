"""Export research projects with external entities to the observatory Sheet."""

from __future__ import annotations

import shlex

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator


DAG_ID = "export_research_projects_observatory"
DEFAULT_REMOTE_DIR = "/srv/research_projects_export"
DEFAULT_SOURCE_SPREADSHEET_ID = ""
DEFAULT_TARGET_SPREADSHEET_ID = ""


def _quote(value: str) -> str:
    return shlex.quote(str(value))


@dag(
    dag_id=DAG_ID,
    description="Export research projects with non-UdeA entities to Google Sheets.",
    start_date=pendulum.datetime(2026, 1, 1, tz="America/Bogota"),
    schedule="0 16 * * 5",
    catchup=False,
    max_active_runs=1,
    tags=["export", "research-projects", "observatory", "google-sheets"],
)
def export_research_projects_observatory() -> None:
    remote_dir = Variable.get(
        "RESEARCH_PROJECTS_EXPORT_REMOTE_DIR",
        default_var=DEFAULT_REMOTE_DIR,
    )
    python_bin = Variable.get(
        "RESEARCH_PROJECTS_EXPORT_REMOTE_PYTHON",
        default_var=f"{remote_dir}/venv/bin/python",
    )
    script_path = Variable.get(
        "RESEARCH_PROJECTS_EXPORT_REMOTE_SCRIPT",
        default_var=f"{remote_dir}/export_research_projects_observatory.py",
    )
    remote_env = Variable.get(
        "RESEARCH_PROJECTS_EXPORT_REMOTE_ENV",
        default_var=f"{remote_dir}/.env",
    )
    token_path = Variable.get(
        "RESEARCH_PROJECTS_EXPORT_TOKEN_PATH",
        default_var="/srv/kahi_exports/secrets/token.pickle",
    )
    source_spreadsheet_id = Variable.get(
        "RESEARCH_PROJECTS_SOURCE_SPREADSHEET_ID",
        default_var=DEFAULT_SOURCE_SPREADSHEET_ID,
    )
    target_spreadsheet_id = Variable.get(
        "RESEARCH_PROJECTS_TARGET_SPREADSHEET_ID",
        default_var=DEFAULT_TARGET_SPREADSHEET_ID,
    )
    source_sheet_name = Variable.get(
        "RESEARCH_PROJECTS_SOURCE_SHEET_NAME",
        default_var="Hoja 1",
    )
    target_sheet_name = Variable.get(
        "RESEARCH_PROJECTS_TARGET_SHEET_NAME",
        default_var="Hoja 1",
    )
    ssh_conn_id = Variable.get(
        "RESEARCH_PROJECTS_EXPORT_SSH_CONN_ID",
        default_var="ssh_kahi_default",
    )
    lock_path = Variable.get(
        "RESEARCH_PROJECTS_EXPORT_LOCK_PATH",
        default_var=f"/tmp/{DAG_ID}.lock",
    )
    cmd_timeout = int(
        Variable.get("RESEARCH_PROJECTS_EXPORT_CMD_TIMEOUT", default_var="1800")
    )
    chunk_size = int(
        Variable.get("RESEARCH_PROJECTS_EXPORT_CHUNK_SIZE", default_var="1000")
    )
    min_write_interval_seconds = float(
        Variable.get(
            "RESEARCH_PROJECTS_EXPORT_MIN_WRITE_INTERVAL_SECONDS",
            default_var="1.2",
        )
    )
    max_api_attempts = int(
        Variable.get("RESEARCH_PROJECTS_EXPORT_MAX_API_ATTEMPTS", default_var="8")
    )
    dry_run = (
        Variable.get("RESEARCH_PROJECTS_EXPORT_DRY_RUN", default_var="false")
        .strip()
        .lower()
        in {"1", "true", "yes", "y"}
    )
    skip_backup = (
        Variable.get("RESEARCH_PROJECTS_EXPORT_SKIP_BACKUP", default_var="false")
        .strip()
        .lower()
        in {"1", "true", "yes", "y"}
    )

    command_parts = [
        f"flock -n {_quote(lock_path)}",
        _quote(python_bin),
        _quote(script_path),
        f"--source-spreadsheet-id {_quote(source_spreadsheet_id)}",
        f"--target-spreadsheet-id {_quote(target_spreadsheet_id)}",
        f"--source-sheet-name {_quote(source_sheet_name)}",
        f"--target-sheet-name {_quote(target_sheet_name)}",
        f"--token-path {_quote(token_path)}",
        f"--chunk-size {_quote(str(chunk_size))}",
        f"--min-write-interval-seconds {_quote(str(min_write_interval_seconds))}",
        f"--max-api-attempts {_quote(str(max_api_attempts))}",
    ]
    if dry_run:
        command_parts.append("--dry-run")
    if skip_backup:
        command_parts.append("--skip-backup")
    export_command = " ".join(command_parts)

    remote_command = "\n".join(
        [
            "set -euo pipefail",
            f"cd {_quote(remote_dir)}",
            f"test -x {_quote(python_bin)}",
            f"test -f {_quote(script_path)}",
            f"test -f {_quote(token_path)}",
            f"if [ -f {_quote(remote_env)} ]; then set -a; . {_quote(remote_env)}; set +a; fi",
            export_command,
        ]
    )

    SSHOperator(
        task_id="extract_transform_load",
        ssh_conn_id=ssh_conn_id,
        command=remote_command,
        cmd_timeout=cmd_timeout,
    )


export_research_projects_observatory()
