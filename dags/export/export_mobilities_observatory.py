"""Export normalized mobilities from MongoDB raw data into a Google Sheet."""

from __future__ import annotations

import shlex

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator


DAG_ID = "export_mobilities_observatory"
DEFAULT_REMOTE_DIR = "/srv/mobilities_transform"
DEFAULT_SPREADSHEET_ID = ""
DEFAULT_SHEET_NAME = "movilidades"


def _quote(value: str) -> str:
    return shlex.quote(str(value))


@dag(
    dag_id=DAG_ID,
    description="Export normalized mobilities from MongoDB raw data to Google Sheets.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["export", "mobilities", "mongodb", "google-sheets"],
)
def export_mobilities_observatory() -> None:
    remote_dir = Variable.get("MOBILITIES_TRANSFORM_REMOTE_DIR", default_var=DEFAULT_REMOTE_DIR)
    python_bin = Variable.get(
        "MOBILITIES_TRANSFORM_REMOTE_PYTHON",
        default_var=f"{remote_dir}/venv/bin/python",
    )
    script_path = Variable.get(
        "MOBILITIES_TRANSFORM_REMOTE_SCRIPT",
        default_var=f"{remote_dir}/transform_mobilities_observatory.py",
    )
    setup_script = Variable.get(
        "MOBILITIES_TRANSFORM_REMOTE_SETUP_SCRIPT",
        default_var=f"{remote_dir}/setup_mobilities_transform.py",
    )
    remote_env = Variable.get(
        "MOBILITIES_TRANSFORM_REMOTE_ENV",
        default_var=f"{remote_dir}/.env",
    )
    token_path = Variable.get(
        "MOBILITIES_TRANSFORM_TOKEN_PATH",
        default_var=f"{remote_dir}/secrets/token.pickle",
    )
    spreadsheet_id = Variable.get(
        "MOBILITIES_TRANSFORM_SPREADSHEET_ID",
        default_var=DEFAULT_SPREADSHEET_ID,
    )
    sheet_name = Variable.get(
        "MOBILITIES_TRANSFORM_SHEET_NAME",
        default_var=DEFAULT_SHEET_NAME,
    )
    db_name = Variable.get("MOBILITIES_TRANSFORM_DB", default_var="international")
    collection = Variable.get("MOBILITIES_TRANSFORM_COLLECTION", default_var="mobilities")
    source_type = Variable.get("MOBILITIES_TRANSFORM_SOURCE_TYPE", default_var="mobilities")
    ssh_conn_id = Variable.get(
        "MOBILITIES_TRANSFORM_SSH_CONN_ID",
        default_var="ssh_capture_default",
    )
    lock_path = Variable.get(
        "MOBILITIES_TRANSFORM_LOCK_PATH",
        default_var=f"/tmp/{DAG_ID}.lock",
    )
    cmd_timeout = int(Variable.get("MOBILITIES_TRANSFORM_CMD_TIMEOUT", default_var="7200"))
    chunk_size = int(Variable.get("MOBILITIES_TRANSFORM_CHUNK_SIZE", default_var="5000"))
    batch_size = int(Variable.get("MOBILITIES_TRANSFORM_BATCH_SIZE", default_var="1000"))
    setup_skip_pip = (
        Variable.get("MOBILITIES_TRANSFORM_SETUP_SKIP_PIP", default_var="false").strip().lower()
        in {"1", "true", "yes", "y"}
    )
    dry_run = (
        Variable.get("MOBILITIES_TRANSFORM_DRY_RUN", default_var="false").strip().lower()
        in {"1", "true", "yes", "y"}
    )
    skip_backup = (
        Variable.get("MOBILITIES_TRANSFORM_SKIP_BACKUP", default_var="false").strip().lower()
        in {"1", "true", "yes", "y"}
    )

    transform_command_parts = [
        f"flock -n {_quote(lock_path)}",
        _quote(python_bin),
        _quote(script_path),
        f"--db {_quote(db_name)}",
        f"--collection {_quote(collection)}",
        f"--source-type {_quote(source_type)}",
        f"--sheet-name {_quote(sheet_name)}",
        f"--token-path {_quote(token_path)}",
        f"--batch-size {_quote(str(batch_size))}",
        f"--chunk-size {_quote(str(chunk_size))}",
    ]
    if str(spreadsheet_id).strip():
        transform_command_parts.append(f"--spreadsheet-id {_quote(spreadsheet_id)}")
    transform_command = " ".join(transform_command_parts)
    if dry_run:
        transform_command = f"{transform_command} --dry-run"
    if skip_backup:
        transform_command = f"{transform_command} --skip-backup"

    setup_command = "\n".join(
        [
            "set -euo pipefail",
            f"cd {_quote(remote_dir)}",
            f"test -f {_quote(setup_script)}",
            " ".join(
                part
                for part in [
                    f"python3 {_quote(setup_script)}",
                    f"--base-dir {_quote(remote_dir)}",
                    "--skip-pip" if setup_skip_pip else "",
                ]
                if part
            ),
        ]
    )

    transform_command_remote = "\n".join(
        [
            "set -euo pipefail",
            f"cd {_quote(remote_dir)}",
            f"test -x {_quote(python_bin)}",
            f"test -f {_quote(script_path)}",
            f"test -f {_quote(token_path)}",
            f"if [ -f {_quote(remote_env)} ]; then set -a; . {_quote(remote_env)}; set +a; fi",
            transform_command,
        ]
    )

    prepare_remote_runtime = SSHOperator(
        task_id="prepare_remote_runtime",
        ssh_conn_id=ssh_conn_id,
        command=setup_command,
        cmd_timeout=cmd_timeout,
    )

    run_remote_transform = SSHOperator(
        task_id="run_remote_export",
        ssh_conn_id=ssh_conn_id,
        command=transform_command_remote,
        cmd_timeout=cmd_timeout,
    )

    prepare_remote_runtime >> run_remote_transform


export_mobilities_observatory()
