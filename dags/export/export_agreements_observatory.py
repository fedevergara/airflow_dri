"""Export agreement records from MongoDB raw data into a Google Sheet."""

from __future__ import annotations

import shlex

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator


DAG_ID = "export_agreements_observatory"
DEFAULT_REMOTE_DIR = "/srv/mobilities_transform/agreements_export"
DEFAULT_SHEET_NAME = "convenios"
DEFAULT_TOKEN_PATH = "/srv/mobilities_transform/secrets/token.pickle"


def _quote(value: str) -> str:
    return shlex.quote(str(value))


@dag(
    dag_id=DAG_ID,
    description="Export raw agreements from MongoDB to Google Sheets.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["export", "agreements", "mongodb", "google-sheets"],
)
def export_agreements_observatory() -> None:
    remote_dir = Variable.get("AGREEMENTS_EXPORT_REMOTE_DIR", default_var=DEFAULT_REMOTE_DIR)
    python_bin = Variable.get(
        "AGREEMENTS_EXPORT_REMOTE_PYTHON",
        default_var=f"{remote_dir}/venv/bin/python",
    )
    script_path = Variable.get(
        "AGREEMENTS_EXPORT_REMOTE_SCRIPT",
        default_var=f"{remote_dir}/export_agreements_observatory.py",
    )
    setup_script = Variable.get(
        "AGREEMENTS_EXPORT_REMOTE_SETUP_SCRIPT",
        default_var=f"{remote_dir}/setup_agreements_export.py",
    )
    remote_env = Variable.get(
        "AGREEMENTS_EXPORT_REMOTE_ENV",
        default_var=f"{remote_dir}/.env",
    )
    token_path = Variable.get(
        "AGREEMENTS_EXPORT_TOKEN_PATH",
        default_var=DEFAULT_TOKEN_PATH,
    )
    sheet_name = Variable.get(
        "AGREEMENTS_EXPORT_SHEET_NAME",
        default_var=DEFAULT_SHEET_NAME,
    )
    spreadsheet_id = Variable.get("AGREEMENTS_EXPORT_SPREADSHEET_ID", default_var="")
    db_name = Variable.get("AGREEMENTS_EXPORT_DB", default_var="international")
    collection = Variable.get("AGREEMENTS_EXPORT_COLLECTION", default_var="agreements")
    source_type = Variable.get("AGREEMENTS_EXPORT_SOURCE_TYPE", default_var="agreements")
    ssh_conn_id = Variable.get(
        "AGREEMENTS_EXPORT_SSH_CONN_ID",
        default_var="ssh_capture_default",
    )
    lock_path = Variable.get(
        "AGREEMENTS_EXPORT_LOCK_PATH",
        default_var=f"/tmp/{DAG_ID}.lock",
    )
    cmd_timeout = int(Variable.get("AGREEMENTS_EXPORT_CMD_TIMEOUT", default_var="7200"))
    chunk_size = int(Variable.get("AGREEMENTS_EXPORT_CHUNK_SIZE", default_var="5000"))
    batch_size = int(Variable.get("AGREEMENTS_EXPORT_BATCH_SIZE", default_var="1000"))
    setup_skip_pip = (
        Variable.get("AGREEMENTS_EXPORT_SETUP_SKIP_PIP", default_var="false").strip().lower()
        in {"1", "true", "yes", "y"}
    )
    dry_run = (
        Variable.get("AGREEMENTS_EXPORT_DRY_RUN", default_var="false").strip().lower()
        in {"1", "true", "yes", "y"}
    )
    skip_backup = (
        Variable.get("AGREEMENTS_EXPORT_SKIP_BACKUP", default_var="false").strip().lower()
        in {"1", "true", "yes", "y"}
    )

    export_command_parts = [
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
        export_command_parts.append(f"--spreadsheet-id {_quote(spreadsheet_id)}")
    export_command = " ".join(export_command_parts)
    if dry_run:
        export_command = f"{export_command} --dry-run"
    if skip_backup:
        export_command = f"{export_command} --skip-backup"

    setup_command = "\n".join(
        [
            "set -euo pipefail",
            f"mkdir -p {_quote(remote_dir)}",
            f"cd {_quote(remote_dir)}",
            f"test -f {_quote(setup_script)}",
            " ".join(
                part
                for part in [
                    f"python3 {_quote(setup_script)}",
                    f"--base-dir {_quote(remote_dir)}",
                    f"--token-path {_quote(token_path)}",
                    f"--sheet-name {_quote(sheet_name)}",
                    f"--spreadsheet-id {_quote(spreadsheet_id)}"
                    if str(spreadsheet_id).strip()
                    else "",
                    "--skip-pip" if setup_skip_pip else "",
                ]
                if part
            ),
        ]
    )

    export_command_remote = "\n".join(
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

    prepare_remote_runtime = SSHOperator(
        task_id="prepare_remote_runtime",
        ssh_conn_id=ssh_conn_id,
        command=setup_command,
        cmd_timeout=cmd_timeout,
    )

    run_remote_export = SSHOperator(
        task_id="run_remote_export",
        ssh_conn_id=ssh_conn_id,
        command=export_command_remote,
        cmd_timeout=cmd_timeout,
    )

    prepare_remote_runtime >> run_remote_export


export_agreements_observatory()
