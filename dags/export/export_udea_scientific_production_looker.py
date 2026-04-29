"""Export UdeA scientific production into the Looker Google Sheet."""

from __future__ import annotations

import shlex

import pendulum
from airflow.decorators import dag
from airflow.models import Variable
from airflow.providers.ssh.operators.ssh import SSHOperator


DAG_ID = "export_udea_scientific_production_looker"
DEFAULT_REMOTE_DIR = "/srv/kahi_exports"


def _quote(value: str) -> str:
    return shlex.quote(str(value))


@dag(
    dag_id=DAG_ID,
    description="Export UdeA scientific production into Google Sheets for Looker Studio.",
    start_date=pendulum.datetime(2026, 1, 1, tz="UTC"),
    schedule="@daily",
    catchup=False,
    max_active_runs=1,
    tags=["export", "kahi", "looker", "google-sheets"],
)
def export_udea_scientific_production_looker() -> None:
    remote_dir = Variable.get("KAHI_LOOKER_REMOTE_DIR", default_var=DEFAULT_REMOTE_DIR)
    python_bin = Variable.get(
        "KAHI_LOOKER_REMOTE_PYTHON",
        default_var=f"{remote_dir}/venv/bin/python",
    )
    script_path = Variable.get(
        "KAHI_LOOKER_REMOTE_SCRIPT",
        default_var=f"{remote_dir}/export_kahi_looker.py",
    )
    setup_script = Variable.get(
        "KAHI_LOOKER_REMOTE_SETUP_SCRIPT",
        default_var=f"{remote_dir}/setup_kahi_export.py",
    )
    remote_env = Variable.get(
        "KAHI_LOOKER_REMOTE_ENV",
        default_var=f"{remote_dir}/.env",
    )
    token_path = Variable.get(
        "KAHI_LOOKER_TOKEN_PATH",
        default_var=f"{remote_dir}/secrets/token.pickle",
    )
    institution_id = Variable.get("KAHI_LOOKER_INSTITUTION_ID", default_var="03bp5hc83")
    db_name = Variable.get("KAHI_LOOKER_DB", default_var="kahi")
    ssh_conn_id = Variable.get("KAHI_LOOKER_SSH_CONN_ID", default_var="ssh_kahi_default")
    lock_path = Variable.get(
        "KAHI_LOOKER_LOCK_PATH",
        default_var=f"/tmp/{DAG_ID}.lock",
    )
    cmd_timeout = int(Variable.get("KAHI_LOOKER_CMD_TIMEOUT", default_var="7200"))
    chunk_size = int(Variable.get("KAHI_LOOKER_CHUNK_SIZE", default_var="5000"))
    dry_run = (
        Variable.get("KAHI_LOOKER_DRY_RUN", default_var="false").strip().lower()
        in {"1", "true", "yes", "y"}
    )

    export_command = (
        f"flock -n {_quote(lock_path)} "
        f"{_quote(python_bin)} {_quote(script_path)} "
        f"--institution-id {_quote(institution_id)} "
        f"--db {_quote(db_name)} "
        f"--token-path {_quote(token_path)} "
        f"--chunk-size {_quote(str(chunk_size))}"
    )
    if dry_run:
        export_command = f"{export_command} --dry-run"

    setup_command = "\n".join(
        [
            "set -euo pipefail",
            f"cd {_quote(remote_dir)}",
            f"test -f {_quote(setup_script)}",
            f"python3 {_quote(setup_script)} --base-dir {_quote(remote_dir)} --skip-pip",
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
            'test -n "${KAHI_LOOKER_SPREADSHEET_ID:-}"',
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


export_udea_scientific_production_looker()
