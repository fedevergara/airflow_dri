"""Reusable capture DAG builder for Google Sheets -> MongoDB raw collections."""

from __future__ import annotations

import base64
import hashlib
import pickle
from pathlib import Path
from typing import Any

import pendulum
from airflow.decorators import dag, task
from airflow.exceptions import AirflowException
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.python import get_current_context

DEFAULT_ARGS = {
    "owner": "data-platform",
    "retries": 2,
    "retry_delay": pendulum.duration(minutes=5),
}


def _get_required_variable(name: str) -> str:
    """Return a required Airflow Variable or raise a clear error."""
    try:
        value = Variable.get(name)
    except KeyError as exc:
        raise AirflowException(
            f"Missing Airflow Variable '{name}'. Define it before running capture DAGs."
        ) from exc

    if not value or not str(value).strip():
        raise AirflowException(
            f"Airflow Variable '{name}' is empty. Set a valid value before running."
        )
    return str(value).strip()


def _build_headers(first_row: list[str]) -> list[str]:
    """Build stable and unique header names from the first row of the sheet."""
    headers: list[str] = []
    seen: dict[str, int] = {}

    for index, raw_name in enumerate(first_row, start=1):
        base = str(raw_name).strip() or f"col_{index}"
        count = seen.get(base, 0)
        seen[base] = count + 1
        headers.append(base if count == 0 else f"{base}_{count + 1}")

    return headers


def _call_get_values(gsheets_hook: Any, spreadsheet_id: str, range_name: str) -> list[list[str]]:
    """Provider-compatible wrapper for get_values across minor API differences."""
    try:
        return gsheets_hook.get_values(spreadsheet_id=spreadsheet_id, range_=range_name)
    except TypeError:
        return gsheets_hook.get_values(spreadsheet_id=spreadsheet_id, range_name=range_name)


def _normalize_sheet_or_range(value: str, default_range: str = "A:Z") -> str:
    """Accept sheet name or A1 range; return a valid A1 range."""
    cleaned = str(value).strip()
    if not cleaned:
        return default_range
    if "!" in cleaned or ":" in cleaned:
        return cleaned
    safe_sheet = cleaned.replace("'", "''")
    return f"'{safe_sheet}'!{default_range}"


def _looks_like_file_path(value: str) -> bool:
    """Best-effort check to detect path-like token values."""
    cleaned = str(value).strip()
    return cleaned.startswith(("/", "~", "./", "../")) or cleaned.endswith(".pickle")


def _load_values_via_token_pickle(
    *,
    spreadsheet_id: str,
    range_name: str,
    token_pickle_b64: str = "",
    token_pickle_path: str = "",
) -> list[list[str]]:
    """Load sheet values using OAuth credentials serialized in token.pickle."""
    try:
        from google.auth.transport.requests import Request
        from googleapiclient.discovery import build
    except Exception as exc:
        raise AirflowException(
            "Missing Google API libs for token.pickle auth. Install apache-airflow-providers-google."
        ) from exc

    creds = None
    if token_pickle_b64:
        try:
            creds = pickle.loads(base64.b64decode(token_pickle_b64))
        except Exception as exc:
            raise AirflowException("Invalid GOOGLE_SHEETS_TOKEN_PICKLE_B64 value.") from exc
    elif token_pickle_path:
        token_path = Path(token_pickle_path).expanduser()
        if not token_path.exists():
            raise AirflowException(f"Token pickle not found at '{token_pickle_path}'.")
        with token_path.open("rb") as token_file:
            creds = pickle.load(token_file)
    else:
        raise AirflowException("No token pickle provided.")

    if creds and getattr(creds, "expired", False) and getattr(creds, "refresh_token", None):
        creds.refresh(Request())

    if not creds or not getattr(creds, "valid", False):
        raise AirflowException("No valid credentials in token.pickle. Regenerate OAuth token.")

    service = build("sheets", "v4", credentials=creds)
    values = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
        .get("values", [])
    )
    return values


def _build_mongo_client_kwargs(mongo_conn_id: str) -> tuple[dict[str, Any], str]:
    """Build Mongo client kwargs from an Airflow connection without exposing secrets in code."""
    mongo_conn = BaseHook.get_connection(mongo_conn_id)
    extras = mongo_conn.extra_dejson or {}
    kwargs: dict[str, Any] = {}

    if mongo_conn.login:
        kwargs["username"] = mongo_conn.login
    if mongo_conn.password:
        kwargs["password"] = mongo_conn.password

    # Common pymongo options supported from connection extras.
    mapping = {
        "authSource": "authSource",
        "auth_source": "authSource",
        "replicaSet": "replicaSet",
        "replica_set": "replicaSet",
        "tls": "tls",
        "ssl": "tls",
        "retryWrites": "retryWrites",
        "retry_writes": "retryWrites",
        "directConnection": "directConnection",
        "direct_connection": "directConnection",
        "serverSelectionTimeoutMS": "serverSelectionTimeoutMS",
        "server_selection_timeout_ms": "serverSelectionTimeoutMS",
        "connectTimeoutMS": "connectTimeoutMS",
        "connect_timeout_ms": "connectTimeoutMS",
        "tlsAllowInvalidCertificates": "tlsAllowInvalidCertificates",
    }
    for source_key, target_key in mapping.items():
        if source_key in extras and extras[source_key] not in (None, ""):
            kwargs[target_key] = extras[source_key]

    default_db = str(mongo_conn.schema).strip() if mongo_conn.schema else "capture_raw"
    return kwargs, default_db


def build_capture_dag(
    *,
    dag_id: str,
    description: str,
    source_type: str,
    sources: list[dict[str, str]],
    schedule: str = "@daily",
    start_date: pendulum.DateTime | None = None,
) -> Any:
    """Create a capture DAG with dynamic task mapping over configured sources."""

    start_date = start_date or pendulum.datetime(2026, 1, 1, tz="UTC")

    @dag(
        dag_id=dag_id,
        description=description,
        default_args=DEFAULT_ARGS,
        start_date=start_date,
        schedule=schedule,
        catchup=False,
        tags=["capture", "google-sheets", "mongodb", source_type],
    )
    def _capture_dag() -> None:
        @task
        def resolve_sources() -> list[dict[str, Any]]:
            gcp_conn_id = Variable.get("GOOGLE_SHEETS_CONN_ID", default_var="google_sheets_default")
            mongo_conn_id = Variable.get("MONGO_CAPTURE_CONN_ID", default_var="mongo_default")
            mongo_db = Variable.get("MONGO_CAPTURE_DB", default_var="capture_raw")
            ssh_conn_id = Variable.get("MONGO_CAPTURE_SSH_CONN_ID", default_var="ssh_capture_default")
            mongo_remote_host = Variable.get("MONGO_CAPTURE_REMOTE_HOST", default_var="127.0.0.1")
            mongo_remote_port = int(Variable.get("MONGO_CAPTURE_REMOTE_PORT", default_var="27017"))

            resolved: list[dict[str, Any]] = []
            for source in sources:
                spreadsheet_id = _get_required_variable(source["spreadsheet_id_var"])
                range_raw = Variable.get(source["range_var"], default_var=source["default_range"])
                range_name = _normalize_sheet_or_range(range_raw, default_range="A:Z")
                resolved.append(
                    {
                        "source_name": source["source_name"],
                        "source_type": source_type,
                        "spreadsheet_id": spreadsheet_id,
                        "range_name": range_name,
                        "collection": source["collection"],
                        "gcp_conn_id": gcp_conn_id,
                        "mongo_conn_id": mongo_conn_id,
                        "mongo_db": mongo_db,
                        "ssh_conn_id": ssh_conn_id,
                        "mongo_remote_host": mongo_remote_host,
                        "mongo_remote_port": mongo_remote_port,
                    }
                )

            return resolved

        @task
        def capture_source(source: dict[str, Any]) -> dict[str, Any]:
            try:
                from airflow.providers.ssh.hooks.ssh import SSHHook
            except Exception as exc:
                raise AirflowException(
                    "Missing SSH provider. Install: apache-airflow-providers-ssh"
                ) from exc

            context = get_current_context()
            run_id = context["run_id"]
            logical_date = context["logical_date"].to_iso8601_string()
            extracted_at = pendulum.now("UTC").to_iso8601_string()

            token_pickle_b64 = Variable.get("GOOGLE_SHEETS_TOKEN_PICKLE_B64", default_var="").strip()
            token_pickle_path = Variable.get("GOOGLE_SHEETS_TOKEN_PICKLE_PATH", default_var="").strip()
            # Backward compatibility: allow putting a file path in GOOGLE_SHEETS_TOKEN_PICKLE_B64.
            if token_pickle_b64 and not token_pickle_path and _looks_like_file_path(token_pickle_b64):
                token_pickle_path = token_pickle_b64
                token_pickle_b64 = ""

            if token_pickle_b64 or token_pickle_path:
                values = _load_values_via_token_pickle(
                    spreadsheet_id=source["spreadsheet_id"],
                    range_name=source["range_name"],
                    token_pickle_b64=token_pickle_b64,
                    token_pickle_path=token_pickle_path,
                )
            else:
                try:
                    from airflow.providers.google.suite.hooks.sheets import GSheetsHook
                except Exception as exc:
                    raise AirflowException(
                        "Missing Google provider. Install: apache-airflow-providers-google"
                    ) from exc

                gsheets_hook = GSheetsHook(gcp_conn_id=source["gcp_conn_id"])
                values = _call_get_values(
                    gsheets_hook=gsheets_hook,
                    spreadsheet_id=source["spreadsheet_id"],
                    range_name=source["range_name"],
                )

            if not values or len(values) < 2:
                return {
                    "source_name": source["source_name"],
                    "rows_inserted": 0,
                    "message": "Sheet has no data rows (header only or empty).",
                }

            headers = _build_headers(values[0])
            data_rows = values[1:]

            mongo_kwargs, default_db = _build_mongo_client_kwargs(source["mongo_conn_id"])
            target_db = source["mongo_db"] or default_db
            documents = []
            for row_number, row in enumerate(data_rows, start=2):
                normalized_row = [str(item) if item is not None else "" for item in row]
                row_payload = {
                    header: normalized_row[idx] if idx < len(normalized_row) else ""
                    for idx, header in enumerate(headers)
                }
                row_fingerprint = hashlib.sha256(
                    (source["source_name"] + "|" + "|".join(normalized_row)).encode("utf-8")
                ).hexdigest()

                documents.append(
                    {
                        "source": source["source_name"],
                        "source_type": source["source_type"],
                        "spreadsheet_id": source["spreadsheet_id"],
                        "sheet_range": source["range_name"],
                        "row_number": row_number,
                        "row_fingerprint": row_fingerprint,
                        "extracted_at": extracted_at,
                        "logical_date": logical_date,
                        "airflow_run_id": run_id,
                        "raw_payload": row_payload,
                        "raw_row": normalized_row,
                    }
                )

            ssh_hook = SSHHook(ssh_conn_id=source["ssh_conn_id"])
            tunnel = ssh_hook.get_tunnel(
                remote_port=int(source["mongo_remote_port"]),
                remote_host=source["mongo_remote_host"],
            )
            tunnel.start()

            mongo_client = None
            try:
                from pymongo import MongoClient

                mongo_client = MongoClient(
                    host="127.0.0.1",
                    port=tunnel.local_bind_port,
                    **mongo_kwargs,
                )
                collection = mongo_client[target_db][source["collection"]]
                if documents:
                    collection.insert_many(documents, ordered=False)
            finally:
                if mongo_client is not None:
                    mongo_client.close()
                tunnel.stop()

            return {
                "source_name": source["source_name"],
                "rows_inserted": len(documents),
                "mongo_db": target_db,
                "mongo_collection": source["collection"],
            }

        capture_source.expand(source=resolve_sources())

    return _capture_dag()
