"""Reusable capture DAG builder for Google Sheets -> MongoDB raw collections."""

from __future__ import annotations

import base64
import hashlib
import io
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


def _normalize_sheet_or_range(value: str, default_range: str = "A:ZZZ") -> str:
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


def _derive_via(source_name: str) -> str:
    """Infer mobility direction from source name."""
    lowered = source_name.lower()
    if "entrante" in lowered:
        return "Entrante"
    if "saliente" in lowered:
        return "Saliente"
    return "NoDefinida"


def _derive_ambito(source_name: str) -> str:
    """Infer mobility scope from source name."""
    lowered = source_name.lower()
    if "international" in lowered or "internacional" in lowered:
        return "internacional"
    if "national" in lowered or "nacional" in lowered:
        return "nacional"
    return "no_definido"


def _parse_sheet_names(value: str) -> list[str]:
    """Parse comma/newline separated sheet names."""
    raw = str(value).replace(";", ",").replace("\n", ",")
    parsed = [item.strip() for item in raw.split(",") if item.strip()]
    return parsed


def _get_google_credentials(
    *,
    token_pickle_b64: str = "",
    token_pickle_path: str = "",
) -> Any:
    """Load and refresh OAuth credentials serialized in token.pickle."""
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
        from google.auth.transport.requests import Request

        creds.refresh(Request())

    if not creds or not getattr(creds, "valid", False):
        raise AirflowException("No valid credentials in token.pickle. Regenerate OAuth token.")

    return creds


def _load_values_via_token_pickle(
    *,
    spreadsheet_id: str,
    range_name: str,
    token_pickle_b64: str = "",
    token_pickle_path: str = "",
) -> list[list[str]]:
    """Load sheet values using OAuth credentials serialized in token.pickle."""
    try:
        from googleapiclient.discovery import build
    except Exception as exc:
        raise AirflowException(
            "Missing Google API libs for token.pickle auth. Install apache-airflow-providers-google."
        ) from exc

    creds = _get_google_credentials(
        token_pickle_b64=token_pickle_b64,
        token_pickle_path=token_pickle_path,
    )

    service = build("sheets", "v4", credentials=creds)
    values = (
        service.spreadsheets()
        .values()
        .get(spreadsheetId=spreadsheet_id, range=range_name)
        .execute()
        .get("values", [])
    )
    return values


def _load_rows_via_drive_excel(
    *,
    file_id: str,
    sheet_names: list[str],
    token_pickle_path: str,
) -> list[dict[str, Any]]:
    """Download Excel-like content from Drive and return normalized row records.

    Supports both:
    - Binary `.xlsx` files (`files.get_media`)
    - Google Sheets native files (`files.export_media` to xlsx)
    """
    try:
        import pandas as pd
        from googleapiclient.discovery import build
        from googleapiclient.errors import HttpError
        from googleapiclient.http import MediaIoBaseDownload
    except Exception as exc:
        raise AirflowException(
            "Missing dependencies for Drive Excel capture. Install openpyxl and google-api-python-client."
        ) from exc

    creds = _get_google_credentials(token_pickle_path=token_pickle_path)
    drive = build("drive", "v3", credentials=creds)

    def _download_to_buffer(request: Any) -> io.BytesIO:
        buffer = io.BytesIO()
        downloader = MediaIoBaseDownload(buffer, request)
        done = False
        while not done:
            _, done = downloader.next_chunk()
        buffer.seek(0)
        return buffer

    try:
        buffer = _download_to_buffer(drive.files().get_media(fileId=file_id))
    except HttpError as exc:
        # Google Sheets native files are not directly downloadable via get_media.
        # Fall back to exporting as xlsx.
        details = str(exc)
        if getattr(exc, "resp", None) and exc.resp.status == 403 and "fileNotDownloadable" in details:
            export_request = drive.files().export_media(
                fileId=file_id,
                mimeType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            )
            buffer = _download_to_buffer(export_request)
        else:
            raise
    try:
        excel_obj = pd.read_excel(
            buffer,
            sheet_name=sheet_names if sheet_names else None,
            dtype=str,
            engine="openpyxl",
        )
    except Exception as exc:
        raise AirflowException(
            "Could not read Drive Excel file. Ensure openpyxl is installed and sheet names are valid."
        ) from exc

    if isinstance(excel_obj, dict):
        sheet_frames = excel_obj
    else:
        sheet_frames = {"Sheet1": excel_obj}

    records: list[dict[str, Any]] = []
    for sheet_name, dataframe in sheet_frames.items():
        if dataframe is None:
            continue
        dataframe = dataframe.fillna("")
        headers = _build_headers([str(col) for col in list(dataframe.columns)])

        for row_number, row in enumerate(dataframe.itertuples(index=False, name=None), start=2):
            normalized_row = [str(item) if item is not None else "" for item in row]
            row_payload = {
                header: normalized_row[idx] if idx < len(normalized_row) else ""
                for idx, header in enumerate(headers)
            }
            records.append(
                {
                    "row_number": row_number,
                    "raw_row": normalized_row,
                    "raw_payload": row_payload,
                    "source_sheet": str(sheet_name),
                }
            )

    return records


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
            mongo_db = Variable.get("MONGO_CAPTURE_DB", default_var="international")
            ssh_conn_id = Variable.get("MONGO_CAPTURE_SSH_CONN_ID", default_var="ssh_capture_default")
            mongo_remote_host = Variable.get("MONGO_CAPTURE_REMOTE_HOST", default_var="127.0.0.1")
            mongo_remote_port = int(Variable.get("MONGO_CAPTURE_REMOTE_PORT", default_var="27017"))

            resolved: list[dict[str, Any]] = []
            for source in sources:
                spreadsheet_id = _get_required_variable(source["spreadsheet_id_var"])
                source_mode = str(source.get("source_mode", "google_sheets")).strip().lower()
                range_raw = Variable.get(source["range_var"], default_var=source["default_range"])

                if source_mode == "drive_excel":
                    drive_sheet_names = _parse_sheet_names(range_raw)
                    range_name = ",".join(drive_sheet_names) if drive_sheet_names else "ALL_SHEETS"
                    drive_token_path_var = source.get(
                        "drive_token_path_var",
                        "GOOGLE_DRIVE_TOKEN_PICKLE_PATH",
                    )
                    drive_token_path_default = source.get(
                        "drive_token_path_default",
                        "/opt/airflow/config/secrets/agreements/agreements_token.pickle",
                    )
                    drive_token_path = Variable.get(
                        drive_token_path_var,
                        default_var=drive_token_path_default,
                    ).strip()
                else:
                    range_name = _normalize_sheet_or_range(range_raw, default_range="A:ZZZ")
                    drive_sheet_names = []
                    drive_token_path = ""

                resolved.append(
                    {
                        "source_name": source["source_name"],
                        "source_mode": source_mode,
                        "source_type": source_type,
                        "spreadsheet_id": spreadsheet_id,
                        "range_name": range_name,
                        "drive_sheet_names": drive_sheet_names,
                        "drive_token_path": drive_token_path,
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

            source_mode = str(source.get("source_mode", "google_sheets")).strip().lower()
            if source_mode == "drive_excel":
                if not source.get("drive_token_path"):
                    raise AirflowException(
                        "Missing drive token path for drive_excel source. Set GOOGLE_DRIVE_TOKEN_PICKLE_PATH."
                    )
                records = _load_rows_via_drive_excel(
                    file_id=source["spreadsheet_id"],
                    sheet_names=source.get("drive_sheet_names", []),
                    token_pickle_path=source["drive_token_path"],
                )
            else:
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
                records = []
                for row_number, row in enumerate(data_rows, start=2):
                    normalized_row = [str(item) if item is not None else "" for item in row]
                    row_payload = {
                        header: normalized_row[idx] if idx < len(normalized_row) else ""
                        for idx, header in enumerate(headers)
                    }
                    records.append(
                        {
                            "row_number": row_number,
                            "raw_row": normalized_row,
                            "raw_payload": row_payload,
                            "source_sheet": "",
                        }
                    )

            if not records:
                return {
                    "source_name": source["source_name"],
                    "rows_inserted": 0,
                    "message": "Source has no data rows.",
                }

            via = _derive_via(source["source_name"])
            ambito = _derive_ambito(source["source_name"])

            mongo_kwargs, default_db = _build_mongo_client_kwargs(source["mongo_conn_id"])
            target_db = source["mongo_db"] or default_db
            operations = []
            for record in records:
                row_number = int(record["row_number"])
                normalized_row = [str(item) if item is not None else "" for item in record["raw_row"]]
                row_payload = dict(record["raw_payload"])
                row_payload["vía"] = via
                row_payload["ambito"] = ambito
                source_sheet = str(record.get("source_sheet", "")).strip()
                if source_sheet:
                    row_payload["source_sheet"] = source_sheet

                fingerprint_parts = [source["source_name"]]
                if source_sheet:
                    fingerprint_parts.append(source_sheet)
                fingerprint_parts.append("|".join(normalized_row))
                row_fingerprint = hashlib.sha256(
                    "|".join(fingerprint_parts).encode("utf-8")
                ).hexdigest()

                operations.append(
                    {
                        "filter": {
                            "source": source["source_name"],
                            "row_fingerprint": row_fingerprint,
                        },
                        "update": {
                            "$setOnInsert": {
                                "source": source["source_name"],
                                "source_type": source["source_type"],
                                "spreadsheet_id": source["spreadsheet_id"],
                                "row_fingerprint": row_fingerprint,
                                "first_extracted_at": extracted_at,
                                "first_logical_date": logical_date,
                                "first_airflow_run_id": run_id,
                            },
                            "$set": {
                                "sheet_range": source["range_name"],
                                "row_number": row_number,
                                "source_sheet": source_sheet,
                                "raw_payload": row_payload,
                                "raw_row": normalized_row,
                                "vía": via,
                                "ambito": ambito,
                                "last_extracted_at": extracted_at,
                                "last_logical_date": logical_date,
                                "last_airflow_run_id": run_id,
                            },
                        },
                    }
                )

            ssh_hook = SSHHook(ssh_conn_id=source["ssh_conn_id"])
            tunnel = ssh_hook.get_tunnel(
                remote_port=int(source["mongo_remote_port"]),
                remote_host=source["mongo_remote_host"],
            )
            tunnel.start()

            mongo_client = None
            inserted = 0
            updated = 0
            try:
                from pymongo import MongoClient, UpdateOne

                mongo_client = MongoClient(
                    host="127.0.0.1",
                    port=tunnel.local_bind_port,
                    **mongo_kwargs,
                )
                collection = mongo_client[target_db][source["collection"]]
                if operations:
                    result = collection.bulk_write(
                        [
                            UpdateOne(
                                filter=operation["filter"],
                                update=operation["update"],
                                upsert=True,
                            )
                            for operation in operations
                        ],
                        ordered=False,
                    )
                    inserted = int(result.upserted_count or 0)
                    updated = int(result.modified_count or 0)
            finally:
                if mongo_client is not None:
                    mongo_client.close()
                tunnel.stop()

            return {
                "source_name": source["source_name"],
                "rows_processed": len(records),
                "rows_inserted": inserted,
                "rows_updated": updated,
                "mongo_db": target_db,
                "mongo_collection": source["collection"],
            }

        capture_source.expand(source=resolve_sources())

    return _capture_dag()
