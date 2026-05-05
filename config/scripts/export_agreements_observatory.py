#!/usr/bin/env python3
"""Normalize agreement records from MongoDB and export them to a Google Sheet."""

from __future__ import annotations

import argparse
import json
import logging
import os
import pickle
import stat
import time
import unicodedata
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from pymongo import MongoClient


DEFAULT_DB_NAME = "international"
DEFAULT_COLLECTION = "agreements"
DEFAULT_SOURCE_TYPE = "agreements"
DEFAULT_SPREADSHEET_ID = ""
DEFAULT_SHEET_NAME = "convenios"
DEFAULT_TOKEN_PATH = "/srv/mobilities_transform/secrets/token.pickle"

TARGET_COLUMNS = [
    "ámbito",
    "país_local",
    "institución_local",
    "continente",
    "código",
    "país/ciudad",
    "institución/entidad",
    "fecha_de_inicio",
    "fecha_de_finalización",
    "fecha_de_inicio_unix",
    "fecha_de_finalización_unix",
    "modalidad",
    "categorías",
    "resúmen",
    "idioma",
    "facultades",
    "unidad_académica_administrativa",
    "enlace",
    "estado",
]
INTERNAL_COLUMNS = {
    "__mongo_id",
    "__source",
    "__source_sheet",
    "__row_number",
    "__ambito",
    "__first_extracted_at",
    "__last_extracted_at",
}


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.lower().split())


def clean_cell(value: Any) -> str:
    text = "" if value is None else str(value).replace("\xa0", " ").strip()
    text = " ".join(text.split())
    return "" if normalize_text(text) in {"", "nan", "none", "no informa"} else text


def clean_or_default(value: Any, default: str = "") -> str:
    cleaned = clean_cell(value)
    return cleaned if cleaned else default


def titlecase_spanish(value: Any) -> str:
    text = clean_cell(value)
    if not text:
        return ""
    out = text.lower().title()
    return (
        out.replace(" De ", " de ")
        .replace(" Del ", " del ")
        .replace(" Y ", " y ")
        .replace(" La ", " la ")
    )


def parse_date_value(value: Any) -> date | None:
    text = clean_cell(value)
    if not text:
        return None
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        parsed = pd.to_datetime(text, errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return None
    return parsed.date()


def current_business_date() -> date:
    return datetime.now(ZoneInfo("America/Bogota")).date()


def normalize_status_from_source_and_end_date(
    *,
    source_sheet: Any,
    end_date_value: Any,
    today: date | None = None,
) -> str:
    sheet = normalize_text(source_sheet)
    if "inactivo" in sheet or "inactivos" in sheet:
        return "vencido"

    end_date = parse_date_value(end_date_value)
    if end_date is None:
        return "vigente"
    return "vencido" if end_date < (today or current_business_date()) else "vigente"


def map_ambito(value: Any, source: Any = "") -> str:
    norm = normalize_text(value)
    source_norm = normalize_text(source)
    if "internacional" in norm or "international" in norm:
        return "Internacional"
    if "nacional" in norm or "national" in norm:
        return "Nacional"
    if "internacional" in source_norm or "international" in source_norm:
        return "Internacional"
    if "nacional" in source_norm or "national" in source_norm:
        return "Nacional"
    return clean_cell(value)


def pick_column(df_raw: pd.DataFrame, candidates: list[str]) -> pd.Series:
    normalized_to_columns: dict[str, list[str]] = {}
    for col in df_raw.columns:
        normalized_to_columns.setdefault(normalize_text(col), []).append(col)

    result = pd.Series([""] * len(df_raw), index=df_raw.index, dtype="object")
    for candidate in candidates:
        for source_col in normalized_to_columns.get(normalize_text(candidate), []):
            source_values = df_raw[source_col].fillna("").astype(str).str.strip()
            fill_mask = result.eq("") & source_values.ne("")
            result = result.where(~fill_mask, source_values)
    return result


def parse_date_strings(value: Any) -> tuple[str, str]:
    text = clean_cell(value)
    if not text:
        return "", ""
    if len(text) >= 10 and text[4] == "-" and text[7] == "-":
        parsed = pd.to_datetime(text, errors="coerce")
    else:
        parsed = pd.to_datetime(text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return "", ""
    formatted = parsed.strftime("%d/%m/%Y")
    unix_seconds = str(int(parsed.timestamp()))
    return formatted, unix_seconds


def normalize_country(value: Any) -> str:
    text = titlecase_spanish(value)
    if normalize_text(text) == "estados unidos de america":
        return "Estados Unidos"
    return text


def is_international_scope(scope: Any) -> bool:
    return normalize_text(scope) == "internacional"


def build_raw_dataframe(
    *,
    db: Any,
    collection: str,
    source_type: str,
    batch_size: int,
) -> pd.DataFrame:
    rows: list[dict[str, Any]] = []
    projection = {
        "_id": 1,
        "source": 1,
        "source_type": 1,
        "source_sheet": 1,
        "row_number": 1,
        "raw_payload": 1,
        "ambito": 1,
        "first_extracted_at": 1,
        "last_extracted_at": 1,
    }
    cursor = db[collection].find({"source_type": source_type}, projection).batch_size(batch_size)
    try:
        for document in cursor:
            payload = dict(document.get("raw_payload") or {})
            source_sheet = document.get("source_sheet") or payload.get("source_sheet") or ""
            payload["__mongo_id"] = str(document.get("_id", ""))
            payload["__source"] = str(document.get("source", ""))
            payload["__source_sheet"] = str(source_sheet)
            payload["__row_number"] = str(document.get("row_number", ""))
            payload["__ambito"] = str(document.get("ambito") or payload.get("ambito") or "")
            payload["__first_extracted_at"] = str(document.get("first_extracted_at", ""))
            payload["__last_extracted_at"] = str(document.get("last_extracted_at", ""))
            rows.append(payload)
    finally:
        cursor.close()

    return pd.DataFrame(rows)


def build_export_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    df = pd.DataFrame(index=df_raw.index)
    source = df_raw.get("__source", pd.Series([""] * len(df_raw), index=df_raw.index))
    source_sheet = df_raw.get("__source_sheet", pd.Series([""] * len(df_raw), index=df_raw.index))
    scope = [
        map_ambito(ambito, source)
        for ambito, source in zip(
            df_raw.get("__ambito", pd.Series([""] * len(df_raw), index=df_raw.index)),
            source,
        )
    ]
    scope_series = pd.Series(scope, index=df_raw.index)
    is_international = scope_series.apply(is_international_scope)

    start_raw = pick_column(df_raw, ["FECHA DE INICIO", "FECHA INICIO"])
    end_raw = pick_column(df_raw, ["FECHA DE FINALIZACIÓN", "FECHA DE FINALIZACION", "FECHA FINALIZACIÓN"])
    start_parsed = start_raw.apply(parse_date_strings)
    end_parsed = end_raw.apply(parse_date_strings)

    modalidad = pick_column(df_raw, ["MODALIDAD"])
    pais = pick_column(df_raw, ["PAÍS", "PAIS"])

    city = pick_column(df_raw, ["CIUDAD"]).where(~is_international, "")
    international_institution = pick_column(
        df_raw,
        ["UNIVERSIDAD O ENTIDAD EXTRANJERA", "INSTITUCIÓN", "INSTITUCION"],
    ).where(is_international, "")
    national_institution = pick_column(df_raw, ["UNIVERSIDAD O ENTIDAD"]).where(~is_international, "")
    international_link = pick_column(df_raw, ["CONVENIO VIGENTE"]).where(is_international, "")
    national_link = pick_column(df_raw, ["URL"]).where(~is_international, "")

    df["ámbito"] = scope_series
    df["país_local"] = "Colombia"
    df["institución_local"] = "Universidad de Antioquia"
    df["continente"] = pick_column(df_raw, ["CONTINENTE"]).where(is_international, "")
    df["código"] = pick_column(
        df_raw,
        ["CÓDIGO CONVENIO", "CODIGO CONVENIO", "CÓDIGO_CONVENIO", "CODIGO_CONVENIO"],
    )
    df["país/ciudad"] = pais.where(is_international, city).apply(normalize_country)
    df["institución/entidad"] = international_institution.where(
        is_international,
        national_institution,
    )
    df["fecha_de_inicio"] = start_parsed.apply(lambda value: value[0])
    df["fecha_de_finalización"] = end_parsed.apply(lambda value: value[0])
    df["fecha_de_inicio_unix"] = start_parsed.apply(lambda value: value[1])
    df["fecha_de_finalización_unix"] = end_parsed.apply(lambda value: value[1])
    df["modalidad"] = modalidad.apply(titlecase_spanish)
    category = pick_column(df_raw, ["CATEGORÍA", "CATEGORIA"])
    subcategory = pick_column(df_raw, ["SUBCATEGORÍA", "SUBCATEGORIA"]).where(
        ~is_international,
        "",
    )
    df["categorías"] = [
        " - ".join(part for part in [clean_cell(cat), clean_cell(subcat)] if part)
        for cat, subcat in zip(category, subcategory)
    ]
    df["resúmen"] = pick_column(df_raw, ["DESCRIPCIÓN", "DESCRIPCION"])
    df["idioma"] = pick_column(df_raw, ["IDIOMA DIFUSIÓN", "IDIOMA DIFUSION"]).where(
        is_international,
        "",
    )
    df["facultades"] = pick_column(df_raw, ["UNIDADES ACADÉMICAS", "UNIDADES ACADEMICAS", "UNIDAD ACADÉMICA", "UNIDAD ACADEMICA"])
    df["unidad_académica_administrativa"] = pick_column(
        df_raw,
        ["DEPENDENCIA RESPONSABLE UDEA"],
    ).where(is_international, "")
    df["enlace"] = international_link.where(is_international, national_link)
    today = current_business_date()
    df["estado"] = [
        normalize_status_from_source_and_end_date(
            source_sheet=sheet,
            end_date_value=end_date_value,
            today=today,
        )
        for sheet, end_date_value in zip(source_sheet, end_raw)
    ]

    for column in df.columns:
        df[column] = df[column].apply(clean_cell)
    df["país/ciudad"] = [
        clean_or_default(location, "No informa")
        for location in df["país/ciudad"]
    ]

    result = df[TARGET_COLUMNS].copy()
    result = result.drop_duplicates(keep="first")
    result = result.sort_values(
        by=["ámbito", "estado", "código", "país/ciudad", "institución/entidad"],
        kind="stable",
    ).reset_index(drop=True)
    return result


def load_credentials(token_path: Path) -> Any:
    if not token_path.exists():
        raise FileNotFoundError(f"token.pickle not found at: {token_path}")

    token_mode = stat.S_IMODE(token_path.stat().st_mode)
    if token_mode & 0o077:
        raise PermissionError(
            f"Refusing to read token.pickle with permissive mode {oct(token_mode)}. Use chmod 600."
        )

    with token_path.open("rb") as token_file:
        creds = pickle.load(token_file)

    if creds is None:
        raise RuntimeError("token.pickle does not contain valid credentials")

    if not creds.valid:
        if creds.expired and creds.refresh_token:
            try:
                creds.refresh(Request())
            except RefreshError as exc:
                raise RuntimeError(
                    "Could not refresh token.pickle. Regenerate the OAuth token."
                ) from exc
            with token_path.open("wb") as token_file:
                pickle.dump(creds, token_file)
        else:
            raise RuntimeError("Google credentials are invalid and cannot be refreshed")

    required_scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if hasattr(creds, "has_scopes") and not creds.has_scopes(required_scopes):
        raise RuntimeError(
            "Google token is missing the Sheets scope: https://www.googleapis.com/auth/spreadsheets"
        )

    return creds


def backup_spreadsheet(sheets_service: Any, spreadsheet_id: str) -> tuple[str, str]:
    source_meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    source_title = source_meta.get("properties", {}).get("title", "Spreadsheet")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")
    backup_title = f"{source_title} - backup {timestamp}"

    backup_spreadsheet = sheets_service.spreadsheets().create(
        body={"properties": {"title": backup_title}}
    ).execute()
    backup_id = backup_spreadsheet["spreadsheetId"]

    backup_meta_initial = sheets_service.spreadsheets().get(
        spreadsheetId=backup_id,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    initial_sheet_id = backup_meta_initial["sheets"][0]["properties"]["sheetId"]
    temp_initial_title = f"__tmp_backup_{timestamp}__"
    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=backup_id,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": initial_sheet_id,
                            "title": temp_initial_title,
                        },
                        "fields": "title",
                    }
                }
            ]
        },
    ).execute()

    copied_sheet_ids: list[int] = []
    rename_requests: list[dict[str, Any]] = []
    for sheet in source_meta.get("sheets", []):
        src_sheet_id = sheet["properties"]["sheetId"]
        src_title = sheet["properties"]["title"]
        copied = sheets_service.spreadsheets().sheets().copyTo(
            spreadsheetId=spreadsheet_id,
            sheetId=src_sheet_id,
            body={"destinationSpreadsheetId": backup_id},
        ).execute()
        new_sheet_id = copied["sheetId"]
        copied_sheet_ids.append(new_sheet_id)
        rename_requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": new_sheet_id, "title": src_title},
                    "fields": "title",
                }
            }
        )

    if rename_requests:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=backup_id,
            body={"requests": rename_requests},
        ).execute()

    backup_meta = sheets_service.spreadsheets().get(spreadsheetId=backup_id).execute()
    delete_requests = []
    for sheet in backup_meta.get("sheets", []):
        sheet_id = sheet["properties"]["sheetId"]
        if sheet_id not in copied_sheet_ids:
            delete_requests.append({"deleteSheet": {"sheetId": sheet_id}})

    if delete_requests:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=backup_id,
            body={"requests": delete_requests},
        ).execute()

    return backup_id, backup_title


def ensure_sheet_exists(sheets_service: Any, spreadsheet_id: str, sheet_name: str) -> dict[str, Any]:
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in meta.get("sheets", []):
        if sheet.get("properties", {}).get("title") == sheet_name:
            return sheet

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={"requests": [{"addSheet": {"properties": {"title": sheet_name}}}]},
    ).execute()

    refreshed = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    for sheet in refreshed.get("sheets", []):
        if sheet.get("properties", {}).get("title") == sheet_name:
            return sheet

    raise RuntimeError(f"Could not create or locate sheet '{sheet_name}' in spreadsheet.")


def ensure_sheet_grid_capacity(
    *,
    sheets_service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    required_rows: int,
    required_columns: int,
) -> None:
    sheet = ensure_sheet_exists(sheets_service, spreadsheet_id, sheet_name)
    properties = sheet.get("properties", {})
    sheet_id = properties["sheetId"]
    grid = properties.get("gridProperties", {})
    current_rows = int(grid.get("rowCount", 0) or 0)
    current_columns = int(grid.get("columnCount", 0) or 0)

    target_rows = max(current_rows, required_rows)
    target_columns = max(current_columns, required_columns)
    if target_rows == current_rows and target_columns == current_columns:
        return

    sheets_service.spreadsheets().batchUpdate(
        spreadsheetId=spreadsheet_id,
        body={
            "requests": [
                {
                    "updateSheetProperties": {
                        "properties": {
                            "sheetId": sheet_id,
                            "gridProperties": {
                                "rowCount": target_rows,
                                "columnCount": target_columns,
                            },
                        },
                        "fields": "gridProperties.rowCount,gridProperties.columnCount",
                    }
                }
            ]
        },
    ).execute()


def dataframe_values(dataframe: pd.DataFrame) -> list[list[Any]]:
    clean = dataframe.astype(object).where(pd.notna(dataframe), "")
    return clean.values.tolist()


def write_sheet_dataframe(
    *,
    sheets_service: Any,
    spreadsheet_id: str,
    sheet_name: str,
    dataframe: pd.DataFrame,
    chunk_size: int,
) -> int:
    ensure_sheet_grid_capacity(
        sheets_service=sheets_service,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
        required_rows=max(2, len(dataframe.index) + 1),
        required_columns=max(1, len(dataframe.columns)),
    )

    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body={"values": [list(dataframe.columns)]},
    ).execute()

    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A2:ZZZ",
        body={},
    ).execute()

    if dataframe.empty:
        return 0

    values = dataframe_values(dataframe)
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        start_row = start + 2
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"'{sheet_name}'!A{start_row}",
            valueInputOption="RAW",
            body={"values": chunk},
        ).execute()

    return len(values)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export raw agreement records to Google Sheets.")
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    parser.add_argument("--db", default=os.getenv("AGREEMENTS_RAW_DB", DEFAULT_DB_NAME))
    parser.add_argument(
        "--collection",
        default=os.getenv("AGREEMENTS_RAW_COLLECTION", DEFAULT_COLLECTION),
    )
    parser.add_argument(
        "--source-type",
        default=os.getenv("AGREEMENTS_RAW_SOURCE_TYPE", DEFAULT_SOURCE_TYPE),
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("AGREEMENTS_EXPORT_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
    )
    parser.add_argument(
        "--sheet-name",
        default=os.getenv("AGREEMENTS_EXPORT_SHEET_NAME", DEFAULT_SHEET_NAME),
    )
    parser.add_argument(
        "--token-path",
        default=os.getenv("GOOGLE_SHEETS_TOKEN_PICKLE_PATH", DEFAULT_TOKEN_PATH),
    )
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument("--mongo-timeout-ms", type=int, default=10000)
    parser.add_argument("--skip-backup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    if not str(args.spreadsheet_id).strip():
        raise ValueError(
            "Missing spreadsheet id. Pass --spreadsheet-id or set AGREEMENTS_EXPORT_SPREADSHEET_ID."
        )

    started = time.perf_counter()
    token_path = Path(args.token_path).expanduser()

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=args.mongo_timeout_ms)
    try:
        client.admin.command("ping")
        logging.info("Connected to MongoDB database '%s' collection '%s'", args.db, args.collection)
        df_raw = build_raw_dataframe(
            db=client[args.db],
            collection=args.collection,
            source_type=args.source_type,
            batch_size=args.batch_size,
        )
    finally:
        client.close()

    exported = build_export_dataframe(df_raw)
    summary: dict[str, Any] = {
        "db": args.db,
        "collection": args.collection,
        "source_type": args.source_type,
        "raw_rows": int(len(df_raw.index)),
        "export_rows": int(len(exported.index)),
        "target_sheet": args.sheet_name,
    }

    if args.dry_run:
        summary["columns"] = list(exported.columns)
        summary["preview"] = exported.head(10).astype(str).to_dict(orient="records")
        summary["elapsed_seconds"] = round(time.perf_counter() - started, 2)
        print(json.dumps(summary, ensure_ascii=False, sort_keys=True))
        return

    creds = load_credentials(token_path)
    sheets_service = build("sheets", "v4", credentials=creds)
    ensure_sheet_exists(sheets_service, args.spreadsheet_id, args.sheet_name)

    if args.skip_backup:
        summary["backup_spreadsheet_id"] = None
        summary["backup_title"] = None
    else:
        backup_id, backup_title = backup_spreadsheet(sheets_service, args.spreadsheet_id)
        summary["backup_spreadsheet_id"] = backup_id
        summary["backup_title"] = backup_title

    rows_written = write_sheet_dataframe(
        sheets_service=sheets_service,
        spreadsheet_id=args.spreadsheet_id,
        sheet_name=args.sheet_name,
        dataframe=exported,
        chunk_size=args.chunk_size,
    )
    summary["rows_written"] = rows_written
    summary["elapsed_seconds"] = round(time.perf_counter() - started, 2)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
