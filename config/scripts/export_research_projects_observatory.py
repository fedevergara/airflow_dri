#!/usr/bin/env python3
"""Export research projects with non-UdeA entities between Google Sheets."""

from __future__ import annotations

import argparse
import json
import logging
import os
import pickle
import random
import re
import stat
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable


DEFAULT_SOURCE_SPREADSHEET_ID = ""
DEFAULT_TARGET_SPREADSHEET_ID = ""
DEFAULT_SOURCE_SHEET_NAME = "Hoja 1"
DEFAULT_TARGET_SHEET_NAME = "Hoja 1"
DEFAULT_TOKEN_PATH = "/srv/kahi_exports/secrets/token.pickle"

SOURCE_COLUMNS = {
    "project_code": "CODIGO_PROYECTO",
    "year": "AÑO_INICIO",
    "start_date": "FECHA_INICIO",
    "end_date": "FECHA_FINALIZACION",
    "area": "Area OCDE, con respectoa  los grupos",
    "group": "GRUPO",
    "entity": "RAZON_SOCIAL_APORTANTE",
    "contributor_type": "TIPO_APORTANTE",
    "cash": "TOTAL_FRESCO",
    "in_kind": "TOTAL_ESPECIE",
    "currency": "MONEDA",
    "country": "PAIS_ENTIDAD",
}
TARGET_COLUMNS = [
    "código_proyecto",
    "año",
    "fecha_de_inicio",
    "fecha_de_finalización",
    "area",
    "grupo",
    "institución/entidad",
    "tipo_aportante",
    "fresco",
    "especie",
    "moneda",
    "pais",
]
MANAGED_TARGET_COLUMNS = set(TARGET_COLUMNS) | {"unidad_académica_administrativa"}
EMPTY_MARKERS = {"", "nan", "none", "null", "#n/a", "#n/a ()", "no informa"}
TRANSIENT_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}


def normalize_text(value: Any) -> str:
    text = unicodedata.normalize("NFKD", str(value or ""))
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.lower().split())


def clean_cell(value: Any) -> str:
    text = " ".join(str(value or "").replace("\xa0", " ").split())
    return "" if normalize_text(text) in EMPTY_MARKERS else text


def normalize_header(value: Any) -> str:
    return normalize_text(value).replace("_", " ")


def is_udea_entity(value: Any) -> bool:
    normalized = normalize_text(value).replace(".", "")
    return (
        normalized in {"udea", "universidad de antioquia", "universidad antioquia"}
        or "universidad de antioquia" in normalized
    )


def normalize_entity_name(value: Any) -> str:
    return clean_cell(value).upper()


def parse_date(value: Any) -> str:
    text = clean_cell(value)
    if not text:
        return ""

    normalized = text.split("T", 1)[0].strip()
    for date_format in ("%d/%m/%Y", "%Y-%m-%d", "%d-%m-%Y", "%Y/%m/%d"):
        try:
            return datetime.strptime(normalized, date_format).strftime("%d/%m/%Y")
        except ValueError:
            continue
    raise ValueError(f"Unsupported date value: {text!r}")


def parse_year(value: Any, start_date: str) -> int | str:
    text = clean_cell(value)
    if text:
        try:
            year = int(float(text))
        except ValueError as exc:
            raise ValueError(f"Unsupported year value: {text!r}") from exc
        if 1900 <= year <= 2200:
            return year
        raise ValueError(f"Year is outside the accepted range: {year}")
    return int(start_date[-4:]) if start_date else ""


def parse_amount(value: Any) -> int | float | str:
    text = clean_cell(value)
    if not text:
        return ""

    compact = re.sub(r"[$\s]", "", text)
    if re.fullmatch(r"-?\d{1,3}(,\d{3})+(\.\d+)?", compact):
        compact = compact.replace(",", "")
    elif re.fullmatch(r"-?\d+(\.\d+)?", compact):
        pass
    else:
        raise ValueError(f"Unsupported monetary value: {text!r}")

    number = float(compact)
    return int(number) if number.is_integer() else number


def resolve_source_indices(headers: list[Any]) -> dict[str, int]:
    normalized_indices: dict[str, int] = {}
    for index, header in enumerate(headers):
        normalized_indices.setdefault(normalize_header(header), index)

    indices: dict[str, int] = {}
    missing: list[str] = []
    for logical_name, source_header in SOURCE_COLUMNS.items():
        index = normalized_indices.get(normalize_header(source_header))
        if index is None:
            missing.append(source_header)
        else:
            indices[logical_name] = index

    if missing:
        raise ValueError(f"Missing required source columns: {', '.join(missing)}")
    return indices


def build_export_rows(values: list[list[Any]]) -> tuple[list[list[Any]], dict[str, int]]:
    if not values:
        raise ValueError("The source sheet is empty and has no header row")

    headers = values[0]
    indices = resolve_source_indices(headers)
    required_width = max(indices.values()) + 1
    output: list[list[Any]] = []
    seen: set[tuple[Any, ...]] = set()
    source_projects: set[str] = set()
    external_projects: set[str] = set()
    external_entity_rows = 0
    rows_without_year_or_dates_omitted = 0
    selected_rows = 0

    for row_number, source_row in enumerate(values[1:], start=2):
        row = list(source_row) + [""] * max(0, required_width - len(source_row))
        if not any(clean_cell(cell) for cell in row):
            continue

        project_code = clean_cell(row[indices["project_code"]])
        if project_code:
            source_projects.add(project_code)

        entity = clean_cell(row[indices["entity"]])
        if not entity or is_udea_entity(entity):
            continue

        external_entity_rows += 1

        try:
            start_date = parse_date(row[indices["start_date"]])
            end_date = parse_date(row[indices["end_date"]])
            year = parse_year(row[indices["year"]], start_date)
            cash = parse_amount(row[indices["cash"]])
            in_kind = parse_amount(row[indices["in_kind"]])
        except ValueError as exc:
            raise ValueError(f"Source row {row_number}: {exc}") from exc

        if not year and not start_date and not end_date:
            rows_without_year_or_dates_omitted += 1
            continue

        selected_rows += 1
        if project_code:
            external_projects.add(project_code)

        target_row: list[Any] = [
            project_code,
            year,
            start_date,
            end_date,
            clean_cell(row[indices["area"]]),
            clean_cell(row[indices["group"]]),
            normalize_entity_name(entity),
            clean_cell(row[indices["contributor_type"]]),
            cash,
            in_kind,
            clean_cell(row[indices["currency"]]).upper(),
            clean_cell(row[indices["country"]]),
        ]

        dedupe_key = tuple(target_row)
        if dedupe_key in seen:
            continue
        seen.add(dedupe_key)
        output.append(target_row)

    output.sort(
        key=lambda row: (
            normalize_text(row[0]),
            str(row[1]),
            normalize_text(row[6]),
            normalize_text(row[5]),
            normalize_text(row[4]),
            str(row[2]),
        )
    )
    return output, {
        "source_rows": max(0, len(values) - 1),
        "source_unique_projects": len(source_projects),
        "external_entity_rows": external_entity_rows,
        "rows_without_year_or_dates_omitted": rows_without_year_or_dates_omitted,
        "selected_source_rows": selected_rows,
        "external_unique_projects": len(external_projects),
        "output_rows": len(output),
        "duplicates_removed": selected_rows - len(output),
    }


class SheetsRequestController:
    """Throttle write requests and retry transient Google API errors."""

    def __init__(
        self,
        *,
        min_interval_seconds: float = 1.2,
        max_attempts: int = 8,
        sleep_fn: Callable[[float], None] = time.sleep,
        monotonic_fn: Callable[[], float] = time.monotonic,
        jitter_fn: Callable[[], float] = random.random,
    ) -> None:
        if min_interval_seconds < 0:
            raise ValueError("min_interval_seconds cannot be negative")
        if max_attempts < 1:
            raise ValueError("max_attempts must be at least one")
        self.min_interval_seconds = float(min_interval_seconds)
        self.max_attempts = int(max_attempts)
        self.sleep_fn = sleep_fn
        self.monotonic_fn = monotonic_fn
        self.jitter_fn = jitter_fn
        self.last_write_started: float | None = None

    def _throttle(self) -> None:
        now = float(self.monotonic_fn())
        if self.last_write_started is not None:
            remaining = self.min_interval_seconds - (now - self.last_write_started)
            if remaining > 0:
                self.sleep_fn(remaining)
                now = float(self.monotonic_fn())
        self.last_write_started = now

    def execute(self, request: Any, *, operation: str) -> Any:
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            try:
                return request.execute()
            except Exception as exc:
                status = getattr(getattr(exc, "resp", None), "status", None)
                if status not in TRANSIENT_HTTP_STATUS_CODES or attempt >= self.max_attempts:
                    raise
                delay = min((2 ** (attempt - 1)) + float(self.jitter_fn()), 64.0)
                logging.warning(
                    "Google Sheets %s failed with HTTP %s; retrying in %.2fs "
                    "(attempt %s/%s)",
                    operation,
                    status,
                    delay,
                    attempt + 1,
                    self.max_attempts,
                )
                self.sleep_fn(delay)
        raise RuntimeError(f"Google Sheets {operation} exhausted retries")


def load_credentials(token_path: Path) -> Any:
    from google.auth.exceptions import RefreshError
    from google.auth.transport.requests import Request

    if not token_path.exists():
        raise FileNotFoundError(f"token.pickle not found at: {token_path}")
    token_mode = stat.S_IMODE(token_path.stat().st_mode)
    if token_mode & 0o077:
        raise PermissionError(
            f"Refusing to read token.pickle with permissive mode {oct(token_mode)}. "
            "Use chmod 600."
        )
    with token_path.open("rb") as token_file:
        credentials = pickle.load(token_file)
    if credentials is None:
        raise RuntimeError("token.pickle does not contain valid credentials")
    if not credentials.valid:
        if not (credentials.expired and credentials.refresh_token):
            raise RuntimeError("Google credentials are invalid and cannot be refreshed")
        try:
            credentials.refresh(Request())
        except RefreshError as exc:
            raise RuntimeError("Could not refresh token.pickle") from exc
        with token_path.open("wb") as token_file:
            pickle.dump(credentials, token_file)
    required_scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    if hasattr(credentials, "has_scopes") and not credentials.has_scopes(required_scopes):
        raise RuntimeError("Google token is missing the Sheets scope")
    return credentials


def quote_sheet_title(sheet_name: str) -> str:
    return "'" + str(sheet_name).replace("'", "''") + "'"


def column_index_to_label(column_index: int) -> str:
    if column_index < 1:
        raise ValueError("column_index must be greater than zero")
    label = ""
    current = column_index
    while current:
        current, remainder = divmod(current - 1, 26)
        label = chr(65 + remainder) + label
    return label


def response_has_values(response: dict[str, Any]) -> bool:
    return any(str(cell or "").strip() for row in response.get("values", []) for cell in row)


def response_contains_only_managed_columns(response: dict[str, Any]) -> bool:
    rows = response.get("values", [])
    if not response_has_values(response):
        return True
    width = max((len(row) for row in rows), default=0)
    managed_headers = {normalize_header(column) for column in MANAGED_TARGET_COLUMNS}
    for column_index in range(width):
        column_has_values = any(
            column_index < len(row) and str(row[column_index] or "").strip()
            for row in rows
        )
        if not column_has_values:
            continue
        header = rows[0][column_index] if rows and column_index < len(rows[0]) else ""
        if normalize_header(header) not in managed_headers:
            return False
    return True


def read_source_values(
    sheets_service: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
) -> list[list[Any]]:
    response = sheets_service.spreadsheets().values().get(
        spreadsheetId=spreadsheet_id,
        range=f"{quote_sheet_title(sheet_name)}!A:ZZ",
        valueRenderOption="UNFORMATTED_VALUE",
        dateTimeRenderOption="FORMATTED_STRING",
        majorDimension="ROWS",
    ).execute()
    return response.get("values", [])


def get_sheet_properties(
    sheets_service: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
) -> dict[str, Any]:
    metadata = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))",
    ).execute()
    for sheet in metadata.get("sheets", []):
        properties = sheet["properties"]
        if properties.get("title") == sheet_name:
            return properties
    raise RuntimeError(f"Sheet {sheet_name!r} does not exist")


def backup_spreadsheet(
    sheets_service: Any,
    *,
    spreadsheet_id: str,
    controller: SheetsRequestController,
) -> tuple[str, str]:
    source_meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    source_title = source_meta.get("properties", {}).get("title", "Spreadsheet")
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_UTC")
    backup_title = f"{source_title} - backup {timestamp}"
    backup = controller.execute(
        sheets_service.spreadsheets().create(body={"properties": {"title": backup_title}}),
        operation="backup spreadsheet creation",
    )
    backup_id = backup["spreadsheetId"]
    backup_meta = sheets_service.spreadsheets().get(
        spreadsheetId=backup_id,
        fields="sheets(properties(sheetId,title))",
    ).execute()
    initial_sheet_id = backup_meta["sheets"][0]["properties"]["sheetId"]
    temporary_title = f"__tmp_backup_{timestamp}__"
    controller.execute(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=backup_id,
            body={
                "requests": [
                    {
                        "updateSheetProperties": {
                            "properties": {"sheetId": initial_sheet_id, "title": temporary_title},
                            "fields": "title",
                        }
                    }
                ]
            },
        ),
        operation="temporary backup sheet rename",
    )

    copied_sheet_ids: list[int] = []
    rename_requests: list[dict[str, Any]] = []
    for sheet in source_meta.get("sheets", []):
        properties = sheet["properties"]
        copied = controller.execute(
            sheets_service.spreadsheets().sheets().copyTo(
                spreadsheetId=spreadsheet_id,
                sheetId=properties["sheetId"],
                body={"destinationSpreadsheetId": backup_id},
            ),
            operation=f"backup copy of {properties['title']!r}",
        )
        copied_sheet_ids.append(copied["sheetId"])
        rename_requests.append(
            {
                "updateSheetProperties": {
                    "properties": {"sheetId": copied["sheetId"], "title": properties["title"]},
                    "fields": "title",
                }
            }
        )
    if rename_requests:
        controller.execute(
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=backup_id,
                body={"requests": rename_requests},
            ),
            operation="backup sheet renames",
        )
    controller.execute(
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=backup_id,
            body={"requests": [{"deleteSheet": {"sheetId": initial_sheet_id}}]},
        ),
        operation="temporary backup sheet deletion",
    )
    return backup_id, backup_title


def resize_target_grid(
    sheets_service: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
    output_rows: int,
    controller: SheetsRequestController,
) -> dict[str, int]:
    properties = get_sheet_properties(
        sheets_service,
        spreadsheet_id=spreadsheet_id,
        sheet_name=sheet_name,
    )
    sheet_id = int(properties["sheetId"])
    grid = properties.get("gridProperties", {})
    current_rows = int(grid.get("rowCount", 0) or 0)
    current_columns = int(grid.get("columnCount", 0) or 0)
    required_rows = max(2, output_rows + 1)
    required_columns = len(TARGET_COLUMNS)
    requests: list[dict[str, Any]] = []

    if current_columns > required_columns:
        first_surplus = column_index_to_label(required_columns + 1)
        last_surplus = column_index_to_label(current_columns)
        surplus = sheets_service.spreadsheets().values().get(
            spreadsheetId=spreadsheet_id,
            range=f"{quote_sheet_title(sheet_name)}!{first_surplus}:{last_surplus}",
            valueRenderOption="FORMULA",
            majorDimension="ROWS",
        ).execute()
        if response_has_values(surplus) and not response_contains_only_managed_columns(
            surplus
        ):
            raise RuntimeError(
                f"Refusing to delete non-empty surplus columns in {sheet_name!r} "
                f"({first_surplus}:{last_surplus})"
            )
        if response_has_values(surplus):
            logging.info(
                "Deleting managed surplus columns in %r (%s:%s)",
                sheet_name,
                first_surplus,
                last_surplus,
            )
        requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "COLUMNS",
                        "startIndex": required_columns,
                        "endIndex": current_columns,
                    }
                }
            }
        )
    elif current_columns < required_columns:
        requests.append(
            {
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "COLUMNS",
                    "length": required_columns - current_columns,
                }
            }
        )
    if current_rows > required_rows:
        requests.append(
            {
                "deleteDimension": {
                    "range": {
                        "sheetId": sheet_id,
                        "dimension": "ROWS",
                        "startIndex": required_rows,
                        "endIndex": current_rows,
                    }
                }
            }
        )
    elif current_rows < required_rows:
        requests.append(
            {
                "appendDimension": {
                    "sheetId": sheet_id,
                    "dimension": "ROWS",
                    "length": required_rows - current_rows,
                }
            }
        )
    if requests:
        controller.execute(
            sheets_service.spreadsheets().batchUpdate(
                spreadsheetId=spreadsheet_id,
                body={"requests": requests},
            ),
            operation="target grid resize",
        )
    return {"rows": required_rows, "columns": required_columns}


def write_target(
    sheets_service: Any,
    *,
    spreadsheet_id: str,
    sheet_name: str,
    rows: list[list[Any]],
    chunk_size: int,
    controller: SheetsRequestController,
) -> int:
    quoted = quote_sheet_title(sheet_name)
    controller.execute(
        sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{quoted}!A1",
            valueInputOption="RAW",
            body={"values": [TARGET_COLUMNS]},
        ),
        operation="target header write",
    )
    controller.execute(
        sheets_service.spreadsheets().values().clear(
            spreadsheetId=spreadsheet_id,
            range=f"{quoted}!A2:{column_index_to_label(len(TARGET_COLUMNS))}",
            body={},
        ),
        operation="target data clear",
    )
    for start in range(0, len(rows), chunk_size):
        controller.execute(
            sheets_service.spreadsheets().values().update(
                spreadsheetId=spreadsheet_id,
                range=f"{quoted}!A{start + 2}",
                valueInputOption="RAW",
                body={"values": rows[start : start + chunk_size]},
            ),
            operation=f"target data write at row {start + 2}",
        )
    return len(rows)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export research projects with non-UdeA entities to Google Sheets."
    )
    parser.add_argument(
        "--source-spreadsheet-id",
        default=os.getenv("RESEARCH_PROJECTS_SOURCE_SPREADSHEET_ID", DEFAULT_SOURCE_SPREADSHEET_ID),
    )
    parser.add_argument(
        "--target-spreadsheet-id",
        default=os.getenv("RESEARCH_PROJECTS_TARGET_SPREADSHEET_ID", DEFAULT_TARGET_SPREADSHEET_ID),
    )
    parser.add_argument(
        "--source-sheet-name",
        default=os.getenv("RESEARCH_PROJECTS_SOURCE_SHEET_NAME", DEFAULT_SOURCE_SHEET_NAME),
    )
    parser.add_argument(
        "--target-sheet-name",
        default=os.getenv("RESEARCH_PROJECTS_TARGET_SHEET_NAME", DEFAULT_TARGET_SHEET_NAME),
    )
    parser.add_argument("--token-path", default=os.getenv("RESEARCH_PROJECTS_TOKEN_PATH", DEFAULT_TOKEN_PATH))
    parser.add_argument("--chunk-size", type=int, default=1000)
    parser.add_argument("--min-write-interval-seconds", type=float, default=1.2)
    parser.add_argument("--max-api-attempts", type=int, default=8)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--skip-backup", action="store_true")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    started = time.monotonic()
    if args.chunk_size < 1:
        raise ValueError("chunk-size must be at least one")
    if not str(args.source_spreadsheet_id).strip():
        raise ValueError("A source spreadsheet ID is required")
    if not str(args.target_spreadsheet_id).strip():
        raise ValueError("A target spreadsheet ID is required")
    if args.source_spreadsheet_id == args.target_spreadsheet_id:
        raise ValueError("Source and target spreadsheet IDs must be different")

    from googleapiclient.discovery import build

    credentials = load_credentials(Path(args.token_path).expanduser())
    service = build("sheets", "v4", credentials=credentials, cache_discovery=False)
    source_values = read_source_values(
        service,
        spreadsheet_id=args.source_spreadsheet_id,
        sheet_name=args.source_sheet_name,
    )
    rows, metrics = build_export_rows(source_values)
    logging.info("Prepared research project export: %s", metrics)

    result: dict[str, Any] = {
        "source_spreadsheet_id": args.source_spreadsheet_id,
        "target_spreadsheet_id": args.target_spreadsheet_id,
        "source_sheet_name": args.source_sheet_name,
        "target_sheet_name": args.target_sheet_name,
        "dry_run": args.dry_run,
        "metrics": metrics,
        "written_rows": 0,
        "backup_spreadsheet_id": None,
        "grid_shape": None,
    }
    if not args.dry_run:
        controller = SheetsRequestController(
            min_interval_seconds=args.min_write_interval_seconds,
            max_attempts=args.max_api_attempts,
        )
        if not args.skip_backup:
            backup_id, backup_title = backup_spreadsheet(
                service,
                spreadsheet_id=args.target_spreadsheet_id,
                controller=controller,
            )
            result["backup_spreadsheet_id"] = backup_id
            result["backup_title"] = backup_title
            logging.info("Backup created: %s", backup_id)
        result["grid_shape"] = resize_target_grid(
            service,
            spreadsheet_id=args.target_spreadsheet_id,
            sheet_name=args.target_sheet_name,
            output_rows=len(rows),
            controller=controller,
        )
        result["written_rows"] = write_target(
            service,
            spreadsheet_id=args.target_spreadsheet_id,
            sheet_name=args.target_sheet_name,
            rows=rows,
            chunk_size=args.chunk_size,
            controller=controller,
        )
    result["elapsed_seconds"] = round(time.monotonic() - started, 2)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
