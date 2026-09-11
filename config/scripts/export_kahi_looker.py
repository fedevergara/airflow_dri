#!/usr/bin/env python3
"""Export Kahi institution data to a Google Sheet for Looker Studio.

This script is intended to run on the remote server that can reach the Kahi
MongoDB locally. Airflow should orchestrate it over SSH instead of moving raw
MongoDB data through the Airflow worker.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import pickle
import random
import stat
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd


DEFAULT_INSTITUTION_ID = "03bp5hc83"
DEFAULT_DB_NAME = "kahi"
DEFAULT_SPREADSHEET_ID = ""
DEFAULT_TOKEN_PATH = "/srv/kahi_exports/secrets/token.pickle"

EXCLUDED_WORKS_AFFILIATION_TYPES = {"group", "faculty", "department"}
INSTITUTION_NAME_NORMALIZATION = {
    "University of Antioquia": "Universidad de Antioquia",
}
TARGET_INSTITUTION_NAMES = {
    "universidad de antioquia",
    "university of antioquia",
    "udea",
}
DASHBOARD_PRIMARY_SUBJECTS = {
    "Medicine",
    "Biology",
    "Chemistry",
    "Physics",
    "Computer science",
}
SHEET_COLUMNS = {
    "works": [
        "colav_id",
        "year_published",
        "author_name",
        "autor_id",
        "intitution",
        "country_code",
        "country_name",
    ],
    "subjects": ["colav_id", "subjects"],
    "affiliations": [
        "colav_id",
        "autor_id",
        "group_id",
        "group_name",
        "faculty_name",
        "department_anme",
    ],
    "dashboard_general": [
        "colav_id",
        "año",
        "cantidad_autores",
        "colaboración",
        "cantidad_países_extranjeros",
    ],
    "dashboard_paises": [
        "colav_id",
        "año",
        "código_país",
        "país",
        "productos",
    ],
    "dashboard_instituciones": [
        "colav_id",
        "año",
        "institución",
        "código_país",
        "país",
        "colaboración",
        "productos",
    ],
    "dashboard_temas": [
        "colav_id",
        "año",
        "tema",
        "categoría_tema",
        "nivel",
        "colaboración",
        "asignaciones",
    ],
    "dashboard_grupos": [
        "colav_id",
        "año",
        "group_id",
        "grupo",
        "facultad",
        "departamento",
        "unidad_académica",
        "colaboración",
        "productos",
    ],
}
TRANSIENT_HTTP_STATUS_CODES = {429, 500, 502, 503, 504}


class SheetsWriteController:
    """Throttle writes and retry transient Google Sheets API failures."""

    def __init__(
        self,
        *,
        min_interval_seconds: float = 1.2,
        max_attempts: int = 8,
        sleep_fn: Any = time.sleep,
        monotonic_fn: Any = time.monotonic,
        jitter_fn: Any = random.random,
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
            elapsed = now - self.last_write_started
            remaining = self.min_interval_seconds - elapsed
            if remaining > 0:
                self.sleep_fn(remaining)
                now = float(self.monotonic_fn())
        self.last_write_started = now

    @staticmethod
    def _retry_after_seconds(exc: Exception) -> float:
        response = getattr(exc, "resp", None)
        if response is None or not hasattr(response, "get"):
            return 0.0
        try:
            return max(0.0, float(response.get("retry-after", 0) or 0))
        except (TypeError, ValueError):
            return 0.0

    def execute(self, request: Any, *, operation: str) -> Any:
        for attempt in range(1, self.max_attempts + 1):
            self._throttle()
            try:
                return request.execute()
            except Exception as exc:
                status = getattr(getattr(exc, "resp", None), "status", None)
                if status not in TRANSIENT_HTTP_STATUS_CODES or attempt >= self.max_attempts:
                    raise

                exponential_delay = min(
                    (2 ** (attempt - 1)) + float(self.jitter_fn()),
                    64.0,
                )
                delay = max(self._retry_after_seconds(exc), exponential_delay)
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


def affiliation_country(affiliation: dict[str, Any]) -> tuple[str, str]:
    for address in affiliation.get("addresses", []) or []:
        country_code = address.get("country_code") or ""
        country_name = address.get("country") or ""
        if country_code or country_name:
            return str(country_code), str(country_name)
    return "", ""


def author_affiliation_ids(author: dict[str, Any]) -> list[Any]:
    return [
        affiliation.get("id")
        for affiliation in (author.get("affiliations") or [])
        if isinstance(affiliation, dict) and affiliation.get("id") is not None
    ]


def include_affiliation_in_works(affiliation: dict[str, Any]) -> bool:
    types = {
        item.get("type")
        for item in affiliation.get("types", [])
        if isinstance(item, dict) and item.get("type")
    }
    return not bool(types & EXCLUDED_WORKS_AFFILIATION_TYPES)


def normalize_name(text: Any) -> str:
    return " ".join(str(text or "").split())


def unique_normalized(values: list[Any]) -> list[str]:
    seen: set[str] = set()
    output: list[str] = []
    for value in values:
        normalized = normalize_name(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        output.append(normalized)
    return output


def normalize_institution_name(name: Any) -> str:
    normalized = normalize_name(name)
    return INSTITUTION_NAME_NORMALIZATION.get(normalized, normalized)


def is_foreign_country(country_code: Any, country_name: Any) -> bool:
    code = normalize_name(country_code).upper()
    name = normalize_name(country_name).casefold()
    if not code and not name:
        return False
    return code not in {"CO", "COL"} and name not in {
        "colombia",
        "república de colombia",
        "republica de colombia",
    }


def institution_collaboration(country_code: Any, country_name: Any) -> str:
    code = normalize_name(country_code).upper()
    name = normalize_name(country_name).casefold()
    if not code and not name:
        return "Sin país"
    return "Internacional" if is_foreign_country(code, name) else "Nacional"


def is_target_institution(
    affiliation: dict[str, Any], target_affiliation_ids: set[Any]
) -> bool:
    affiliation_id = affiliation.get("id")
    institution_name = normalize_name(affiliation.get("name", "")).casefold()
    return (
        affiliation_id in target_affiliation_ids
        or institution_name in TARGET_INSTITUTION_NAMES
    )


def dashboard_subject_category(subject_name: Any) -> str:
    normalized = normalize_name(subject_name)
    return normalized if normalized in DASHBOARD_PRIMARY_SUBJECTS else "Otros"


def effective_author_count(work: dict[str, Any], authors: list[dict[str, Any]]) -> int:
    raw_count = work.get("author_count")
    if not isinstance(raw_count, int) or raw_count <= 0:
        return len(authors)
    if authors and raw_count != len(authors):
        return len(authors)
    return raw_count


def build_sheet_data(
    *,
    db: Any,
    institution_id: str,
    limit_works: int | None,
    batch_size: int,
) -> dict[str, pd.DataFrame]:
    target_aff_ids = [
        doc["_id"]
        for doc in db["affiliations"].find(
            {"$or": [{"_id": institution_id}, {"relations.id": institution_id}]},
            {"_id": 1},
        )
    ]
    target_aff_set = set(target_aff_ids)
    logging.info("Target affiliations found: %s", len(target_aff_ids))

    works_query = {"authors.affiliations.id": {"$in": target_aff_ids}}
    works_projection = {
        "_id": 1,
        "author_count": 1,
        "year_published": 1,
        "authors.id": 1,
        "authors.full_name": 1,
        "authors.affiliations": 1,
        "subjects": 1,
    }

    rows_works: list[dict[str, Any]] = []
    rows_subjects: list[dict[str, Any]] = []
    rows_affiliations: list[dict[str, Any]] = []
    rows_dashboard_general: list[dict[str, Any]] = []
    rows_dashboard_countries: list[dict[str, Any]] = []
    rows_dashboard_institutions: list[dict[str, Any]] = []
    rows_dashboard_subjects: list[dict[str, Any]] = []
    rows_dashboard_groups: list[dict[str, Any]] = []
    seen_subjects: set[tuple[str, str]] = set()
    seen_dashboard_subjects: set[tuple[str, str]] = set()

    cursor = db["works"].find(works_query, works_projection).batch_size(batch_size)
    if limit_works:
        cursor = cursor.limit(int(limit_works))

    try:
        for work in cursor:
            authors = work.get("authors") or []
            if not authors:
                continue

            author_count_effective = effective_author_count(work, authors)
            if not 2 <= author_count_effective <= 20:
                continue

            has_target_author = False
            has_external_author = False
            author_has_target = []

            for author in authors:
                ids = author_affiliation_ids(author)
                has_target = any(aff_id in target_aff_set for aff_id in ids)
                has_other = any(aff_id not in target_aff_set for aff_id in ids)
                author_has_target.append(has_target)
                if has_target:
                    has_target_author = True
                if not has_target and has_other:
                    has_external_author = True

            if not (has_target_author and has_external_author):
                continue

            colav_id = str(work["_id"])
            year = work.get("year_published")
            foreign_countries: set[tuple[str, str]] = set()
            external_institutions: set[tuple[str, str, str, str]] = set()

            for author in authors:
                author_id = str(author.get("id", ""))
                author_name = author.get("full_name", "")
                affiliations = [
                    affiliation
                    for affiliation in (author.get("affiliations") or [])
                    if isinstance(affiliation, dict)
                    and include_affiliation_in_works(affiliation)
                ]

                if not affiliations:
                    rows_works.append(
                        {
                            "colav_id": colav_id,
                            "year_published": year,
                            "author_name": author_name,
                            "autor_id": author_id,
                            "intitution": "",
                            "country_code": "",
                            "country_name": "",
                        }
                    )
                    continue

                for affiliation in affiliations:
                    country_code, country_name = affiliation_country(affiliation)
                    institution_name = normalize_institution_name(
                        affiliation.get("name", "")
                    )
                    rows_works.append(
                        {
                            "colav_id": colav_id,
                            "year_published": year,
                            "author_name": author_name,
                            "autor_id": author_id,
                            "intitution": institution_name,
                            "country_code": country_code,
                            "country_name": country_name,
                        }
                    )
                    country_key = (
                        normalize_name(country_code).upper(),
                        normalize_name(country_name),
                    )
                    if is_foreign_country(*country_key):
                        foreign_countries.add(country_key)
                    if institution_name and not is_target_institution(
                        affiliation, target_aff_set
                    ):
                        external_institutions.add(
                            (
                                institution_name,
                                country_key[0],
                                country_key[1],
                                institution_collaboration(*country_key),
                            )
                        )

            collaboration = "Internacional" if foreign_countries else "Nacional"
            rows_dashboard_general.append(
                {
                    "colav_id": colav_id,
                    "año": year,
                    "cantidad_autores": author_count_effective,
                    "colaboración": collaboration,
                    "cantidad_países_extranjeros": len(foreign_countries),
                }
            )
            for country_code, country_name in sorted(foreign_countries):
                rows_dashboard_countries.append(
                    {
                        "colav_id": colav_id,
                        "año": year,
                        "código_país": country_code,
                        "país": country_name or country_code,
                        "productos": 1,
                    }
                )
            for institution_name, country_code, country_name, scope in sorted(
                external_institutions
            ):
                rows_dashboard_institutions.append(
                    {
                        "colav_id": colav_id,
                        "año": year,
                        "institución": institution_name,
                        "código_país": country_code,
                        "país": country_name or country_code,
                        "colaboración": scope,
                        "productos": 1,
                    }
                )

            for subject_block in work.get("subjects") or []:
                for subject in subject_block.get("subjects") or []:
                    subject_name = subject.get("name")
                    if not subject_name:
                        continue
                    key = (colav_id, str(subject_name))
                    if key not in seen_subjects:
                        seen_subjects.add(key)
                        rows_subjects.append(
                            {
                                "colav_id": colav_id,
                                "subjects": subject_name,
                            }
                        )
                    if (
                        str(subject.get("level", "")) == "0"
                        and key not in seen_dashboard_subjects
                    ):
                        seen_dashboard_subjects.add(key)
                        rows_dashboard_subjects.append(
                            {
                                "colav_id": colav_id,
                                "año": year,
                                "tema": subject_name,
                                "categoría_tema": dashboard_subject_category(
                                    subject_name
                                ),
                                "nivel": 0,
                                "colaboración": collaboration,
                                "asignaciones": 1,
                            }
                        )

            dashboard_group_rows: dict[
                tuple[str, str, str, str], dict[str, Any]
            ] = {}
            for author, has_target in zip(authors, author_has_target):
                if not has_target:
                    continue

                author_id = str(author.get("id", ""))
                affiliations = [
                    item
                    for item in (author.get("affiliations") or [])
                    if isinstance(item, dict)
                ]

                groups: list[tuple[str, str]] = []
                faculties: list[Any] = []
                departments: list[Any] = []

                for affiliation in affiliations:
                    aff_id = affiliation.get("id")
                    aff_name = affiliation.get("name", "")
                    types = {
                        item.get("type")
                        for item in affiliation.get("types", [])
                        if isinstance(item, dict)
                    }

                    if "group" in types:
                        groups.append((str(aff_id or ""), aff_name))
                    if "faculty" in types and aff_id in target_aff_set:
                        faculties.append(aff_name)
                    if "department" in types and aff_id in target_aff_set:
                        departments.append(aff_name)

                groups = list(dict.fromkeys(groups)) or [("", "")]
                faculties = unique_normalized(faculties) or [""]
                departments = unique_normalized(departments)
                department_name = departments[0] if departments else ""

                for faculty_name in faculties:
                    for group_id, group_name in groups:
                        affiliation_row = {
                            "colav_id": colav_id,
                            "autor_id": author_id,
                            "group_id": group_id,
                            "group_name": group_name,
                            "faculty_name": faculty_name,
                            "department_anme": department_name,
                        }
                        rows_affiliations.append(affiliation_row)
                        dashboard_key = (
                            group_id,
                            normalize_name(group_name),
                            normalize_name(faculty_name),
                            normalize_name(department_name),
                        )
                        dashboard_group_rows[dashboard_key] = {
                            "colav_id": colav_id,
                            "año": year,
                            "group_id": group_id,
                            "grupo": normalize_name(group_name),
                            "facultad": normalize_name(faculty_name),
                            "departamento": normalize_name(department_name),
                            "unidad_académica": normalize_name(faculty_name)
                            or normalize_name(department_name),
                            "colaboración": collaboration,
                            "productos": 1,
                        }
            rows_dashboard_groups.extend(dashboard_group_rows.values())
    finally:
        cursor.close()

    return {
        "works": pd.DataFrame(rows_works, columns=SHEET_COLUMNS["works"]),
        "subjects": pd.DataFrame(rows_subjects, columns=SHEET_COLUMNS["subjects"]),
        "affiliations": pd.DataFrame(
            rows_affiliations,
            columns=SHEET_COLUMNS["affiliations"],
        ),
        "dashboard_general": pd.DataFrame(
            rows_dashboard_general,
            columns=SHEET_COLUMNS["dashboard_general"],
        ),
        "dashboard_paises": pd.DataFrame(
            rows_dashboard_countries,
            columns=SHEET_COLUMNS["dashboard_paises"],
        ),
        "dashboard_instituciones": pd.DataFrame(
            rows_dashboard_institutions,
            columns=SHEET_COLUMNS["dashboard_instituciones"],
        ),
        "dashboard_temas": pd.DataFrame(
            rows_dashboard_subjects,
            columns=SHEET_COLUMNS["dashboard_temas"],
        ),
        "dashboard_grupos": pd.DataFrame(
            rows_dashboard_groups,
            columns=SHEET_COLUMNS["dashboard_grupos"],
        ),
    }


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
            "Google token is missing the Sheets scope: "
            "https://www.googleapis.com/auth/spreadsheets"
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


def ensure_sheets_exist(
    sheets_service: Any,
    spreadsheet_id: str,
    sheet_names: list[str],
) -> None:
    meta = sheets_service.spreadsheets().get(spreadsheetId=spreadsheet_id).execute()
    existing = {sheet["properties"]["title"] for sheet in meta.get("sheets", [])}

    add_requests = [
        {"addSheet": {"properties": {"title": sheet_name}}}
        for sheet_name in sheet_names
        if sheet_name not in existing
    ]
    if add_requests:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": add_requests},
        ).execute()


def column_index_to_label(column_index: int) -> str:
    """Convert a one-based column index to an A1 column label."""
    if column_index < 1:
        raise ValueError("column_index must be greater than zero")

    label = ""
    current = column_index
    while current:
        current, remainder = divmod(current - 1, 26)
        label = chr(65 + remainder) + label
    return label


def quote_sheet_title(sheet_name: str) -> str:
    return "'" + str(sheet_name).replace("'", "''") + "'"


def response_has_values(response: dict[str, Any]) -> bool:
    return any(
        str(cell or "").strip()
        for row in response.get("values", [])
        for cell in row
    )


def resize_sheet_grids(
    *,
    sheets_service: Any,
    spreadsheet_id: str,
    sheet_data: dict[str, pd.DataFrame],
) -> dict[str, dict[str, int]]:
    """Validate empty surplus columns and resize every generated sheet exactly."""
    meta = sheets_service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        fields="sheets(properties(sheetId,title,gridProperties(rowCount,columnCount)))",
    ).execute()
    properties_by_title = {
        sheet["properties"]["title"]: sheet["properties"]
        for sheet in meta.get("sheets", [])
    }

    requests: list[dict[str, Any]] = []
    shapes: dict[str, dict[str, int]] = {}
    for sheet_name, dataframe in sheet_data.items():
        properties = properties_by_title.get(sheet_name)
        if not properties:
            raise RuntimeError(f"Could not locate sheet '{sheet_name}' before resizing.")

        sheet_id = int(properties["sheetId"])
        grid = properties.get("gridProperties", {})
        current_rows = int(grid.get("rowCount", 0) or 0)
        current_columns = int(grid.get("columnCount", 0) or 0)
        required_rows = max(2, len(dataframe.index) + 1)
        required_columns = max(1, len(dataframe.columns))

        if current_columns > required_columns:
            first_surplus = column_index_to_label(required_columns + 1)
            last_surplus = column_index_to_label(current_columns)
            surplus_range = (
                f"{quote_sheet_title(sheet_name)}!{first_surplus}:{last_surplus}"
            )
            surplus_values = sheets_service.spreadsheets().values().get(
                spreadsheetId=spreadsheet_id,
                range=surplus_range,
                majorDimension="ROWS",
                valueRenderOption="FORMULA",
            ).execute()
            if response_has_values(surplus_values):
                raise RuntimeError(
                    f"Refusing to delete non-empty surplus columns in sheet '{sheet_name}' "
                    f"({first_surplus}:{last_surplus})."
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

        shapes[sheet_name] = {
            "rows": required_rows,
            "columns": required_columns,
        }

    if requests:
        sheets_service.spreadsheets().batchUpdate(
            spreadsheetId=spreadsheet_id,
            body={"requests": requests},
        ).execute()

    return shapes


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
    write_controller: SheetsWriteController | None = None,
) -> int:
    sheet_range = quote_sheet_title(sheet_name)
    last_column = column_index_to_label(max(1, len(dataframe.columns)))
    header_request = sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_range}!A1",
        valueInputOption="RAW",
        body={"values": [list(dataframe.columns)]},
    )
    if write_controller:
        write_controller.execute(header_request, operation=f"header write for '{sheet_name}'")
    else:
        header_request.execute()

    clear_request = sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"{sheet_range}!A2:{last_column}",
        body={},
    )
    if write_controller:
        write_controller.execute(clear_request, operation=f"clear for '{sheet_name}'")
    else:
        clear_request.execute()

    if dataframe.empty:
        return 0

    values = dataframe_values(dataframe)
    for start in range(0, len(values), chunk_size):
        chunk = values[start : start + chunk_size]
        start_row = start + 2
        chunk_request = sheets_service.spreadsheets().values().update(
            spreadsheetId=spreadsheet_id,
            range=f"{sheet_range}!A{start_row}",
            valueInputOption="RAW",
            body={"values": chunk},
        )
        if write_controller:
            write_controller.execute(
                chunk_request,
                operation=f"data write for '{sheet_name}' at row {start_row}",
            )
        else:
            chunk_request.execute()

    return len(values)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Export Kahi institution data into Google Sheets for Looker Studio."
    )
    parser.add_argument("--institution-id", default=DEFAULT_INSTITUTION_ID)
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    parser.add_argument("--db", default=os.getenv("KAHI_DB", DEFAULT_DB_NAME))
    parser.add_argument("--spreadsheet-id", default=os.getenv("KAHI_LOOKER_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID))
    parser.add_argument("--token-path", default=os.getenv("GOOGLE_SHEETS_TOKEN_PICKLE_PATH", DEFAULT_TOKEN_PATH))
    parser.add_argument("--limit-works", type=int, default=None)
    parser.add_argument("--batch-size", type=int, default=1000)
    parser.add_argument("--chunk-size", type=int, default=5000)
    parser.add_argument(
        "--min-write-interval-seconds",
        type=float,
        default=float(os.getenv("KAHI_LOOKER_MIN_WRITE_INTERVAL_SECONDS", "1.2")),
    )
    parser.add_argument(
        "--max-api-attempts",
        type=int,
        default=int(os.getenv("KAHI_LOOKER_MAX_API_ATTEMPTS", "8")),
    )
    parser.add_argument("--mongo-timeout-ms", type=int, default=10000)
    parser.add_argument("--skip-backup", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--log-level", default="INFO")
    return parser.parse_args()


def main() -> None:
    from googleapiclient.discovery import build
    from pymongo import MongoClient

    args = parse_args()
    logging.basicConfig(
        level=getattr(logging, str(args.log_level).upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    started = time.perf_counter()
    token_path = Path(args.token_path).expanduser()

    if not str(args.spreadsheet_id).strip():
        raise ValueError(
            "Missing spreadsheet id. Pass --spreadsheet-id or set KAHI_LOOKER_SPREADSHEET_ID."
        )

    client = MongoClient(args.mongo_uri, serverSelectionTimeoutMS=args.mongo_timeout_ms)
    try:
        client.admin.command("ping")
        logging.info("Connected to MongoDB database '%s'", args.db)
        sheet_data = build_sheet_data(
            db=client[args.db],
            institution_id=args.institution_id,
            limit_works=args.limit_works,
            batch_size=args.batch_size,
        )
    finally:
        client.close()

    counts = {sheet_name: int(dataframe.shape[0]) for sheet_name, dataframe in sheet_data.items()}
    logging.info("Prepared rows: %s", counts)

    summary: dict[str, Any] = {
        "institution_id": args.institution_id,
        "database": args.db,
        "spreadsheet_id_set": bool(str(args.spreadsheet_id).strip()),
        "rows": counts,
        "dry_run": bool(args.dry_run),
    }

    if not args.dry_run:
        creds = load_credentials(token_path)
        sheets_service = build("sheets", "v4", credentials=creds, cache_discovery=False)

        if args.skip_backup:
            summary["backup_spreadsheet_id"] = None
            summary["backup_title"] = None
            logging.warning("Skipping spreadsheet backup by request")
        else:
            backup_id, backup_title = backup_spreadsheet(sheets_service, args.spreadsheet_id)
            summary["backup_spreadsheet_id"] = backup_id
            summary["backup_title"] = backup_title
            logging.info("Backup created: %s", backup_id)

        ensure_sheets_exist(sheets_service, args.spreadsheet_id, list(sheet_data))
        grid_shapes = resize_sheet_grids(
            sheets_service=sheets_service,
            spreadsheet_id=args.spreadsheet_id,
            sheet_data=sheet_data,
        )
        summary["grid_shapes"] = grid_shapes
        logging.info("Sheet grids resized: %s", grid_shapes)
        write_controller = SheetsWriteController(
            min_interval_seconds=args.min_write_interval_seconds,
            max_attempts=args.max_api_attempts,
        )
        written: dict[str, int] = {}
        for sheet_name, dataframe in sheet_data.items():
            written[sheet_name] = write_sheet_dataframe(
                sheets_service=sheets_service,
                spreadsheet_id=args.spreadsheet_id,
                sheet_name=sheet_name,
                dataframe=dataframe,
                chunk_size=args.chunk_size,
                write_controller=write_controller,
            )
            logging.info("Sheet '%s' written with %s rows", sheet_name, written[sheet_name])
        summary["written_rows"] = written
    else:
        logging.info("Dry run enabled; Google Sheet was not modified")

    elapsed_seconds = time.perf_counter() - started
    summary["elapsed_seconds"] = round(elapsed_seconds, 2)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
