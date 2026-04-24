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
import stat
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from pymongo import MongoClient


DEFAULT_INSTITUTION_ID = "03bp5hc83"
DEFAULT_DB_NAME = "kahi"
DEFAULT_SPREADSHEET_ID = ""
DEFAULT_TOKEN_PATH = "/srv/kahi_exports/secrets/token.pickle"

EXCLUDED_WORKS_AFFILIATION_TYPES = {"group", "faculty", "department"}
INSTITUTION_NAME_NORMALIZATION = {
    "University of Antioquia": "Universidad de Antioquia",
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
}


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
    seen_subjects: set[tuple[str, str]] = set()

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
                    rows_works.append(
                        {
                            "colav_id": colav_id,
                            "year_published": year,
                            "author_name": author_name,
                            "autor_id": author_id,
                            "intitution": normalize_institution_name(
                                affiliation.get("name", "")
                            ),
                            "country_code": country_code,
                            "country_name": country_name,
                        }
                    )

            for subject_block in work.get("subjects") or []:
                for subject in subject_block.get("subjects") or []:
                    subject_name = subject.get("name")
                    if not subject_name:
                        continue
                    key = (colav_id, str(subject_name))
                    if key in seen_subjects:
                        continue
                    seen_subjects.add(key)
                    rows_subjects.append(
                        {
                            "colav_id": colav_id,
                            "subjects": subject_name,
                        }
                    )

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
                        rows_affiliations.append(
                            {
                                "colav_id": colav_id,
                                "autor_id": author_id,
                                "group_id": group_id,
                                "group_name": group_name,
                                "faculty_name": faculty_name,
                                "department_anme": department_name,
                            }
                        )
    finally:
        cursor.close()

    return {
        "works": pd.DataFrame(rows_works, columns=SHEET_COLUMNS["works"]),
        "subjects": pd.DataFrame(rows_subjects, columns=SHEET_COLUMNS["subjects"]),
        "affiliations": pd.DataFrame(
            rows_affiliations,
            columns=SHEET_COLUMNS["affiliations"],
        ),
    }


def load_credentials(token_path: Path) -> Any:
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
    sheets_service.spreadsheets().values().update(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A1",
        valueInputOption="RAW",
        body={"values": [list(dataframe.columns)]},
    ).execute()

    sheets_service.spreadsheets().values().clear(
        spreadsheetId=spreadsheet_id,
        range=f"'{sheet_name}'!A2:ZZ",
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
        written: dict[str, int] = {}
        for sheet_name, dataframe in sheet_data.items():
            written[sheet_name] = write_sheet_dataframe(
                sheets_service=sheets_service,
                spreadsheet_id=args.spreadsheet_id,
                sheet_name=sheet_name,
                dataframe=dataframe,
                chunk_size=args.chunk_size,
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
