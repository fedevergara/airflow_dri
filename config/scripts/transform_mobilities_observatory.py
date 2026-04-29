#!/usr/bin/env python3
"""Transform raw mobility records from MongoDB and publish them to Google Sheets."""

from __future__ import annotations

import argparse
import json
import logging
import os
import pickle
import stat
import time
import unicodedata
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pandas as pd
from google.auth.exceptions import RefreshError
from google.auth.transport.requests import Request
from googleapiclient.discovery import build
from pymongo import MongoClient


DEFAULT_DB_NAME = "international"
DEFAULT_COLLECTION = "mobilities"
DEFAULT_SPREADSHEET_ID = ""
DEFAULT_SHEET_NAME = "movilidades"
DEFAULT_TOKEN_PATH = "/srv/mobilities_transform/secrets/token.pickle"
MIN_YEAR = 2015

YEAR_COL = "año"
TARGET_COLUMNS = [
    "año",
    "semestre",
    "modalidad",
    "ambito",
    "vía",
    "categoría",
    "país/ciudad",
    "unidad_académica_administrativa",
    "programa_académico",
    "objetivo_de_la_movilidad",
    "categoría_estudiantes",
    "institución/entidad",
    "movilidad_por_convenio",
    "código_convenio",
    "campus",
    "country/city",
]

COMMON_CANDIDATES = {
    "ano": ["ANO", "ANIO", "AÑO"],
    "semestre": ["SEMESTRE"],
    "modalidad": ["MODALIDAD"],
    "categoria": ["CATEGORIA", "CATEGORÍA"],
    "fecha_inicio": ["FECHA DE INICIO DE LABORES", "FECHA DE INICIO"],
    "fecha_fin": ["FECHA DE TERMINACION DE LABORES", "FECHA DE TERMINACION"],
    "identificacion": ["NUMERO DE IDENTIFICACION", "NÚMERO DE IDENTIFICACIÓN"],
    "unidad_academica": [
        "FACULTAD/ESCUELA/INSTITUTO DONDE HARA LA MOVILIDAD",
        "FACULTAD/ESCUELA/INSTITUTO/CORPORACION",
        "FACULTAD/ESCUELA/INSTITUTO/CORPORACIÓN",
    ],
    "programa": [
        "PROGRAMA ACADEMICO DONDE HARA LA MOVILIDAD",
        "PROGRAMA ACADEMICO AL QUE PERTENECE",
        "PROGRAMA ACADÉMICO DONDE HARA LA MOVILIDAD",
        "PROGRAMA ACADÉMICO AL QUE PERTENECE",
    ],
    "codigo_programa_udea": [
        "CÓDIGO UDEA PROGRAMA",
        "CODIGO UDEA PROGRAMA",
        "CÓDIGO UDEA\n(PROGRAMA)",
        "CODIGO UDEA\n(PROGRAMA)",
    ],
    "codigo_unidad_udea": [
        "CÓDIGO UDEA UA",
        "CODIGO UDEA UA",
        "CÓDIGO UDEA",
        "CODIGO UDEA",
    ],
    "tipo_movilidad": ["TIPO DE MOVILIDAD"],
    "categoria_estudiante": [
        "CATEGORIA SOLO EN CASO DE ESTUDIANTE",
        "CATEGORIA EN CASO DE SER ESTUDIANTE",
        "CATEGORÍA SOLO EN CASO DE ESTUDIANTE",
        "CATEGORÍA EN CASO DE SER ESTUDIANTE",
    ],
    "institucion": [
        "INSTITUCION EXTRANJERA DE ORIGEN",
        "INSTITUCION EXTRANJERA DE DESTINO",
        "INSTITUCION DE ORIGEN",
        "INSTITUCION DE DESTINO",
        "INSTITUCIÓN EXTRANJERA DE ORIGEN",
        "INSTITUCIÓN EXTRANJERA DE DESTINO",
        "INSTITUCIÓN DE ORIGEN",
        "INSTITUCIÓN DE DESTINO",
    ],
    "movilidad_por_convenio": [
        "MOVILIDAD POR CONVENIO",
        "CONVENIO",
    ],
    "codigo_convenio": [
        "CODIGO_CONVENIO",
        "CODIGO CONVENIO",
        "CÓDIGO_CONVENIO",
        "CÓDIGO CONVENIO",
    ],
    "campus": ["SEDE", "SEDE ORIGEN", "SEDE DESTINO", "SEDE DE ORIGEN"],
    "pais": [
        "PAIS DE PROCEDENCIA",
        "PAIS EXTRANJERO DE DESTINO",
        "PAÍS DE PROCEDENCIA",
        "PAÍS EXTRANJERO DE DESTINO",
    ],
    "ciudad": ["CIUDADES DE ORIGEN", "CIUDAD DE DESTINO"],
    "via": ["vía", "via", "VÍA", "VIA"],
    "ambito": ["ambito", "ámbito", "AMBITO", "ÁMBITO"],
}

TIPO_MOVILIDAD_MAPPING = {
    "comision de servicios": "Misión",
    "semestre academico de intercambio": "Semestre académico de Intercambio",
    "curso corto": "Curso corto",
    "rotacion medica": "Rotación médica",
    "pasantia": "Pasantía o práctica",
    "estancia de investigacion": "Estancia de Investigación",
    "profesor programa de maestria": "Profesor Programa Maestría",
    "profesor visitante": "Profesor Visitante",
    "estudios de maestria": "Estudios de Maestría",
    "asistencia a eventos": "Asistencia a eventos",
    "pasantia de investigacion": "Estancia de Investigación",
    "estudios de doctorado": "Estudios de Doctorado",
    "profesor programa de pregrado": "Profesor Programa Pregrado",
    "estudios de posdoctorado": "Estudios de Posdoctorado",
    "estudios de especializacion": "Estudios de Especialización",
    "doble titulacion": "Doble Titulación",
    "profesor programa de doctorado": "Profesor programa Doctorado",
}

CAMPUS_MAPPING = {
    "sede central (medellin)": "Campus Medellín",
    "campus medellin": "Campus Medellín",
    "occidente (santa fe de antioquia)": "Campus Santa Fe de Antioquia",
    "campus santa fe de antioquia": "Campus Santa Fe de Antioquia",
    "sonson": "Campus Sonsón",
    "campus sonson": "Campus Sonsón",
    "suroeste (andes)": "Campus Andes",
    "campus andes": "Campus Andes",
    "oriente (el carmen de viboral)": "Campus El Carmen de Viboral",
    "campus el carmen de viboral": "Campus El Carmen de Viboral",
    "amalfi": "Campus Amalfi",
    "campus amalfi": "Campus Amalfi",
    "uraba (apartado, turbo y carepa)": "Campus Urabá",
    "campus uraba": "Campus Urabá",
    "region": "Campus Medellín",
    "norte (yarumal)": "Campus Yarumal",
    "campus yarumal": "Campus Yarumal",
    "no aplica": "No Aplica",
    "campus caucasia": "Campus Caucasia",
    "campus magdalena medio": "Campus Magdalena Medio",
    "campus carepa": "Campus Carepa",
    "campus apartado": "Campus Apartadó",
    "campus segovia": "Campus Segovia",
    "campus puerto berrio": "Campus Puerto Berrío",
}

PLACEHOLDER_NAMES = {"", "no aplica", "no informa", "n/a", "na"}

PAIS_ES_MAPPING = {
    "estados unidos de america": "Estados Unidos",
    "corea": "Corea del Sur",
    "republica checa": "República Checa",
    "mexico": "México",
    "belgica": "Bélgica",
    "paises bajos": "Países Bajos",
    "benin": "Benín",
    "escocia": "Reino Unido",
}

PAIS_EN_MAPPING = {
    "alemania": "Germany",
    "argentina": "Argentina",
    "australia": "Australia",
    "austria": "Austria",
    "belgica": "Belgium",
    "benin": "Benin",
    "bolivia": "Bolivia",
    "brasil": "Brazil",
    "canada": "Canada",
    "chile": "Chile",
    "china": "China",
    "colombia": "Colombia",
    "corea del sur": "South Korea",
    "costa rica": "Costa Rica",
    "cuba": "Cuba",
    "ecuador": "Ecuador",
    "el salvador": "El Salvador",
    "espana": "Spain",
    "estados unidos": "United States",
    "francia": "France",
    "india": "India",
    "italia": "Italy",
    "japon": "Japan",
    "mexico": "Mexico",
    "paises bajos": "Netherlands",
    "panama": "Panama",
    "paraguay": "Paraguay",
    "peru": "Peru",
    "portugal": "Portugal",
    "reino unido": "United Kingdom",
    "republica checa": "Czech Republic",
    "suiza": "Switzerland",
    "suecia": "Sweden",
    "uruguay": "Uruguay",
    "venezuela": "Venezuela",
}


def normalize_text(value: Any) -> str:
    text = "" if value is None else str(value).strip()
    text = unicodedata.normalize("NFKD", text)
    text = "".join(char for char in text if not unicodedata.combining(char))
    return " ".join(text.lower().split())


def clean_missing(value: Any) -> str:
    text = "" if value is None else str(value).replace("\xa0", " ").strip()
    text = " ".join(text.split())
    return "" if normalize_text(text) in {"", "no informa", "n", "nan", "none"} else text


def titlecase_spanish(value: Any) -> str:
    text = clean_missing(value)
    if not text:
        return ""
    out = text.lower().title()
    return (
        out.replace(" De ", " de ")
        .replace(" Del ", " del ")
        .replace(" Y ", " y ")
        .replace(" La ", " la ")
    )


def pick_column(df_raw: pd.DataFrame, candidates: list[str]) -> pd.Series:
    normalized_lookup: dict[str, list[str]] = {}
    for col in df_raw.columns:
        normalized_lookup.setdefault(normalize_text(col), []).append(col)

    result = pd.Series([""] * len(df_raw), index=df_raw.index, dtype="object")
    for candidate in candidates:
        for source_col in normalized_lookup.get(normalize_text(candidate), []):
            source_values = df_raw[source_col].fillna("").astype(str).str.strip()
            fill_mask = result.eq("") & source_values.ne("")
            result = result.where(~fill_mask, source_values)
    return result


def map_categoria(value: Any) -> str:
    text = clean_missing(value)
    norm = normalize_text(text)
    if not norm:
        return ""
    if norm.startswith("administrativo"):
        return "Administrativo"
    if norm.startswith("docente") or norm.startswith("profesor"):
        return "Profesor"
    if norm.startswith("estudiante"):
        return "Estudiante"
    return titlecase_spanish(text)


def map_tipo_movilidad(value: Any) -> str:
    text = clean_missing(value)
    norm = normalize_text(text)
    if not norm:
        return ""
    return TIPO_MOVILIDAD_MAPPING.get(norm, titlecase_spanish(text))


def map_categoria_estudiante(value: Any, categoria: str) -> str:
    if categoria != "Estudiante":
        return ""

    text = clean_missing(value)
    norm = normalize_text(text)
    if not norm:
        return ""
    if norm in {"pregrado", "undergraduate"}:
        return "Pregrado"
    if norm in {"posgrado", "postgrado", "maestria", "doctorado"}:
        return "Posgrado"
    if norm in {"posgrafo", "extension (egresado)", "extension egresado", "extensión (egresado)"}:
        return "Posgrado"
    return titlecase_spanish(text)


def map_yes_no(value: Any) -> str:
    norm = normalize_text(value)
    if norm in {"si", "s", "yes"}:
        return "Sí"
    if norm in {"no", "n", ""}:
        return "No"
    return clean_missing(value)


def map_movilidad_por_convenio(value: Any, ambito: str) -> str:
    norm = normalize_text(value)
    if ambito == "Nacional":
        return "No" if norm in {"", "n", "no", "none"} else "Sí"
    return map_yes_no(value)


def map_modalidad(value: Any, ambito: str) -> str:
    norm = normalize_text(value)
    if norm == "virtual":
        return "Virtual"
    if norm in {"presencial", "mixta"}:
        return "Presencial"
    if not norm and ambito == "Nacional":
        return "Presencial"
    return titlecase_spanish(value)


def parse_year(value: Any, fecha_inicio: Any) -> str:
    text = clean_missing(value)
    if text:
        digits = "".join(char for char in text if char.isdigit())
        if len(digits) >= 4:
            return digits[:4]

    date_text = clean_missing(fecha_inicio)
    if not date_text:
        return ""
    parsed = pd.to_datetime(date_text, errors="coerce", dayfirst=True)
    if pd.isna(parsed):
        return ""
    return str(parsed.year)


def parse_semester(value: Any, fecha_inicio: Any, ambito: str) -> str:
    norm = normalize_text(value)
    if norm in {"1", "i", "primero", "primer", "semestre 1"}:
        return "1"
    if norm in {"2", "ii", "segundo", "semestre 2"}:
        return "2"

    date_text = clean_missing(fecha_inicio)
    if date_text:
        parsed = pd.to_datetime(date_text, errors="coerce", dayfirst=True)
        if not pd.isna(parsed):
            return "1" if int(parsed.month) <= 6 else "2"

    if ambito == "Internacional" and not norm:
        return "1"
    return ""


def derive_ambito_from_source(source: Any) -> str:
    norm = normalize_text(source)
    if "internacional" in norm or "international" in norm:
        return "Internacional"
    if "nacional" in norm or "national" in norm:
        return "Nacional"
    return ""


def derive_via_from_source(source: Any) -> str:
    norm = normalize_text(source)
    if "entrante" in norm or "incoming" in norm:
        return "Entrante"
    if "saliente" in norm or "outgoing" in norm:
        return "Saliente"
    return ""


def map_ambito(value: Any, source: Any = "") -> str:
    norm = normalize_text(value)
    if "internacional" in norm or "international" in norm:
        return "Internacional"
    if "nacional" in norm or "national" in norm:
        return "Nacional"
    derived = derive_ambito_from_source(source)
    if derived:
        return derived
    return titlecase_spanish(value)


def map_via(value: Any, source: Any = "") -> str:
    norm = normalize_text(value)
    if "entrante" in norm or "incoming" in norm:
        return "Entrante"
    if "saliente" in norm or "outgoing" in norm:
        return "Saliente"
    derived = derive_via_from_source(source)
    if derived:
        return derived
    return titlecase_spanish(value)


def map_pais_es(value: Any, ambito: str) -> str:
    text = clean_missing(value)
    norm = normalize_text(text)
    if not norm:
        return "Colombia" if ambito == "Nacional" else ""
    if "otros paises o territorios" in norm:
        return "No Informa"
    mapped = PAIS_ES_MAPPING.get(norm)
    if mapped:
        return mapped
    return titlecase_spanish(text)


def map_pais_en(pais_es: str) -> str:
    norm = normalize_text(pais_es)
    if not norm:
        return ""
    if norm == "no informa":
        return "No Informa"
    return PAIS_EN_MAPPING.get(norm, pais_es)


def map_campus_token(value: Any) -> str:
    text = clean_missing(value)
    norm = normalize_text(text)
    if norm in CAMPUS_MAPPING:
        return CAMPUS_MAPPING[norm]
    return ""


def map_campus(value: Any) -> str:
    text = clean_missing(value)
    if not text:
        return "Campus Medellín"

    separators = [",", ";", "|"]
    compound = text
    for separator in separators:
        compound = compound.replace(separator, ",")
    parts = [part.strip() for part in compound.split(",") if part.strip()]
    expanded_parts: list[str] = []
    for part in parts or [text]:
        if " y " in normalize_text(part):
            expanded_parts.extend(item.strip() for item in part.split(" y ") if item.strip())
        else:
            expanded_parts.append(part)

    resolved: list[str] = []
    unknown_found = False
    for part in expanded_parts:
        mapped = map_campus_token(part)
        if mapped:
            resolved.append(mapped)
        else:
            unknown_found = True

    if unknown_found:
        return "Campus Medellín"

    unique_resolved = list(dict.fromkeys(resolved))
    non_medellin = [
        campus_name
        for campus_name in unique_resolved
        if campus_name not in {"Campus Medellín", "No Aplica"}
    ]
    if non_medellin:
        return non_medellin[0]
    if unique_resolved:
        return unique_resolved[0]
    return "Campus Medellín"


def build_location_es(ambito: str, pais_es: str, ciudad: str) -> str:
    if ambito == "Nacional":
        return clean_missing(ciudad)
    return clean_missing(pais_es)


def build_location_en(ambito: str, country_en: str, ciudad: str) -> str:
    if ambito == "Nacional":
        return clean_missing(ciudad)
    return clean_missing(country_en)


def build_code_dictionary(codes: pd.Series, labels: pd.Series) -> dict[str, str]:
    counts: dict[str, dict[str, int]] = {}
    display_by_norm: dict[tuple[str, str], str] = {}

    for code_value, label_value in zip(codes, labels):
        code = clean_missing(code_value)
        label = clean_missing(label_value)
        if not code or not label:
            continue

        norm_label = normalize_text(label)
        if norm_label in PLACEHOLDER_NAMES:
            continue

        counts.setdefault(code, {})
        counts[code][norm_label] = counts[code].get(norm_label, 0) + 1
        display_by_norm.setdefault((code, norm_label), label)

    dictionary: dict[str, str] = {}
    for code, label_counts in counts.items():
        if not label_counts:
            continue
        best_norm = sorted(
            label_counts.items(),
            key=lambda item: (-item[1], item[0]),
        )[0][0]
        dictionary[code] = display_by_norm[(code, best_norm)]
    return dictionary


def normalize_name_by_code(value: Any, code: Any, dictionary: dict[str, str]) -> str:
    cleaned_value = clean_missing(value)
    cleaned_code = clean_missing(code)
    if cleaned_code and cleaned_code in dictionary:
        return dictionary[cleaned_code]
    return cleaned_value


def join_country_city(country: str, city: str) -> str:
    left = "" if country is None else str(country).strip()
    right = clean_missing(city)
    if left and right:
        return f"{left}/{right}"
    return left or right


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
        "row_number": 1,
        "raw_payload": 1,
        "vía": 1,
        "ambito": 1,
        "first_extracted_at": 1,
        "last_extracted_at": 1,
    }
    cursor = db[collection].find({"source_type": source_type}, projection).batch_size(batch_size)
    try:
        for document in cursor:
            payload = dict(document.get("raw_payload") or {})
            payload["__mongo_id"] = str(document.get("_id", ""))
            payload["__source"] = str(document.get("source", ""))
            payload["__row_number"] = str(document.get("row_number", ""))
            payload["__ambito"] = str(document.get("ambito") or payload.get("ambito") or "")
            payload["__via"] = str(
                document.get("vía") or payload.get("vía") or payload.get("via") or ""
            )
            payload["__first_extracted_at"] = str(document.get("first_extracted_at", ""))
            payload["__last_extracted_at"] = str(document.get("last_extracted_at", ""))
            rows.append(payload)
    finally:
        cursor.close()

    return pd.DataFrame(rows)


def build_standard_dataframe(df_raw: pd.DataFrame) -> pd.DataFrame:
    if df_raw.empty:
        return pd.DataFrame(columns=TARGET_COLUMNS)

    data = {
        column_name: pick_column(df_raw, candidates)
        for column_name, candidates in COMMON_CANDIDATES.items()
    }
    df = pd.DataFrame(data)
    df["ambito"] = df_raw.get("__ambito", pd.Series([""] * len(df_raw), index=df_raw.index))
    df["via"] = df_raw.get("__via", pd.Series([""] * len(df_raw), index=df_raw.index))
    df["source"] = df_raw.get("__source", pd.Series([""] * len(df_raw), index=df_raw.index))

    for column_name in df.columns:
        df[column_name] = df[column_name].fillna("").astype(str).str.strip()

    df["ambito"] = [
        map_ambito(ambito, source)
        for ambito, source in zip(df["ambito"], df["source"])
    ]
    df["vía"] = [
        map_via(via, source)
        for via, source in zip(df["via"], df["source"])
    ]
    df["categoría"] = df["categoria"].apply(map_categoria)
    df["modalidad"] = [
        map_modalidad(modalidad, ambito)
        for modalidad, ambito in zip(df["modalidad"], df["ambito"])
    ]
    df["objetivo_de_la_movilidad"] = df["tipo_movilidad"].apply(map_tipo_movilidad)
    df["categoría_estudiantes"] = [
        map_categoria_estudiante(cat_est, categoria)
        for cat_est, categoria in zip(df["categoria_estudiante"], df["categoría"])
    ]
    unidad_dictionary = build_code_dictionary(df["codigo_unidad_udea"], df["unidad_academica"])
    programa_dictionary = build_code_dictionary(df["codigo_programa_udea"], df["programa"])
    df["movilidad_por_convenio"] = [
        map_movilidad_por_convenio(value, ambito)
        for value, ambito in zip(df["movilidad_por_convenio"], df["ambito"])
    ]
    df["código_convenio"] = df["codigo_convenio"].apply(
        lambda value: "" if clean_missing(value) == "0" else clean_missing(value)
    )
    df["campus"] = df["campus"].apply(map_campus)
    df["unidad_académica_administrativa"] = [
        normalize_name_by_code(value, code, unidad_dictionary)
        for value, code in zip(df["unidad_academica"], df["codigo_unidad_udea"])
    ]
    df["programa_académico"] = [
        normalize_name_by_code(value, code, programa_dictionary)
        for value, code in zip(df["programa"], df["codigo_programa_udea"])
    ]
    df["institución/entidad"] = df["institucion"].apply(clean_missing)
    df["país_es"] = [
        map_pais_es(pais, ambito)
        for pais, ambito in zip(df["pais"], df["ambito"])
    ]
    df["country_en"] = df["país_es"].apply(map_pais_en)
    df["ciudad"] = df["ciudad"].apply(clean_missing)
    df["país/ciudad"] = [
        build_location_es(ambito, country, city)
        for ambito, country, city in zip(df["ambito"], df["país_es"], df["ciudad"])
    ]
    df["country/city"] = [
        build_location_en(ambito, country, city)
        for ambito, country, city in zip(df["ambito"], df["country_en"], df["ciudad"])
    ]
    df["año"] = [
        parse_year(year, fecha_inicio)
        for year, fecha_inicio in zip(df["ano"], df["fecha_inicio"])
    ]
    df["semestre"] = [
        parse_semester(semestre, fecha_inicio, ambito)
        for semestre, fecha_inicio, ambito in zip(
            df["semestre"], df["fecha_inicio"], df["ambito"]
        )
    ]

    result = df[TARGET_COLUMNS].copy()
    for column_name in TARGET_COLUMNS:
        result[column_name] = result[column_name].fillna("").astype(str).str.strip()

    year_numeric = pd.to_numeric(result["año"], errors="coerce")
    result = result.loc[year_numeric.ge(MIN_YEAR)].copy()

    result = result.sort_values(
        by=["año", "semestre", "ambito", "vía", "categoría", "país/ciudad"],
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
        description="Transform raw mobility records and publish them to Google Sheets."
    )
    parser.add_argument("--mongo-uri", default=os.getenv("MONGO_URI", "mongodb://localhost:27017/"))
    parser.add_argument("--db", default=os.getenv("MOBILITIES_RAW_DB", DEFAULT_DB_NAME))
    parser.add_argument(
        "--collection",
        default=os.getenv("MOBILITIES_RAW_COLLECTION", DEFAULT_COLLECTION),
    )
    parser.add_argument(
        "--source-type",
        default=os.getenv("MOBILITIES_RAW_SOURCE_TYPE", "mobilities"),
    )
    parser.add_argument(
        "--spreadsheet-id",
        default=os.getenv("MOBILITIES_TRANSFORM_SPREADSHEET_ID", DEFAULT_SPREADSHEET_ID),
    )
    parser.add_argument(
        "--sheet-name",
        default=os.getenv("MOBILITIES_TRANSFORM_SHEET_NAME", DEFAULT_SHEET_NAME),
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
            "Missing spreadsheet id. Pass --spreadsheet-id or set MOBILITIES_TRANSFORM_SPREADSHEET_ID."
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

    transformed = build_standard_dataframe(df_raw)
    summary: dict[str, Any] = {
        "db": args.db,
        "collection": args.collection,
        "source_type": args.source_type,
        "raw_rows": int(len(df_raw.index)),
        "transformed_rows": int(len(transformed.index)),
        "target_sheet": args.sheet_name,
        "spreadsheet_id": args.spreadsheet_id,
    }

    if args.dry_run:
        summary["preview"] = transformed.head(10).astype(str).to_dict(orient="records")
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
        dataframe=transformed,
        chunk_size=args.chunk_size,
    )
    summary["rows_written"] = rows_written
    summary["elapsed_seconds"] = round(time.perf_counter() - started, 2)
    print(json.dumps(summary, ensure_ascii=False, sort_keys=True))


if __name__ == "__main__":
    main()
