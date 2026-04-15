"""Capture international and national mobilities from Google Sheets into MongoDB."""

from airflow.models.dag import DAG  # noqa: F401 - Helps DAG safe discovery.
from capture.capture_pipeline import build_capture_dag

MOBILITIES_SOURCES = [
    {
        "source_name": "mobilities_international_entrante",
        "spreadsheet_id_var": "GSHEET_MOBILITIES_INTERNATIONAL_ID",
        "range_var": "SHEET_ENTRANTE",
        "default_range": "Movilidad Entrante",
        "collection": "mobilities",
    },
    {
        "source_name": "mobilities_international_saliente",
        "spreadsheet_id_var": "GSHEET_MOBILITIES_INTERNATIONAL_ID",
        "range_var": "SHEET_SALIENTE",
        "default_range": "Movilidad Saliente",
        "collection": "mobilities",
    },
    {
        "source_name": "mobilities_national_entrante",
        "spreadsheet_id_var": "GSHEET_MOBILITIES_NATIONAL_ID",
        "range_var": "SHEET_ENTRANTE",
        "default_range": "Movilidad Entrante",
        "collection": "mobilities",
    },
    {
        "source_name": "mobilities_national_saliente",
        "spreadsheet_id_var": "GSHEET_MOBILITIES_NATIONAL_ID",
        "range_var": "SHEET_SALIENTE",
        "default_range": "Movilidad Saliente",
        "collection": "mobilities",
    },
]


dag = build_capture_dag(
    dag_id="capture_mobilities",
    description="Capture mobilities (international + national) from Google Sheets to MongoDB.",
    source_type="mobilities",
    sources=MOBILITIES_SOURCES,
)
