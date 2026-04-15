"""Capture mobilities sources from Google Sheets into MongoDB raw collections."""

from airflow.models.dag import DAG  # noqa: F401 - Helps DAG safe discovery.
from capture_pipeline import build_capture_dag

MOBILITIES_SOURCES = [
    {
        "source_name": "mobilities_international",
        "spreadsheet_id_var": "GSHEET_MOBILITIES_INTERNATIONAL_ID",
        "range_var": "GSHEET_MOBILITIES_INTERNATIONAL_RANGE",
        "default_range": "A:Z",
        "collection": "mobilities_international_raw",
    },
    {
        "source_name": "mobilities_national",
        "spreadsheet_id_var": "GSHEET_MOBILITIES_NATIONAL_ID",
        "range_var": "GSHEET_MOBILITIES_NATIONAL_RANGE",
        "default_range": "A:Z",
        "collection": "mobilities_national_raw",
    },
]


dag = build_capture_dag(
    dag_id="capture_mobilities",
    description="Capture mobilities (national + international) from Google Sheets to MongoDB raw.",
    source_type="mobilities",
    sources=MOBILITIES_SOURCES,
)
