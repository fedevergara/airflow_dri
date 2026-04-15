"""Capture agreements sources from Google Sheets into MongoDB raw collections."""

from airflow.models.dag import DAG  # noqa: F401 - Helps DAG safe discovery.
from capture_pipeline import build_capture_dag

AGREEMENTS_SOURCES = [
    {
        "source_name": "agreements_international",
        "spreadsheet_id_var": "GSHEET_AGREEMENTS_INTERNATIONAL_ID",
        "range_var": "GSHEET_AGREEMENTS_INTERNATIONAL_RANGE",
        "default_range": "A:Z",
        "collection": "agreements_international_raw",
    },
    {
        "source_name": "agreements_national",
        "spreadsheet_id_var": "GSHEET_AGREEMENTS_NATIONAL_ID",
        "range_var": "GSHEET_AGREEMENTS_NATIONAL_RANGE",
        "default_range": "A:Z",
        "collection": "agreements_national_raw",
    },
]


dag = build_capture_dag(
    dag_id="capture_agreements",
    description="Capture agreements (national + international) from Google Sheets to MongoDB raw.",
    source_type="agreements",
    sources=AGREEMENTS_SOURCES,
)
