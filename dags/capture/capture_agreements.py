"""Capture international and national agreements from Google sources into MongoDB."""

from airflow.models.dag import DAG  # noqa: F401 - Helps DAG safe discovery.
from capture.capture_pipeline import build_capture_dag

AGREEMENTS_REQUIRED_DATE_GROUPS = [
    ["FECHA DE INICIO", "FECHA INICIO"],
]

AGREEMENTS_SOURCES = [
    {
        "source_name": "agreements_international",
        "spreadsheet_id_var": "GSHEET_AGREEMENTS_INTERNATIONAL_ID",
        "range_var": "GSHEET_AGREEMENTS_INTERNATIONAL_RANGE",
        "default_range": "ACTIVOS,INACTIVOS",
        "source_mode": "drive_excel",
        "drive_token_path_var": "GOOGLE_DRIVE_EXCEL_TOKEN_PICKLE_PATH",
        "drive_token_path_default": "/opt/airflow/config/secrets/agreements/agreements_drive_token.pickle",
        "required_non_empty_groups": AGREEMENTS_REQUIRED_DATE_GROUPS,
        "collection": "agreements",
    },
    {
        "source_name": "agreements_national",
        "spreadsheet_id_var": "GSHEET_AGREEMENTS_NATIONAL_ID",
        "range_var": "GSHEET_AGREEMENTS_NATIONAL_RANGE",
        "default_range": "ACTIVOS,INACTIVOS",
        "source_mode": "google_sheets_workbook",
        "drive_token_path_var": "GOOGLE_DRIVE_TOKEN_PICKLE_PATH",
        "drive_token_path_default": "/opt/airflow/config/secrets/agreements/agreements_token.pickle",
        "required_non_empty_groups": AGREEMENTS_REQUIRED_DATE_GROUPS,
        "collection": "agreements",
    },
]


dag = build_capture_dag(
    dag_id="capture_agreements",
    description="Capture agreements (international + national) from Google sources to MongoDB.",
    source_type="agreements",
    sources=AGREEMENTS_SOURCES,
)
