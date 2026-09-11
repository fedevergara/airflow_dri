"""Focused tests for Kahi Google Sheet grid management."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

import pandas as pd


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "scripts"
    / "export_kahi_looker.py"
)
SPEC = importlib.util.spec_from_file_location("export_kahi_looker", SCRIPT_PATH)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


class FakeRequest:
    def __init__(self, response: dict | None = None) -> None:
        self.response = response or {}

    def execute(self) -> dict:
        return self.response


class FakeHttpResponse(dict):
    def __init__(self, status: int) -> None:
        super().__init__()
        self.status = status


class FakeHttpError(Exception):
    def __init__(self, status: int) -> None:
        super().__init__(f"HTTP {status}")
        self.resp = FakeHttpResponse(status)


class FlakyRequest:
    def __init__(self, failures: list[int], response: dict | None = None) -> None:
        self.failures = list(failures)
        self.response = response or {"ok": True}
        self.calls = 0

    def execute(self) -> dict:
        self.calls += 1
        if self.failures:
            raise FakeHttpError(self.failures.pop(0))
        return self.response


class FakeValuesResource:
    def __init__(self, range_values: dict[str, dict] | None = None) -> None:
        self.range_values = range_values or {}
        self.get_calls: list[dict] = []
        self.update_calls: list[dict] = []
        self.clear_calls: list[dict] = []

    def get(self, **kwargs: object) -> FakeRequest:
        self.get_calls.append(dict(kwargs))
        return FakeRequest(self.range_values.get(str(kwargs["range"]), {}))

    def update(self, **kwargs: object) -> FakeRequest:
        self.update_calls.append(dict(kwargs))
        return FakeRequest()

    def clear(self, **kwargs: object) -> FakeRequest:
        self.clear_calls.append(dict(kwargs))
        return FakeRequest()


class FakeSpreadsheetsResource:
    def __init__(self, metadata: dict, range_values: dict[str, dict] | None = None) -> None:
        self.metadata = metadata
        self.values_resource = FakeValuesResource(range_values)
        self.batch_update_calls: list[dict] = []

    def get(self, **kwargs: object) -> FakeRequest:
        return FakeRequest(self.metadata)

    def values(self) -> FakeValuesResource:
        return self.values_resource

    def batchUpdate(self, **kwargs: object) -> FakeRequest:  # noqa: N802 - Google API name.
        self.batch_update_calls.append(dict(kwargs))
        return FakeRequest()


class FakeSheetsService:
    def __init__(self, metadata: dict, range_values: dict[str, dict] | None = None) -> None:
        self.resource = FakeSpreadsheetsResource(metadata, range_values)

    def spreadsheets(self) -> FakeSpreadsheetsResource:
        return self.resource


class FakeCursor(list):
    def batch_size(self, _: int) -> "FakeCursor":
        return self

    def limit(self, amount: int) -> "FakeCursor":
        return FakeCursor(self[:amount])

    def close(self) -> None:
        return None


class FakeCollection:
    def __init__(self, documents: list[dict]) -> None:
        self.documents = documents

    def find(self, *_: object, **__: object) -> FakeCursor:
        return FakeCursor(self.documents)


class FakeDatabase(dict):
    pass


def sheet_metadata(
    title: str,
    sheet_id: int,
    rows: int,
    columns: int,
) -> dict:
    return {
        "properties": {
            "title": title,
            "sheetId": sheet_id,
            "gridProperties": {"rowCount": rows, "columnCount": columns},
        }
    }


class KahiGridManagementTests(unittest.TestCase):
    def test_dashboard_model_preserves_every_pdf_dimension_without_blends(self) -> None:
        work = {
            "_id": "work-1",
            "author_count": 2,
            "year_published": 2025,
            "authors": [
                {
                    "id": "udea-author",
                    "full_name": "UdeA Author",
                    "affiliations": [
                        {
                            "id": "udea",
                            "name": "Universidad de Antioquia",
                            "types": [{"type": "institution"}],
                            "addresses": [
                                {"country_code": "CO", "country": "Colombia"}
                            ],
                        },
                        {
                            "id": "group-1",
                            "name": "Grupo Uno",
                            "types": [{"type": "group"}],
                        },
                        {
                            "id": "faculty-1",
                            "name": "Facultad Uno",
                            "types": [{"type": "faculty"}],
                        },
                    ],
                },
                {
                    "id": "external-author",
                    "full_name": "External Author",
                    "affiliations": [
                        {
                            "id": "external-institution",
                            "name": "Universidad Nacional Autónoma de México",
                            "types": [{"type": "institution"}],
                            "addresses": [
                                {"country_code": "MX", "country": "Mexico"}
                            ],
                        },
                        {
                            "id": "colombian-institution",
                            "name": "Universidad Nacional de Colombia",
                            "types": [{"type": "institution"}],
                            "addresses": [
                                {"country_code": "CO", "country": "Colombia"}
                            ],
                        },
                    ],
                },
            ],
            "subjects": [
                {
                    "source": "openalex",
                    "subjects": [
                        {"name": "Medicine", "level": 0},
                        {"name": "Internal medicine", "level": 1},
                        {"name": "Public health", "level": 0},
                    ],
                }
            ],
        }
        database = FakeDatabase(
            affiliations=FakeCollection(
                [{"_id": "udea"}, {"_id": "group-1"}, {"_id": "faculty-1"}]
            ),
            works=FakeCollection([work]),
        )

        result = EXPORTER.build_sheet_data(
            db=database,
            institution_id="udea",
            limit_works=None,
            batch_size=100,
        )

        self.assertEqual(set(result), set(EXPORTER.SHEET_COLUMNS))
        self.assertEqual(result["dashboard_general"].iloc[0].to_dict(), {
            "colav_id": "work-1",
            "año": 2025,
            "cantidad_autores": 2,
            "colaboración": "Internacional",
            "cantidad_países_extranjeros": 1,
        })
        self.assertEqual(result["dashboard_paises"].iloc[0]["país"], "Mexico")
        institutions = result["dashboard_instituciones"].set_index("institución")
        self.assertEqual(
            set(institutions.index),
            {
                "Universidad Nacional Autónoma de México",
                "Universidad Nacional de Colombia",
            },
        )
        self.assertEqual(
            institutions.loc["Universidad Nacional de Colombia", "colaboración"],
            "Nacional",
        )
        self.assertEqual(
            institutions.loc[
                "Universidad Nacional Autónoma de México", "colaboración"
            ],
            "Internacional",
        )
        self.assertEqual(
            result["dashboard_temas"][["tema", "categoría_tema"]].values.tolist(),
            [["Medicine", "Medicine"], ["Public health", "Otros"]],
        )
        self.assertEqual(result["dashboard_grupos"].iloc[0]["grupo"], "Grupo Uno")
        self.assertEqual(
            result["dashboard_grupos"].iloc[0]["unidad_académica"],
            "Facultad Uno",
        )

    def test_foreign_country_normalization(self) -> None:
        self.assertFalse(EXPORTER.is_foreign_country("CO", "Colombia"))
        self.assertFalse(EXPORTER.is_foreign_country("COL", ""))
        self.assertTrue(EXPORTER.is_foreign_country("MX", "Mexico"))
        self.assertFalse(EXPORTER.is_foreign_country("", ""))
        self.assertEqual(EXPORTER.institution_collaboration("CO", "Colombia"), "Nacional")
        self.assertEqual(EXPORTER.institution_collaboration("MX", "Mexico"), "Internacional")
        self.assertEqual(EXPORTER.institution_collaboration("", ""), "Sin país")

    def test_column_index_to_label(self) -> None:
        self.assertEqual(EXPORTER.column_index_to_label(1), "A")
        self.assertEqual(EXPORTER.column_index_to_label(26), "Z")
        self.assertEqual(EXPORTER.column_index_to_label(27), "AA")
        self.assertEqual(EXPORTER.column_index_to_label(702), "ZZ")

    def test_resize_removes_empty_columns_and_adjusts_rows(self) -> None:
        service = FakeSheetsService(
            {
                "sheets": [
                    sheet_metadata("works", 1, 100, 26),
                    sheet_metadata("subjects", 2, 60, 26),
                    sheet_metadata("affiliations", 3, 80, 6),
                ]
            }
        )
        sheet_data = {
            "works": pd.DataFrame([[""] * 7] * 199, columns=[f"w{i}" for i in range(7)]),
            "subjects": pd.DataFrame([[""] * 2] * 49, columns=["s1", "s2"]),
            "affiliations": pd.DataFrame(
                [[""] * 6] * 79,
                columns=[f"a{i}" for i in range(6)],
            ),
        }

        shapes = EXPORTER.resize_sheet_grids(
            sheets_service=service,
            spreadsheet_id="spreadsheet",
            sheet_data=sheet_data,
        )

        self.assertEqual(
            shapes,
            {
                "works": {"rows": 200, "columns": 7},
                "subjects": {"rows": 50, "columns": 2},
                "affiliations": {"rows": 80, "columns": 6},
            },
        )
        requests = service.resource.batch_update_calls[0]["body"]["requests"]
        self.assertEqual(requests[0]["deleteDimension"]["range"]["startIndex"], 7)
        self.assertEqual(requests[0]["deleteDimension"]["range"]["endIndex"], 26)
        self.assertEqual(requests[1]["appendDimension"]["dimension"], "ROWS")
        self.assertEqual(requests[1]["appendDimension"]["length"], 100)
        self.assertEqual(requests[2]["deleteDimension"]["range"]["startIndex"], 2)
        self.assertEqual(requests[3]["deleteDimension"]["range"]["dimension"], "ROWS")

    def test_resize_refuses_nonempty_surplus_columns(self) -> None:
        service = FakeSheetsService(
            {"sheets": [sheet_metadata("works", 1, 100, 26)]},
            {"'works'!H:Z": {"values": [["=1+1"]]}},
        )
        sheet_data = {
            "works": pd.DataFrame(columns=[f"w{i}" for i in range(7)]),
        }

        with self.assertRaisesRegex(RuntimeError, "Refusing to delete non-empty"):
            EXPORTER.resize_sheet_grids(
                sheets_service=service,
                spreadsheet_id="spreadsheet",
                sheet_data=sheet_data,
            )

        self.assertEqual(service.resource.batch_update_calls, [])

    def test_writer_clears_only_the_dataframe_columns(self) -> None:
        service = FakeSheetsService({"sheets": []})
        dataframe = pd.DataFrame([["1"] * 7], columns=[f"w{i}" for i in range(7)])

        written = EXPORTER.write_sheet_dataframe(
            sheets_service=service,
            spreadsheet_id="spreadsheet",
            sheet_name="works",
            dataframe=dataframe,
            chunk_size=5000,
        )

        self.assertEqual(written, 1)
        self.assertEqual(
            service.resource.values_resource.clear_calls[0]["range"],
            "'works'!A2:G",
        )
        self.assertEqual(
            [call["range"] for call in service.resource.values_resource.update_calls],
            ["'works'!A1", "'works'!A2"],
        )

    def test_write_controller_retries_quota_errors_with_backoff(self) -> None:
        clock = [0.0]
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            clock[0] += seconds

        controller = EXPORTER.SheetsWriteController(
            min_interval_seconds=1.0,
            max_attempts=3,
            sleep_fn=sleep,
            monotonic_fn=lambda: clock[0],
            jitter_fn=lambda: 0.0,
        )
        request = FlakyRequest([429])

        response = controller.execute(request, operation="test write")

        self.assertEqual(response, {"ok": True})
        self.assertEqual(request.calls, 2)
        self.assertEqual(sleeps, [1.0])

    def test_write_controller_throttles_consecutive_writes(self) -> None:
        clock = [0.0]
        sleeps: list[float] = []

        def sleep(seconds: float) -> None:
            sleeps.append(seconds)
            clock[0] += seconds

        controller = EXPORTER.SheetsWriteController(
            min_interval_seconds=1.2,
            sleep_fn=sleep,
            monotonic_fn=lambda: clock[0],
            jitter_fn=lambda: 0.0,
        )

        controller.execute(FlakyRequest([]), operation="first write")
        controller.execute(FlakyRequest([]), operation="second write")

        self.assertEqual(sleeps, [1.2])


if __name__ == "__main__":
    unittest.main()
