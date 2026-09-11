from __future__ import annotations

import importlib.util
import unittest
from pathlib import Path


MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "scripts"
    / "export_research_projects_observatory.py"
)
SPEC = importlib.util.spec_from_file_location("export_research_projects_observatory", MODULE_PATH)
assert SPEC and SPEC.loader
MODULE = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(MODULE)


HEADERS = [
    "CODIGO_PROYECTO",
    "AÑO_INICIO",
    "FECHA_INICIO",
    "FECHA_FINALIZACION",
    "Area OCDE, con respectoa  los grupos",
    "GRUPO",
    "RAZON_SOCIAL_APORTANTE",
    "TIPO_APORTANTE",
    "TOTAL_FRESCO",
    "TOTAL_ESPECIE",
    "MONEDA",
    "PAIS_ENTIDAD",
]


def source_row(project_code: str, entity: str) -> list[object]:
    return [
        project_code,
        2024,
        "1/2/2024",
        "2025-03-04",
        "Ciencias Sociales",
        "Grupo de Prueba",
        entity,
        "cofinanciador",
        "$1,250,000",
        "$500.50",
        "cop",
        "Colombia",
    ]


class ResearchProjectsTransformTests(unittest.TestCase):
    def test_managed_surplus_column_can_be_removed_during_schema_migration(self) -> None:
        self.assertTrue(
            MODULE.response_contains_only_managed_columns(
                {"values": [["pais"], ["Colombia"], ["España"]]}
            )
        )

    def test_unknown_surplus_column_is_protected(self) -> None:
        self.assertFalse(
            MODULE.response_contains_only_managed_columns(
                {"values": [["nota_manual"], ["No eliminar"]]}
            )
        )
        self.assertFalse(
            MODULE.response_contains_only_managed_columns(
                {"values": [[], ["Dato sin encabezado"]]}
            )
        )

    def test_dependency_is_not_part_of_the_target_schema(self) -> None:
        self.assertNotIn("unidad_académica_administrativa", MODULE.TARGET_COLUMNS)
        self.assertNotIn("unit", MODULE.SOURCE_COLUMNS)

    def test_external_entities_are_exported_and_udea_is_omitted(self) -> None:
        external = source_row("P-1", "Entidad Externa")
        values = [
            HEADERS,
            source_row("P-1", "Universidad de Antioquia"),
            external,
            list(external),
            source_row("P-2", "Entidad Externa"),
        ]

        rows, metrics = MODULE.build_export_rows(values)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0][0:4], ["P-1", 2024, "01/02/2024", "04/03/2025"])
        self.assertEqual(rows[0][6], "ENTIDAD EXTERNA")
        self.assertEqual(rows[0][8:11], [1250000, 500.5, "COP"])
        self.assertEqual(metrics["source_unique_projects"], 2)
        self.assertEqual(metrics["external_unique_projects"], 2)
        self.assertEqual(metrics["selected_source_rows"], 3)
        self.assertEqual(metrics["duplicates_removed"], 1)

    def test_deduplication_does_not_merge_different_projects(self) -> None:
        rows, _ = MODULE.build_export_rows(
            [HEADERS, source_row("P-1", "Entidad X"), source_row("P-2", "Entidad X")]
        )
        self.assertEqual(len(rows), 2)
        self.assertEqual([row[0] for row in rows], ["P-1", "P-2"])
        self.assertEqual(rows[0][1:], rows[1][1:])

    def test_blank_year_is_derived_from_start_date(self) -> None:
        row = source_row("P-1", "Entidad X")
        row[1] = ""
        rows, _ = MODULE.build_export_rows([HEADERS, row])
        self.assertEqual(rows[0][1], 2024)

    def test_row_without_year_or_dates_is_omitted(self) -> None:
        row = source_row("P-1", "Entidad X")
        row[1:4] = ["", "", ""]
        rows, metrics = MODULE.build_export_rows([HEADERS, row])
        self.assertEqual(rows, [])
        self.assertEqual(metrics["rows_without_year_or_dates_omitted"], 1)

    def test_missing_required_column_is_rejected(self) -> None:
        with self.assertRaisesRegex(ValueError, "PAIS_ENTIDAD"):
            MODULE.build_export_rows([HEADERS[:-1]])

    def test_invalid_amount_reports_source_row(self) -> None:
        row = source_row("P-1", "Entidad X")
        row[8] = "valor desconocido"
        with self.assertRaisesRegex(ValueError, "Source row 2"):
            MODULE.build_export_rows([HEADERS, row])

    def test_udea_name_variants_are_detected(self) -> None:
        self.assertTrue(MODULE.is_udea_entity("UDEA"))
        self.assertTrue(MODULE.is_udea_entity("Universidad de Antioquia - UdeA"))
        self.assertFalse(MODULE.is_udea_entity("Universidad de Medellín"))

    def test_entity_names_are_uppercase_including_connectors(self) -> None:
        row = source_row(
            "P-1",
            "Ministry of Science for the Country y Universidad de Prueba",
        )
        rows, _ = MODULE.build_export_rows([HEADERS, row])
        self.assertEqual(
            rows[0][6],
            "MINISTRY OF SCIENCE FOR THE COUNTRY Y UNIVERSIDAD DE PRUEBA",
        )


if __name__ == "__main__":
    unittest.main()
