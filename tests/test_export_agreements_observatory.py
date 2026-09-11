"""Focused tests for agreement export mappings."""

from __future__ import annotations

import importlib.util
from pathlib import Path
import unittest

import pandas as pd


SCRIPT_PATH = (
    Path(__file__).resolve().parents[1]
    / "config"
    / "scripts"
    / "export_agreements_observatory.py"
)
SPEC = importlib.util.spec_from_file_location("export_agreements_observatory", SCRIPT_PATH)
assert SPEC and SPEC.loader
EXPORTER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(EXPORTER)


class AgreementExportMappingTests(unittest.TestCase):
    def test_international_geography_and_academic_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "__source": "agreements_international",
                    "__source_sheet": "ACTIVOS",
                    "__ambito": "internacional",
                    "CONTINENTE": "AMÉRICA,EUROPA",
                    "PAÍS": "MÉXICO",
                    "UNIDADES ACADÉMICAS": "Facultad de Ingeniería",
                    "DEPENDENCIA RESPONSABLE UDEA": "Dirección de Relaciones Internacionales",
                    "FECHA DE INICIO": "01/01/2026",
                }
            ]
        )

        exported = EXPORTER.build_export_dataframe(raw).iloc[0]

        self.assertEqual(exported["continente"], "América, Europa")
        self.assertEqual(exported["departamento"], "")
        self.assertEqual(exported["facultades"], "Facultad de Ingeniería")
        self.assertEqual(
            exported["unidad_académica_administrativa"],
            "Dirección de Relaciones Internacionales",
        )

    def test_national_geography_and_academic_fields(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "__source": "agreements_national",
                    "__source_sheet": "ACTIVOS",
                    "__ambito": "nacional",
                    "DEPARTAMENTO": "VALLE DEL CAUCA",
                    "CIUDAD": "FLORENCIA",
                    "Código de convenio": "231",
                    "UNIDAD ACADÉMICA": "Facultad Nacional de Salud Pública",
                    "DEPENDENCIA RESPONSABLE UDEA": "Vicerrectoría de Extensión",
                    "FECHA INICIO": "01/01/2026",
                }
            ]
        )

        exported = EXPORTER.build_export_dataframe(raw).iloc[0]

        self.assertEqual(exported["continente"], "")
        self.assertEqual(exported["departamento"], "Valle del Cauca")
        self.assertEqual(exported["país/ciudad"], "Florencia (Caquetá)")
        self.assertEqual(exported["código"], "231")
        self.assertEqual(exported["facultades"], "Facultad Nacional de Salud Pública")
        self.assertEqual(
            exported["unidad_académica_administrativa"],
            "Vicerrectoría de Extensión",
        )

    def test_armenia_is_disambiguated(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "__source": "agreements_national",
                    "__source_sheet": "ACTIVOS",
                    "__ambito": "nacional",
                    "CIUDAD": "armenia",
                    "FECHA DE INICIO": "01/01/2026",
                }
            ]
        )

        exported = EXPORTER.build_export_dataframe(raw).iloc[0]

        self.assertEqual(exported["país/ciudad"], "Armenia (Quindío)")

    def test_legacy_national_code_header_is_exported(self) -> None:
        raw = pd.DataFrame(
            [
                {
                    "__source": "agreements_national",
                    "__source_sheet": "ACTIVOS",
                    "__ambito": "nacional",
                    "CÓDIGO": "201",
                    "FECHA DE INICIO": "01/01/2026",
                }
            ]
        )

        exported = EXPORTER.build_export_dataframe(raw).iloc[0]

        self.assertEqual(exported["código"], "201")


if __name__ == "__main__":
    unittest.main()
