from __future__ import annotations

from pathlib import Path

from pipelines.demografia.descarga_datos_publicos import (
    _collect_download_candidates,
    _collect_ine_export_candidates,
    _collect_ine_table_candidates,
    _build_idescat_resources,
    _extract_years,
    _extract_ine_series_years,
    _infer_format,
    _ine_export_sort_key,
    _normalize_text,
    _parse_idescat_open_data_page,
    _parse_amb_rdf_catalog,
    _finalize_entry,
    DatasetSpec,
    PRIORITY_IDESCAT,
    PROVIDER_IDESCAT,
    _pick_preferred_barcelona,
    _year_token,
)


def test_normalize_text_removes_accents_and_punctuation():
    assert _normalize_text("Població per grups quinquennals i sexe") == "poblacio per grups quinquennals i sexe"


def test_infer_format_prefers_known_download_types():
    assert _infer_format("CSV", "https://example.com/data.csv") == "CSV"
    assert _infer_format("JSON", "https://example.com/api") == "JSON"
    assert _infer_format("", "https://example.com/file.xlsx") == "XLSX"


def test_extract_years_deduplicates_and_sorts():
    assert _extract_years("2019 renda", "2022_renda.csv", "2022") == ["2019", "2022"]


def test_extract_years_ignores_uuid_noise():
    assert _extract_years("https://example.com/resource/0e205580-6d55-4599-bd13-086de83130b8/download/2019_renda.csv") == ["2019"]


def test_extract_years_ignores_non_temporal_query_ids():
    assert _extract_years("https://indicadors.institutmetropoli.cat/index.php?ap=0&id_ind=2073") == []


def test_year_token_handles_ranges():
    assert _year_token(["2019"]) == "2019"
    assert _year_token(["2019", "2022", "2020"]) == "2019-2022"
    assert _year_token([]) == "unknown"


def test_pick_preferred_barcelona_prefers_finer_granularity():
    resources = [
        {
            "resource_id": "district",
            "name": "2022 districte",
            "url": "https://example.com/2022-districte.csv",
            "granularity": "districte",
            "years": ["2022"],
        },
        {
            "resource_id": "section",
            "name": "2022 seccio censal",
            "url": "https://example.com/2022-seccio.csv",
            "granularity": "seccio_censal",
            "years": ["2022"],
        },
    ]
    assert _pick_preferred_barcelona(resources) == {"2022": "section"}


def test_collect_download_candidates_extracts_machine_readable_links():
    html = """
    <html><body>
      <a href="/files/pmh_2024.csv">CSV 2024 municipis</a>
      <a href="https://example.com/pmh_2024.json">JSON 2024</a>
      <a href="/view">Vista HTML</a>
    </body></html>
    """
    candidates = _collect_download_candidates(html, "https://www.idescat.cat/dades/obertes/pmh")
    assert len(candidates) == 2
    assert candidates[0]["format"] == "CSV"
    assert candidates[0]["years"] == ["2024"]
    assert candidates[1]["format"] == "JSON"


def test_parse_amb_rdf_catalog_extracts_dataset_and_distribution(tmp_path: Path):
    rdf = """<?xml version="1.0" encoding="UTF-8"?>
    <rdf:RDF
      xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"
      xmlns:dct="http://purl.org/dc/terms/"
      xmlns:rdfs="http://www.w3.org/2000/01/rdf-schema#"
      xmlns:dcat="http://vocab.deri.ie/dcat#">
      <dcat:Dataset rdf:about="#dataset_1">
        <dct:title xml:lang="ca">Taxa d'atur registrat</dct:title>
        <dct:description xml:lang="ca">Serie 2023 per municipi</dct:description>
        <dcat:distribution>
          <dcat:Distribution>
            <dcat:accessURL>http://opendata.amb.cat/atur/search/csv</dcat:accessURL>
            <dct:format>
              <dct:IMT>
                <rdfs:label>CSV</rdfs:label>
              </dct:IMT>
            </dct:format>
          </dcat:Distribution>
        </dcat:distribution>
      </dcat:Dataset>
    </rdf:RDF>"""
    rdf_path = tmp_path / "cataleg.rdf"
    rdf_path.write_text(rdf, encoding="utf-8")

    datasets = _parse_amb_rdf_catalog(rdf_path)
    assert len(datasets) == 1
    assert datasets[0]["dataset_id"] == "dataset_1"
    assert "Taxa d'atur registrat" in datasets[0]["titles"]
    assert datasets[0]["distributions"][0]["format"] == "CSV"
    assert datasets[0]["distributions"][0]["years"] == ["2023"]


def test_parse_idescat_payload_builds_pub_download_urls():
    html = """
    <script>
    IDESCAT.obertes({"id":"IRPF","tagline":"Test","zip":[{"label":"Base imposable","geo":"cat","note":null,"t":null,"n":4070,"time":["2000","2023"],"nobs":[48]}],"geo":["cat"],"corrections":null},{"descarregar":"x"});
    </script>
    """
    payload = _parse_idescat_open_data_page(html)
    spec = DatasetSpec(PROVIDER_IDESCAT, "irpf", "IRPF", "https://www.idescat.cat/dades/obertes/irpf", PRIORITY_IDESCAT, ("renta_media_persona",))
    resources = _build_idescat_resources(spec, payload)
    assert payload["id"] == "IRPF"
    assert len(resources) == 1
    assert resources[0]["url"] == "https://www.idescat.cat/pub/?id=irpf&n=4070&by=cat&f=zip&fi=csv"
    assert resources[0]["format"] == "CSV"
    assert resources[0]["years"] == ["2000", "2023"]


def test_collect_ine_table_candidates_extracts_table_and_export_urls():
    html = """
    <html><body>
      <a href="/jaxiT3/Tabla.htm?t=30896&L=0">Indicadores de renta media y mediana</a>
      <a href="/jaxiT3/Tabla.htm?t=37686&L=0">Índice de Gini y Distribución de la renta P80/P20</a>
    </body></html>
    """
    candidates = _collect_ine_table_candidates(html, "https://www.ine.es/dynt3/inebase/es/index.htm?padre=5608&capsel=5659")
    assert [item["table_id"] for item in candidates] == ["30896", "37686"]
    assert candidates[0]["export_url"] == "https://www.ine.es/jaxiT3/dlgExport.htm?t=30896&L=0"


def test_collect_ine_export_candidates_prefers_semicolon_csv():
    html = """
    <html><body>
      <a href="files/t/es/csv_bd/30896.csv">CSV: separado por tabuladores</a>
      <a href="files/t/es/csv_bdsc/30896.csv">CSV: separado por ;</a>
      <a href="https://servicios.ine.es/wstempus/jsCache/es/DATOS_TABLA/30896?tip=AM&">Json</a>
    </body></html>
    """
    candidates = _collect_ine_export_candidates(html, "https://www.ine.es/jaxiT3/dlgExport.htm?t=30896&L=0")
    ordered = sorted(candidates, key=_ine_export_sort_key, reverse=True)
    assert ordered[0]["url"] == "https://www.ine.es/jaxiT3/files/t/es/csv_bdsc/30896.csv"
    assert ordered[1]["format"] == "CSV"
    assert ordered[2]["format"] == "JSON"


def test_extract_ine_series_years_reads_header_range():
    html = "<h1>Atlas de distribución de renta de los hogares. Serie 2015-2023</h1>"
    assert _extract_ine_series_years(html) == ["2015", "2023"]


def test_finalize_entry_preserves_aggregated_years():
    entry = {
        "years": ["2015", "2016", "2017"],
        "files": [
            {
                "url": "https://example.com/2017.csv",
                "format": "CSV",
                "granularity": "seccio_censal",
                "years": ["2017"],
                "preferred": True,
            }
        ],
    }
    result = _finalize_entry(entry)
    assert result["download_url"] == "https://example.com/2017.csv"
    assert result["years"] == ["2015", "2016", "2017"]
