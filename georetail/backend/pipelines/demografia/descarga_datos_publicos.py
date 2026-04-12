"""Descarga auditable de datos publicos para demografia_v2.

Esta capa separa el descubrimiento y la descarga de ficheros crudos de los
pipelines analiticos que despues consumiran esos datos. No transforma ni limpia
las fuentes: solo descubre, descarga, versiona y deja trazabilidad.
"""
from __future__ import annotations

import argparse
import asyncio
from dataclasses import dataclass
from datetime import datetime, timezone
from hashlib import sha256
from html.parser import HTMLParser
import io
import json
import logging
import os
from pathlib import Path
import re
from typing import Iterable, Optional
from urllib.parse import parse_qs, unquote, urljoin, urlparse
import unicodedata
import xml.etree.ElementTree as ET
import zipfile

import httpx

logger = logging.getLogger(__name__)

REPO_ROOT = Path(__file__).resolve().parents[3]
CSV_ROOT = Path(os.environ.get("CSV_DIR", str(REPO_ROOT / "CSV")))
META_ROOT = CSV_ROOT / "_meta"
AMB_RDF_PATH = CSV_ROOT / "amb_opendata" / "cataleg.rdf"

BCN_CKAN_ACTION = "https://opendata-ajuntament.barcelona.cat/data/api/action"
AMB_CATALOG_URL = "http://www.amb.cat/web/dades-obertes/cataleg.rdf"
IDESCAT_OPEN_DATA_BASE = "https://www.idescat.cat/dades/obertes"
INE_ADRH_SERIES_URL = "https://www.ine.es/dynt3/inebase/es/index.htm?padre=5608"

HTTP_TIMEOUT = httpx.Timeout(60.0)
HTTP_HEADERS = {
    "User-Agent": "GeoRetail public-data downloader/1.0",
    "Accept": "*/*",
}
HTTP_RETRIES = 3
RETRY_DELAY_SECONDS = 1.0

STATUS_DOWNLOADED = "downloaded"
STATUS_DOCUMENTARY_ONLY = "documentary_only"
STATUS_UNRESOLVED = "unresolved"
STATUS_BLOCKED = "blocked"

PROVIDER_BARCELONA = "barcelona_open_data"
PROVIDER_AMB = "amb_opendata"
PROVIDER_IDESCAT = "idescat"
PROVIDER_INE = "ine"

PRIORITY_BARCELONA = "barcelona_primary"
PRIORITY_AMB = "amb_fallback"
PRIORITY_IDESCAT = "idescat_reinforcement"
PRIORITY_INE = "ine_support"

NS = {
    "rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
    "dct": "http://purl.org/dc/terms/",
    "dcat": "http://vocab.deri.ie/dcat#",
    "rdfs": "http://www.w3.org/2000/01/rdf-schema#",
}


@dataclass(frozen=True)
class DatasetSpec:
    provider: str
    dataset_id: str
    title: str
    discovery_url: str
    priority: str
    intended_features: tuple[str, ...]


BARCELONA_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(PROVIDER_BARCELONA, "renda-disponible-llars-bcn", "Renda disponible llars BCN", f"{BCN_CKAN_ACTION}/package_show?id=renda-disponible-llars-bcn", PRIORITY_BARCELONA, ("renta_media_hogar", "delta_renta_3a")),
    DatasetSpec(PROVIDER_BARCELONA, "atles-renda-mitjana", "Atles renda mitjana", f"{BCN_CKAN_ACTION}/package_show?id=atles-renda-mitjana", PRIORITY_BARCELONA, ("renta_media_persona",)),
    DatasetSpec(PROVIDER_BARCELONA, "atles-renda-mediana", "Atles renda mediana", f"{BCN_CKAN_ACTION}/package_show?id=atles-renda-mediana", PRIORITY_BARCELONA, ("renta_mediana_persona",)),
    DatasetSpec(PROVIDER_BARCELONA, "atles-renda-index-gini", "Atles renda index gini", f"{BCN_CKAN_ACTION}/package_show?id=atles-renda-index-gini", PRIORITY_BARCELONA, ("gini",)),
    DatasetSpec(PROVIDER_BARCELONA, "atles-renda-p80-p20-distribucio", "Atles renda P80/P20", f"{BCN_CKAN_ACTION}/package_show?id=atles-renda-p80-p20-distribucio", PRIORITY_BARCELONA, ("p80_p20",)),
    DatasetSpec(PROVIDER_BARCELONA, "pad_mdbas_edat-q", "Padro grups quinquennals edat", f"{BCN_CKAN_ACTION}/package_show?id=pad_mdbas_edat-q", PRIORITY_BARCELONA, ("pct_poblacio_25_44", "edad_media")),
    DatasetSpec(PROVIDER_BARCELONA, "pad_mdbas_niv-educa-esta_sexe", "Padro nivell educatiu", f"{BCN_CKAN_ACTION}/package_show?id=pad_mdbas_niv-educa-esta_sexe", PRIORITY_BARCELONA, ("nivel_estudios_alto_pct",)),
    DatasetSpec(PROVIDER_BARCELONA, "pad_mdb_nacionalitat-g_edat-q_sexe", "Padro nacionalitat", f"{BCN_CKAN_ACTION}/package_show?id=pad_mdb_nacionalitat-g_edat-q_sexe", PRIORITY_BARCELONA, ("pct_extranjeros",)),
    DatasetSpec(PROVIDER_BARCELONA, "pad_dom_mdbas_n-persones", "Padro persones per domicili", f"{BCN_CKAN_ACTION}/package_show?id=pad_dom_mdbas_n-persones", PRIORITY_BARCELONA, ("tamano_hogar",)),
    DatasetSpec(PROVIDER_BARCELONA, "pad_dom_mdbas_tipus-domicili", "Padro tipus domicili", f"{BCN_CKAN_ACTION}/package_show?id=pad_dom_mdbas_tipus-domicili", PRIORITY_BARCELONA, ("tipo_hogar",)),
    DatasetSpec(PROVIDER_BARCELONA, "pad_dom_mdbas_edat-0018", "Padro 0-18 per domicili", f"{BCN_CKAN_ACTION}/package_show?id=pad_dom_mdbas_edat-0018", PRIORITY_BARCELONA, ("hogares_con_menores",)),
    DatasetSpec(PROVIDER_BARCELONA, "pad_sol_mdb_sexe_edat-q", "Padro persones soles", f"{BCN_CKAN_ACTION}/package_show?id=pad_sol_mdb_sexe_edat-q", PRIORITY_BARCELONA, ("personas_solas",)),
)

AMB_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(PROVIDER_AMB, "dataset_6687889", "Poblacio per grups quinquennals i sexe", AMB_CATALOG_URL, PRIORITY_AMB, ("pct_poblacio_25_44", "edad_media")),
    DatasetSpec(PROVIDER_AMB, "dataset_6733466", "Mitjana i mediana de la renda per unitat de consum per municipis", AMB_CATALOG_URL, PRIORITY_AMB, ("renta_media_uc", "renta_mediana_uc")),
    DatasetSpec(PROVIDER_AMB, "dataset_1672_iermb_rfdbh", "Renda familiar disponible bruta per habitant", AMB_CATALOG_URL, PRIORITY_AMB, ("renta_media_hogar",)),
    DatasetSpec(PROVIDER_AMB, "dataset_6740696", "Taxa d'atur registrat", AMB_CATALOG_URL, PRIORITY_AMB, ("tasa_paro",)),
    DatasetSpec(PROVIDER_AMB, "dataset_75_anys_viu_sola", "Poblacio de 75 anys i mes que viu sola", AMB_CATALOG_URL, PRIORITY_AMB, ("mayores_75_solos",)),
    DatasetSpec(PROVIDER_AMB, "dataset_formacio_secundaria", "Poblacio potencialment activa sense formacio secundaria post-obligatoria", AMB_CATALOG_URL, PRIORITY_AMB, ("formacion_baja_pct",)),
    DatasetSpec(PROVIDER_AMB, "dataset_irpf_tram", "Declarants de l'IRPF per tram", AMB_CATALOG_URL, PRIORITY_AMB, ("distribucion_irpf",)),
    DatasetSpec(PROVIDER_AMB, "dataset_ss_comercials", "Treballadors afiliats a la S.S en activitats comercials", AMB_CATALOG_URL, PRIORITY_AMB, ("afiliados_comercio",)),
    DatasetSpec(PROVIDER_AMB, "dataset_ss_coneixement", "Treballadors afiliats a la Seguretat Social per intensitat de coneixement", AMB_CATALOG_URL, PRIORITY_AMB, ("afiliados_conocimiento",)),
    DatasetSpec(PROVIDER_AMB, "dataset_ss_residencia_sexe", "Afiliats a la S.S. segons residencia padronal per sexe", AMB_CATALOG_URL, PRIORITY_AMB, ("afiliados_residencia_sexo",)),
)

IDESCAT_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(PROVIDER_IDESCAT, "irpf", "Impost sobre la renda de les persones fisiques", f"{IDESCAT_OPEN_DATA_BASE}/irpf", PRIORITY_IDESCAT, ("renta_media_persona", "distribucion_irpf")),
    DatasetSpec(PROVIDER_IDESCAT, "ist", "Index socioeconomic territorial", f"{IDESCAT_OPEN_DATA_BASE}/ist", PRIORITY_IDESCAT, ("indice_socioeconomico",)),
    DatasetSpec(PROVIDER_IDESCAT, "pmh", "Padro municipal d'habitants", f"{IDESCAT_OPEN_DATA_BASE}/pmh", PRIORITY_IDESCAT, ("poblacion", "pct_extranjeros", "edad_media")),
    DatasetSpec(PROVIDER_IDESCAT, "rfdbc", "Renda familiar disponible bruta territorial", f"{IDESCAT_OPEN_DATA_BASE}/rfdbc", PRIORITY_IDESCAT, ("renta_media_hogar",)),
    DatasetSpec(PROVIDER_IDESCAT, "afi", "Afiliats i afiliacions a la Seguretat Social", f"{IDESCAT_OPEN_DATA_BASE}/afi", PRIORITY_IDESCAT, ("afiliados_residencia_sexo",)),
    DatasetSpec(PROVIDER_IDESCAT, "censph", "Cens de poblacio i habitatges", f"{IDESCAT_OPEN_DATA_BASE}/censph", PRIORITY_IDESCAT, ("poblacion", "hogares", "viviendas")),
)

INE_DATASETS: tuple[DatasetSpec, ...] = (
    DatasetSpec(PROVIDER_INE, "adrh_barcelona_renta", "ADRH Barcelona renta media y mediana", INE_ADRH_SERIES_URL, PRIORITY_INE, ("renta_media_persona", "renta_media_uc", "renta_mediana_uc")),
    DatasetSpec(PROVIDER_INE, "adrh_barcelona_desigualdad", "ADRH Barcelona gini y P80/P20", INE_ADRH_SERIES_URL, PRIORITY_INE, ("gini", "p80_p20")),
)


class _LinkCollector(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[dict[str, str]] = []
        self._current_href: Optional[str] = None
        self._current_text: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, Optional[str]]]) -> None:
        attrs_dict = dict(attrs)
        if tag == "a":
            href = attrs_dict.get("href")
            if href:
                self._current_href = href
                self._current_text = []

    def handle_data(self, data: str) -> None:
        if self._current_href is not None:
            text = data.strip()
            if text:
                self._current_text.append(text)

    def handle_endtag(self, tag: str) -> None:
        if tag == "a" and self._current_href is not None:
            self.links.append({"href": self._current_href, "text": " ".join(self._current_text).strip()})
            self._current_href = None
            self._current_text = []


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFKD", value)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch))
    normalized = normalized.lower()
    normalized = re.sub(r"[^a-z0-9]+", " ", normalized)
    return re.sub(r"\s+", " ", normalized).strip()


def _slugify(value: str, *, default: str = "resource") -> str:
    slug = _normalize_text(value).replace(" ", "_")
    return slug or default


def _infer_format(label: str | None, url: str | None) -> str:
    joined = f"{label or ''} {url or ''}".lower()
    if ".csv" in joined or "csv" in joined:
        return "CSV"
    if ".json" in joined or "json" in joined:
        return "JSON"
    if ".xlsx" in joined or "xlsx" in joined:
        return "XLSX"
    if ".xls" in joined or " xls" in joined:
        return "XLS"
    if ".xml" in joined or "xml" in joined:
        return "XML"
    if ".rdf" in joined or "rdf" in joined:
        return "RDF"
    if ".zip" in joined or "zip" in joined:
        return "ZIP"
    if ".html" in joined or "search" in joined:
        return "HTML"
    return "UNKNOWN"


def _normalize_url(url: str | None) -> str:
    if not url:
        return ""
    return unquote(url.strip())


def _infer_granularity(*parts: str | None) -> str:
    text = _normalize_text(" ".join(part for part in parts if part))
    checks = (
        ("seccio_censal", ("seccion censal", "seccio censal", "census tract")),
        ("districte", ("districte", "distrito", "district")),
        ("barri", ("barri", "barrio", "neighborhood")),
        ("municipi", ("municipi", "municipio", "municipal")),
        ("ciutat", ("ciutat", "ciudad", "city")),
    )
    for label, tokens in checks:
        if any(token in text for token in tokens):
            return label
    return "unknown"


def _granularity_rank(value: str | None) -> int:
    order = {
        "seccio_censal": 5,
        "districte": 4,
        "barri": 3,
        "municipi": 2,
        "ciutat": 1,
        "unknown": 0,
    }
    return order.get(value or "unknown", 0)


def _extract_years(*parts: str | None) -> list[str]:
    pattern = re.compile(r"(?<![A-Za-z0-9])(19[5-9]\d|20\d{2})(?![A-Za-z0-9])")
    years: set[str] = set()
    for part in parts:
        if not part:
            continue
        text = part.strip()
        parsed = urlparse(text)
        if parsed.scheme and parsed.netloc:
            for match in pattern.findall(unquote(parsed.path)):
                years.add(match)

            query = parse_qs(parsed.query)
            for key, values in query.items():
                normalized_key = _normalize_text(key)
                if not any(token in normalized_key for token in ("year", "anyo", "anio", "any", "date", "fecha", "period", "time")):
                    continue
                for value in values:
                    for match in pattern.findall(unquote(value)):
                        years.add(match)
            continue

        for match in pattern.findall(text):
            years.add(match)
    return sorted(years)


def _year_token(years: Iterable[str]) -> str:
    values = sorted({str(year) for year in years if str(year)})
    if not values:
        return "unknown"
    if len(values) == 1:
        return values[0]
    return f"{values[0]}-{values[-1]}"


def _pick_extension(url: str, format_name: str) -> str:
    path = urlparse(url).path.lower()
    for ext in (".csv", ".json", ".xlsx", ".xls", ".xml", ".rdf", ".zip"):
        if path.endswith(ext):
            return ext.lstrip(".")
    return format_name.lower() if format_name and format_name != "UNKNOWN" else "bin"


def _guess_extension_from_response(response: httpx.Response, fallback_url: str, format_name: str) -> str:
    disposition = (response.headers.get("content-disposition") or "").lower()
    for ext in (".zip", ".csv", ".json", ".xlsx", ".xls", ".xml", ".rdf"):
        if ext in disposition:
            return ext.lstrip(".")

    content_type = (response.headers.get("content-type") or "").lower()
    if "zip" in content_type:
        return "zip"
    if "json" in content_type:
        return "json"
    if "csv" in content_type:
        return "csv"
    return _pick_extension(fallback_url, format_name)


def _extract_disposition_filename(response: httpx.Response) -> str:
    disposition = response.headers.get("content-disposition") or ""
    match = re.search(r'filename="?([^";]+)"?', disposition, re.I)
    return match.group(1) if match else ""


def _read_first_line(content: bytes, ext: str = "") -> str:
    if ext.lower() == "zip":
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as archive:
                names = [name for name in archive.namelist() if not name.endswith("/")]
                if not names:
                    return ""
                with archive.open(names[0]) as handle:
                    sample = handle.read(4096)
                return _read_first_line(sample, Path(names[0]).suffix.lstrip("."))
        except Exception:
            return ""
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            text = content.decode(encoding)
            return text.splitlines()[0].strip() if text else ""
        except UnicodeDecodeError:
            continue
    return ""


def _checksum(content: bytes) -> str:
    return sha256(content).hexdigest()


def _safe_filename_fragment(*parts: str) -> str:
    fragment = "__".join(_slugify(part, default="resource") for part in parts if part)
    return fragment[:120] or "resource"


def _dataset_dir(provider: str, dataset_id: str) -> Path:
    return CSV_ROOT / provider / dataset_id


def _manifest_paths(now: datetime) -> tuple[Path, Path]:
    META_ROOT.mkdir(parents=True, exist_ok=True)
    stamp = now.strftime("%Y%m%dT%H%M%SZ")
    return (
        META_ROOT / f"public_data_manifest_{stamp}.json",
        META_ROOT / "public_data_manifest_latest.json",
    )


def _default_entry(spec: DatasetSpec) -> dict:
    return {
        "provider": spec.provider,
        "dataset_id": spec.dataset_id,
        "title": spec.title,
        "discovery_url": spec.discovery_url,
        "download_url": "",
        "format": "",
        "granularity": "",
        "years": [],
        "license": "",
        "priority": spec.priority,
        "intended_features": list(spec.intended_features),
        "status": STATUS_UNRESOLVED,
        "files": [],
        "notes": [],
    }


def _is_amb_indicator_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(_normalize_url(url))
    return parsed.netloc == "indicadors.institutmetropoli.cat" and "id_ind=" in parsed.query


async def _request_text(client: httpx.AsyncClient, url: str, **kwargs) -> str:
    response = await _request(client, url, **kwargs)
    return response.text


async def _request_bytes(client: httpx.AsyncClient, url: str, **kwargs) -> bytes:
    response = await _request(client, url, **kwargs)
    return response.content


async def _request(client: httpx.AsyncClient, url: str, **kwargs) -> httpx.Response:
    last_error: Optional[Exception] = None
    for attempt in range(HTTP_RETRIES):
        try:
            response = await client.get(url, **kwargs)
            response.raise_for_status()
            return response
        except Exception as exc:  # pragma: no cover - exercised via network runs
            last_error = exc
            if attempt < HTTP_RETRIES - 1:
                await asyncio.sleep(RETRY_DELAY_SECONDS)
    raise last_error or RuntimeError(f"Request failed: {url}")


async def _download_resource(
    client: httpx.AsyncClient,
    *,
    provider: str,
    dataset_id: str,
    resource_label: str,
    resource_url: str,
    format_name: str,
    years: list[str],
    preferred: bool,
    granularity: str,
) -> dict:
    response = await _request(client, resource_url, follow_redirects=True)
    content = response.content
    dataset_dir = _dataset_dir(provider, dataset_id)
    dataset_dir.mkdir(parents=True, exist_ok=True)

    ext = _guess_extension_from_response(response, resource_url, format_name)
    year_part = _year_token(years)
    suffix = "" if preferred else f"__{_safe_filename_fragment(resource_label or resource_url)}"
    path = dataset_dir / f"{year_part}{suffix}.{ext}"
    path.write_bytes(content)

    disposition_name = _extract_disposition_filename(response)
    parsed = urlparse(resource_url)
    if not disposition_name and parsed.query:
        query = parse_qs(parsed.query)
        for key in ("filename", "file", "download", "name"):
            values = query.get(key)
            if values:
                disposition_name = values[0]
                break
    original_name = disposition_name or Path(unquote(parsed.path)).name or path.name

    return {
        "url": resource_url,
        "local_path": str(path.relative_to(REPO_ROOT)),
        "original_filename": original_name,
        "size_bytes": len(content),
        "checksum_sha256": _checksum(content),
        "first_line": _read_first_line(content, ext),
        "downloaded_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "format": format_name,
        "granularity": granularity,
        "years": years,
        "preferred": preferred,
    }


def _finalize_entry(entry: dict) -> dict:
    files = entry.get("files") or []
    preferred_file = next((item for item in files if item.get("preferred")), files[0] if files else None)
    if preferred_file:
        entry["download_url"] = preferred_file.get("url", "")
        entry["format"] = preferred_file.get("format", "")
        entry["granularity"] = preferred_file.get("granularity", "")
        if not entry.get("years"):
            entry["years"] = preferred_file.get("years", [])
    elif not entry.get("years"):
        entry["years"] = []
    return entry


def _collect_download_candidates(html: str, base_url: str) -> list[dict]:
    collector = _LinkCollector()
    collector.feed(html)

    candidates: list[dict] = []
    seen: set[str] = set()
    for link in collector.links:
        href = link.get("href") or ""
        absolute = urljoin(base_url, href)
        format_name = _infer_format(link.get("text"), absolute)
        if format_name not in {"CSV", "JSON", "XLSX", "XLS", "ZIP"}:
            continue
        if absolute in seen:
            continue
        seen.add(absolute)
        candidates.append(
            {
                "url": absolute,
                "label": link.get("text") or absolute,
                "format": format_name,
                "granularity": _infer_granularity(link.get("text"), absolute),
                "years": _extract_years(link.get("text"), absolute),
            }
        )
    return candidates


def _collect_ine_links(html: str, base_url: str) -> list[dict]:
    collector = _LinkCollector()
    collector.feed(html)
    links: list[dict] = []
    for link in collector.links:
        href = link.get("href") or ""
        text = link.get("text") or ""
        absolute = urljoin(base_url, href)
        links.append({"href": absolute, "text": text})
    return links


def _collect_ine_table_candidates(html: str, base_url: str) -> list[dict]:
    candidates: list[dict] = []
    for link in _collect_ine_links(html, base_url):
        href = link.get("href") or ""
        if "/jaxiT3/Tabla.htm" not in href:
            continue
        parsed = urlparse(href)
        table_id = parse_qs(parsed.query).get("t", [""])[0]
        if not table_id:
            continue
        candidates.append(
            {
                "table_id": table_id,
                "label": link.get("text") or href,
                "table_url": href,
                "export_url": f"https://www.ine.es/jaxiT3/dlgExport.htm?t={table_id}&L=0",
            }
        )
    return candidates


def _collect_ine_export_candidates(html: str, base_url: str) -> list[dict]:
    candidates: list[dict] = []
    seen: set[str] = set()
    for link in _collect_ine_links(html, base_url):
        href = link.get("href") or ""
        format_name = _infer_format(link.get("text"), href)
        if format_name not in {"CSV", "JSON", "XLSX", "XLS"}:
            continue
        if href in seen:
            continue
        seen.add(href)
        candidates.append(
            {
                "url": href,
                "label": link.get("text") or href,
                "format": format_name,
            }
        )
    return candidates


def _extract_ine_series_years(html: str) -> list[str]:
    match = re.search(r"Serie\s+(19[5-9]\d|20\d{2})\s*-\s*(19[5-9]\d|20\d{2})", html, re.I)
    if match:
        return [match.group(1), match.group(2)]
    return ["2015", "2023"]


def _find_ine_link(links: list[dict], *tokens: str) -> str:
    normalized_tokens = tuple(_normalize_text(token) for token in tokens if token)
    for link in links:
        normalized_text = _normalize_text(link.get("text"))
        if all(token in normalized_text for token in normalized_tokens):
            return link["href"]
    raise RuntimeError(f"INE page does not expose a link containing tokens: {tokens!r}")


def _ine_export_sort_key(item: dict) -> tuple[int, int]:
    url = (item.get("url") or "").lower()
    if item.get("format") == "CSV" and "csv_bdsc" in url:
        return (4, 0)
    if item.get("format") == "CSV":
        return (3, 0)
    if item.get("format") == "JSON":
        return (2, 0)
    if item.get("format") == "XLSX":
        return (1, 0)
    if item.get("format") == "XLS":
        return (0, 0)
    return (-1, 0)


def _pick_preferred_barcelona(resources: list[dict]) -> dict[str, str]:
    preferred_by_year: dict[str, str] = {}
    grouped: dict[str, list[dict]] = {}
    for resource in resources:
        year = _year_token(resource.get("years", []))
        grouped.setdefault(year, []).append(resource)

    for year, items in grouped.items():
        items.sort(
            key=lambda item: (
                _granularity_rank(item.get("granularity")),
                len(item.get("years") or []),
                _normalize_text(item.get("name") or item.get("url")),
            ),
            reverse=True,
        )
        preferred_by_year[year] = items[0]["resource_id"]
    return preferred_by_year


def _amb_distribution_sort_key(item: dict) -> tuple[int, int]:
    order = {"CSV": 5, "JSON": 4, "XLSX": 3, "XLS": 2, "XML": 1, "HTML": 0, "RDF": 0, "UNKNOWN": -1}
    if _is_amb_indicator_url(item.get("url")):
        return (6, _granularity_rank(item.get("granularity")))
    return (order.get(item.get("format", "UNKNOWN"), -1), _granularity_rank(item.get("granularity")))


def _parse_amb_rdf_catalog(rdf_path: Path) -> list[dict]:
    tree = ET.parse(rdf_path)
    root = tree.getroot()
    datasets: list[dict] = []
    for dataset in root.findall("dcat:Dataset", NS):
        about = dataset.attrib.get(f"{{{NS['rdf']}}}about", "").strip()
        dataset_id = about.lstrip("#") or ""
        titles = [title.text.strip() for title in dataset.findall("dct:title", NS) if title.text]
        description = next((node.text for node in dataset.findall("dct:description", NS) if node.text), "")
        license_url = dataset.attrib.get(f"{{{NS['rdf']}}}resource", "")
        distributions: list[dict] = []
        for dist_node in dataset.findall("dcat:distribution", NS):
            inner = dist_node.find("dcat:Distribution", NS)
            if inner is None:
                continue
            access = _normalize_url(inner.findtext("dcat:accessURL", default="", namespaces=NS).strip())
            labels = [
                label.text.strip()
                for label in inner.findall(".//rdfs:label", NS)
                if label.text and label.text.strip()
            ]
            format_name = _infer_format(" ".join(labels), access)
            if not access:
                continue
            distributions.append(
                {
                    "url": access,
                    "format": format_name,
                    "label": " / ".join(labels) or access,
                    "granularity": _infer_granularity(" ".join(titles), description, access),
                    "years": _extract_years(access, description, " ".join(titles)),
                }
            )
        datasets.append(
            {
                "dataset_id": dataset_id,
                "titles": titles,
                "description": description or "",
                "license": license_url,
                "distributions": distributions,
            }
        )
    return datasets


async def _resolve_amb_indicator_csv(client: httpx.AsyncClient, indicator_url: str) -> str:
    html = await _request_text(client, indicator_url, follow_redirects=True)
    match = re.search(r'<div style="display: none" id="default_display">(.*?)</div>', html, re.S)
    if match is None:
        raise RuntimeError(f"AMB indicator page without default_display payload: {indicator_url}")

    default_display = match.group(1)
    export_page = await client.post(
        urljoin(indicator_url, "ajax/exportCSV.php"),
        data={"default_display": default_display},
    )
    export_page.raise_for_status()
    href_match = re.search(r"href=['\"]([^'\"]+\.csv)['\"]", export_page.text, re.I)
    if href_match is None:
        raise RuntimeError(f"AMB indicator page did not return a CSV export link: {indicator_url}")
    return urljoin(indicator_url, href_match.group(1))


def _idescat_geo_granularity(code: str) -> str:
    mapping = {
        "sec": "seccio_censal",
        "mun": "municipi",
        "neigh_bcn": "barri",
        "dist_bcn": "districte",
        "com": "comarca",
        "at": "municipi",
        "prov": "provincia",
        "cat": "comunitat_autonoma",
    }
    return mapping.get(code, "unknown")


def _parse_idescat_open_data_page(html: str) -> dict:
    marker = "IDESCAT.obertes("
    start = html.find(marker)
    if start < 0:
        raise RuntimeError("Idescat open-data page does not expose an IDESCAT.obertes payload.")
    decoder = json.JSONDecoder()
    payload, _ = decoder.raw_decode(html[start + len(marker):])
    return payload


def _build_idescat_resources(spec: DatasetSpec, catalog_payload: dict) -> list[dict]:
    dataset_id = str(catalog_payload.get("id") or spec.dataset_id).lower()
    resources: list[dict] = []
    for item in catalog_payload.get("zip", []):
        geo = item.get("geo") or "unknown"
        label = f"{item.get('label') or dataset_id} [{geo}]"
        years = [str(year) for year in item.get("time", []) if str(year)]
        params = [f"n={item.get('n')}", f"by={'mun' if geo == 'neigh_bcn' else geo}", "f=zip", "fi=csv"]
        if isinstance(item.get("t"), str) and item.get("t"):
            params.append(f"t={item['t']}")
        resource_url = f"https://www.idescat.cat/pub/?id={dataset_id}&" + "&".join(params)
        resources.append(
            {
                "url": resource_url,
                "label": label,
                "format": "CSV",
                "granularity": _idescat_geo_granularity(geo),
                "years": years,
            }
        )
    return resources


async def _process_barcelona_dataset(client: httpx.AsyncClient, spec: DatasetSpec) -> dict:
    entry = _default_entry(spec)
    try:
        payload = await _request(client, f"{BCN_CKAN_ACTION}/package_show", params={"id": spec.dataset_id})
        result = payload.json().get("result", {})
        entry["title"] = result.get("title") or entry["title"]
        entry["license"] = result.get("license_title") or result.get("license_url") or ""
        resources: list[dict] = []
        for resource in result.get("resources", []):
            resource_url = resource.get("url") or resource.get("download_url") or ""
            format_name = _infer_format(resource.get("format"), resource_url)
            if format_name != "CSV":
                continue
            years = _extract_years(resource.get("name"), resource.get("description"), resource_url)
            resources.append(
                {
                    "resource_id": resource.get("id") or resource_url,
                    "name": resource.get("name") or resource_url,
                    "url": resource_url,
                    "format": format_name,
                    "granularity": _infer_granularity(resource.get("name"), resource.get("description"), resource_url),
                    "years": years,
                }
            )
        if not resources:
            entry["status"] = STATUS_UNRESOLVED
            entry["notes"].append("No CSV resources found in CKAN package_show.")
            return _finalize_entry(entry)

        preferred_by_year = _pick_preferred_barcelona(resources)
        all_years: set[str] = set()
        for resource in resources:
            years = resource.get("years") or []
            all_years.update(years)
            downloaded = await _download_resource(
                client,
                provider=spec.provider,
                dataset_id=spec.dataset_id,
                resource_label=resource["name"],
                resource_url=resource["url"],
                format_name=resource["format"],
                years=years,
                preferred=preferred_by_year.get(_year_token(years)) == resource["resource_id"],
                granularity=resource["granularity"],
            )
            entry["files"].append(downloaded)
        entry["years"] = sorted(all_years)
        entry["status"] = STATUS_DOWNLOADED
        return _finalize_entry(entry)
    except Exception as exc:  # pragma: no cover - exercised via network runs
        entry["status"] = STATUS_BLOCKED
        entry["notes"].append(str(exc))
        return _finalize_entry(entry)


async def _process_amb_dataset(client: httpx.AsyncClient, spec: DatasetSpec, catalog: list[dict]) -> dict:
    entry = _default_entry(spec)
    normalized_target = _normalize_text(spec.title)
    dataset = next(
        (
            item
            for item in catalog
            if normalized_target in {_normalize_text(title) for title in item.get("titles", [])}
            or normalized_target in _normalize_text(" ".join(item.get("titles", [])))
        ),
        None,
    )
    if dataset is None:
        entry["status"] = STATUS_UNRESOLVED
        entry["notes"].append("Dataset title not found in local AMB RDF catalog.")
        return _finalize_entry(entry)

    entry["dataset_id"] = dataset.get("dataset_id") or entry["dataset_id"]
    entry["title"] = next((title for title in dataset.get("titles", []) if title), entry["title"])
    entry["license"] = dataset.get("license") or ""

    distributions = sorted(dataset.get("distributions", []), key=_amb_distribution_sort_key, reverse=True)
    if not distributions:
        entry["status"] = STATUS_DOCUMENTARY_ONLY
        entry["notes"].append("AMB dataset only exposes documentary distributions.")
        return _finalize_entry(entry)

    candidate_resources: list[dict] = []
    errors: list[str] = []
    seen_sources: set[str] = set()
    for dist in distributions:
        raw_url = dist.get("url", "")
        if not raw_url:
            continue
        try:
            if _is_amb_indicator_url(raw_url):
                resolved_url = await _resolve_amb_indicator_csv(client, raw_url)
                format_name = "CSV"
                label = f"{dist['label']} (resolved indicator export)"
            elif dist.get("format") in {"CSV", "JSON", "XLSX", "XLS", "XML"}:
                resolved_url = raw_url
                format_name = dist["format"]
                label = dist["label"]
            else:
                continue

            source_key = _normalize_url(raw_url if _is_amb_indicator_url(raw_url) else resolved_url)
            if source_key in seen_sources:
                continue
            seen_sources.add(source_key)
            candidate_resources.append(
                {
                    "url": resolved_url,
                    "format": format_name,
                    "label": label,
                    "granularity": dist["granularity"],
                    "years": dist.get("years") or _extract_years(resolved_url, label),
                }
            )
        except Exception as exc:
            errors.append(f"{raw_url}: {exc}")

    if not candidate_resources:
        entry["status"] = STATUS_BLOCKED if errors else STATUS_DOCUMENTARY_ONLY
        entry["notes"].extend(errors or ["AMB dataset only exposes documentary distributions."])
        if distributions:
            first = distributions[0]
            entry["download_url"] = first.get("url", "")
            entry["format"] = first.get("format", "")
            entry["granularity"] = first.get("granularity", "")
            entry["years"] = first.get("years", [])
        return _finalize_entry(entry)

    preferred = candidate_resources[0]
    all_years: set[str] = set()
    for dist in candidate_resources:
        years = dist.get("years") or []
        all_years.update(years)
        try:
            downloaded = await _download_resource(
                client,
                provider=spec.provider,
                dataset_id=entry["dataset_id"],
                resource_label=dist["label"],
                resource_url=dist["url"],
                format_name=dist["format"],
                years=years,
                preferred=dist["url"] == preferred["url"],
                granularity=dist["granularity"],
            )
            entry["files"].append(downloaded)
        except Exception as exc:
            errors.append(f"{dist['url']}: {exc}")

    entry["years"] = sorted(all_years)
    entry["notes"].extend(errors)
    entry["status"] = STATUS_DOWNLOADED if entry["files"] else STATUS_BLOCKED
    return _finalize_entry(entry)


async def _process_idescat_dataset(client: httpx.AsyncClient, spec: DatasetSpec) -> dict:
    entry = _default_entry(spec)
    try:
        html = await _request_text(client, spec.discovery_url, follow_redirects=True)
        payload = _parse_idescat_open_data_page(html)
        entry["title"] = payload.get("tagline") or entry["title"]
        candidates = _build_idescat_resources(spec, payload)
        if not candidates:
            entry["status"] = STATUS_UNRESOLVED
            entry["notes"].append("No direct CSV/JSON export links discovered on Idescat page.")
            return _finalize_entry(entry)

        preferred = sorted(candidates, key=lambda item: (_granularity_rank(item.get("granularity")), len(item.get("years") or [])), reverse=True)[0]
        all_years: set[str] = set()
        for candidate in candidates:
            years = candidate.get("years") or []
            all_years.update(years)
            downloaded = await _download_resource(
                client,
                provider=spec.provider,
                dataset_id=spec.dataset_id,
                resource_label=candidate["label"],
                resource_url=candidate["url"],
                format_name=candidate["format"],
                years=years,
                preferred=candidate["url"] == preferred["url"],
                granularity=candidate["granularity"],
            )
            entry["files"].append(downloaded)
        entry["status"] = STATUS_DOWNLOADED
        entry["years"] = sorted(all_years)
        return _finalize_entry(entry)
    except Exception as exc:  # pragma: no cover - exercised via network runs
        entry["status"] = STATUS_BLOCKED
        entry["notes"].append(str(exc))
        return _finalize_entry(entry)


def _matches_ine_metric(spec: DatasetSpec, label: str) -> bool:
    normalized = _normalize_text(label)
    if spec.dataset_id.endswith("renta"):
        return any(token in normalized for token in ("renta media", "renta mediana", "unidad de consumo", "persona"))
    return any(token in normalized for token in ("gini", "p80", "p20"))


async def _discover_ine_barcelona_page(client: httpx.AsyncClient) -> str:
    municipal_tree_url = f"{INE_ADRH_SERIES_URL}&capsel=12384"
    municipal_html = await _request_text(client, municipal_tree_url, follow_redirects=True)
    province_index_url = _find_ine_link(
        _collect_ine_links(municipal_html, municipal_tree_url),
        "por provincias",
    )

    province_index_html = await _request_text(client, province_index_url, follow_redirects=True)
    return _find_ine_link(_collect_ine_links(province_index_html, province_index_url), "barcelona")


async def _process_ine_dataset(client: httpx.AsyncClient, spec: DatasetSpec, barcelona_page_url: str) -> dict:
    entry = _default_entry(spec)
    try:
        html = await _request_text(client, barcelona_page_url, follow_redirects=True)
        table_candidates = [item for item in _collect_ine_table_candidates(html, barcelona_page_url) if _matches_ine_metric(spec, item.get("label", ""))]
        if not table_candidates:
            entry["status"] = STATUS_UNRESOLVED
            entry["notes"].append("No machine-readable ADRH tables matched this metric on the Barcelona page.")
            return _finalize_entry(entry)

        table = table_candidates[0]
        entry["title"] = table["label"]
        entry["discovery_url"] = table["table_url"]

        export_html = await _request_text(client, table["export_url"], follow_redirects=True)
        candidates = sorted(_collect_ine_export_candidates(export_html, table["export_url"]), key=_ine_export_sort_key, reverse=True)
        if not candidates:
            entry["status"] = STATUS_UNRESOLVED
            entry["notes"].append("INE ADRH table does not expose a CSV/JSON export dialog.")
            return _finalize_entry(entry)

        years = _extract_ine_series_years(html)
        preferred = candidates[0]
        downloaded = await _download_resource(
            client,
            provider=spec.provider,
            dataset_id=spec.dataset_id,
            resource_label=table["label"],
            resource_url=preferred["url"],
            format_name=preferred["format"],
            years=years,
            preferred=True,
            granularity="seccio_censal",
        )
        entry["files"].append(downloaded)
        entry["status"] = STATUS_DOWNLOADED
        entry["years"] = years
        return _finalize_entry(entry)
    except Exception as exc:  # pragma: no cover - exercised via network runs
        entry["status"] = STATUS_BLOCKED
        entry["notes"].append(str(exc))
        return _finalize_entry(entry)


def _build_summary(entries: list[dict]) -> dict:
    summary = {
        "downloaded": 0,
        "documentary_only": 0,
        "unresolved": 0,
        "blocked": 0,
        "total_files": 0,
    }
    for entry in entries:
        summary[entry["status"]] = summary.get(entry["status"], 0) + 1
        summary["total_files"] += len(entry.get("files") or [])
    return summary


def _write_manifest(manifest: dict) -> tuple[Path, Path]:
    generated_at = datetime.strptime(manifest["generated_at"], "%Y-%m-%dT%H:%M:%SZ").replace(tzinfo=timezone.utc)
    timestamped_path, latest_path = _manifest_paths(generated_at)
    serialized = json.dumps(manifest, indent=2, ensure_ascii=False)
    timestamped_path.write_text(serialized, encoding="utf-8")
    latest_path.write_text(serialized, encoding="utf-8")
    return timestamped_path, latest_path


async def ejecutar(*, providers: Optional[set[str]] = None) -> dict:
    selected = providers or {PROVIDER_BARCELONA, PROVIDER_AMB, PROVIDER_IDESCAT, PROVIDER_INE}
    entries: list[dict] = []

    async with httpx.AsyncClient(timeout=HTTP_TIMEOUT, headers=HTTP_HEADERS) as client:
        if PROVIDER_BARCELONA in selected:
            for spec in BARCELONA_DATASETS:
                logger.info("Barcelona Open Data: %s", spec.dataset_id)
                entries.append(await _process_barcelona_dataset(client, spec))

        if PROVIDER_AMB in selected:
            if not AMB_RDF_PATH.exists():
                raise FileNotFoundError(f"AMB RDF catalog not found: {AMB_RDF_PATH}")
            amb_catalog = _parse_amb_rdf_catalog(AMB_RDF_PATH)
            for spec in AMB_DATASETS:
                logger.info("AMB Open Data: %s", spec.title)
                entries.append(await _process_amb_dataset(client, spec, amb_catalog))

        if PROVIDER_IDESCAT in selected:
            for spec in IDESCAT_DATASETS:
                logger.info("Idescat Open Data: %s", spec.dataset_id)
                entries.append(await _process_idescat_dataset(client, spec))

        if PROVIDER_INE in selected:
            barcelona_page_url = await _discover_ine_barcelona_page(client)
            for spec in INE_DATASETS:
                logger.info("INE ADRH: %s", spec.dataset_id)
                entries.append(await _process_ine_dataset(client, spec, barcelona_page_url))

    generated_at = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
    manifest = {
        "generated_at": generated_at,
        "csv_root": str(CSV_ROOT),
        "summary": _build_summary(entries),
        "datasets": entries,
    }
    timestamped_path, latest_path = _write_manifest(manifest)
    manifest["manifest_path"] = str(timestamped_path.relative_to(REPO_ROOT))
    manifest["manifest_latest_path"] = str(latest_path.relative_to(REPO_ROOT))
    return manifest


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Descarga auditable de datos publicos para GeoRetail.")
    parser.add_argument(
        "--providers",
        default="barcelona_open_data,amb_opendata,idescat,ine",
        help="Lista separada por comas de proveedores a ejecutar.",
    )
    return parser.parse_args()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
    args = _parse_args()
    providers = {part.strip() for part in args.providers.split(",") if part.strip()}
    result = asyncio.run(ejecutar(providers=providers))
    print(json.dumps(result["summary"], indent=2, ensure_ascii=False))
    print(result["manifest_path"])


if __name__ == "__main__":
    main()
