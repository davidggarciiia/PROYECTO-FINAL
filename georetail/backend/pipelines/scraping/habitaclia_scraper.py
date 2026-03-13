"""
pipelines/scraping/habitaclia_scraper.py — Scraper de Habitaclia para locales en alquiler.

Habitaclia usa SSR con JSON incrustado en <script type="application/json" id="initData">.
Anti-bot: Cloudflare básico + rate limiting.
curl_cffi con chrome131 suele funcionar directamente.

Endpoints observados:
  - HTML:   https://www.habitaclia.com/buscar/local-comercial/barcelona/alquiler/?page=N
  - API v1: https://www.habitaclia.com/api/v1/properties?type=local&operation=rent&city=barcelona&page=N
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.habitaclia.com"
_SEARCH_URL = _BASE_URL + "/buscar/local-comercial/{ciudad}/alquiler/"
_API_URL = _BASE_URL + "/api/v1/properties"

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
}


class HabitacliaScraper:
    """
    Extrae locales en alquiler de Habitaclia.

    Intenta primero la API JSON interna (más limpia),
    si falla hace scraping HTML.
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 5) -> list[dict]:
        ciudad_slug = _CIUDADES.get(ciudad, ciudad)
        resultados: list[dict] = []

        async with BaseScraper(self.cfg) as scraper:
            for pagina in range(1, max_paginas + 1):
                # Intentar API JSON primero
                items = await self._scrape_api(scraper, ciudad_slug, pagina)
                if items is None:
                    # API no disponible → scraping HTML
                    items = await self._scrape_html(scraper, ciudad_slug, pagina)
                if not items:
                    logger.info("Habitaclia: sin más resultados en pág %d", pagina)
                    break

                resultados.extend(items)
                logger.info("Habitaclia: +%d locales (total %d)", len(items), len(resultados))

        return resultados

    async def _scrape_api(self, scraper: BaseScraper, ciudad: str, pagina: int) -> Optional[list[dict]]:
        """Intenta obtener datos vía API JSON interna de Habitaclia."""
        try:
            url = f"{_API_URL}?type=local&operation=rent&city={ciudad}&page={pagina}&limit=30"
            html = await scraper.get(url, referer=_SEARCH_URL.format(ciudad=ciudad))
            if not html:
                return None
            data = json.loads(html)
            items = data.get("items") or data.get("properties") or data.get("results") or []
            if not isinstance(items, list):
                return None
            return [_parse_habitaclia_item(i) for i in items if _parse_habitaclia_item(i)]
        except (json.JSONDecodeError, Exception):
            return None

    async def _scrape_html(self, scraper: BaseScraper, ciudad: str, pagina: int) -> list[dict]:
        """Scraping HTML de Habitaclia con extracción del JSON incrustado."""
        url = _SEARCH_URL.format(ciudad=ciudad)
        if pagina > 1:
            url += f"?page={pagina}"
        referer = _SEARCH_URL.format(ciudad=ciudad) if pagina > 1 else _BASE_URL

        html = await scraper.get(url, referer=referer)
        if not html:
            return []

        return _parse_habitaclia_html(html)


def _parse_habitaclia_html(html: str) -> list[dict]:
    """Extrae listings del JSON incrustado en el HTML de Habitaclia."""
    results = []

    # Método 1: initData JSON (Habitaclia inyecta aquí todos los listings)
    match = re.search(r'<script[^>]+id="initData"[^>]*>(.*?)</script>', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            listings = (
                data.get("listings", [])
                or data.get("properties", [])
                or data.get("items", [])
                or _deep_find_list(data, "listing")
                or []
            )
            for item in listings:
                parsed = _parse_habitaclia_item(item)
                if parsed:
                    results.append(parsed)
            if results:
                return results
        except json.JSONDecodeError:
            pass

    # Método 2: window.__DATA__ o window.__INITIAL_STATE__
    for pattern in [r'window\.__DATA__\s*=\s*({.*?});', r'window\.__INITIAL_STATE__\s*=\s*({.*?});']:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                listings = _deep_find_list(data, "properties") or _deep_find_list(data, "listings") or []
                for item in listings:
                    parsed = _parse_habitaclia_item(item)
                    if parsed:
                        results.append(parsed)
                if results:
                    return results
            except json.JSONDecodeError:
                pass

    # Método 3: JSON-LD
    matches = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
    for m in matches:
        try:
            data = json.loads(m)
            if isinstance(data, list):
                for item in data:
                    parsed = _parse_jsonld_item(item)
                    if parsed:
                        results.append(parsed)
            elif isinstance(data, dict) and data.get("@type") in ("Product", "ItemList"):
                if "itemListElement" in data:
                    for elem in data["itemListElement"]:
                        parsed = _parse_jsonld_item(elem.get("item", elem))
                        if parsed:
                            results.append(parsed)
        except json.JSONDecodeError:
            pass

    return results


def _parse_habitaclia_item(item: dict) -> Optional[dict]:
    """Normaliza un listing de Habitaclia al formato interno."""
    try:
        price = (
            item.get("price")
            or item.get("priceMin")
            or item.get("monthlyCost")
            or item.get("rent")
        )
        lat = item.get("lat") or item.get("latitude") or item.get("geo", {}).get("lat")
        lng = item.get("lng") or item.get("longitude") or item.get("geo", {}).get("lng")
        m2 = item.get("area") or item.get("surface") or item.get("size")
        prop_id = item.get("id") or item.get("propertyId") or item.get("code")
        address = item.get("address") or item.get("fullAddress") or item.get("street", "")
        district = item.get("district") or item.get("location", {}).get("district", "")
        neighborhood = item.get("neighborhood") or item.get("zone", "")
        url_path = item.get("url") or item.get("href") or item.get("slug", "")

        if not price and not lat:
            return None

        return {
            "id": f"habitaclia_{prop_id}" if prop_id else None,
            "fuente": "habitaclia",
            "precio": float(price) if price else None,
            "m2": float(m2) if m2 else None,
            "precio_m2": round(float(price) / float(m2), 2) if price and m2 and float(m2) > 0 else None,
            "lat": float(lat) if lat else None,
            "lng": float(lng) if lng else None,
            "direccion": address,
            "distrito": district,
            "barrio": neighborhood,
            "url": f"{_BASE_URL}{url_path}" if url_path and not url_path.startswith("http") else url_path,
        }
    except Exception as e:
        logger.debug("Error normalizando listing Habitaclia: %s", e)
        return None


def _parse_jsonld_item(item: dict) -> Optional[dict]:
    try:
        price = item.get("offers", {}).get("price") or item.get("price")
        geo = item.get("geo", {})
        addr = item.get("address", {})
        return {
            "id": f"habitaclia_{item.get('identifier', '')}",
            "fuente": "habitaclia",
            "precio": float(price) if price else None,
            "m2": None,
            "precio_m2": None,
            "lat": float(geo.get("latitude")) if geo.get("latitude") else None,
            "lng": float(geo.get("longitude")) if geo.get("longitude") else None,
            "direccion": addr.get("streetAddress", ""),
            "distrito": addr.get("addressLocality", ""),
            "barrio": "",
            "url": item.get("url", ""),
        }
    except Exception:
        return None


def _deep_find_list(obj, key: str, _depth: int = 0):
    """Búsqueda recursiva de una clave con lista (máx profundidad 6)."""
    if _depth > 6:
        return None
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list) and len(obj[key]) > 0:
            return obj[key]
        for v in obj.values():
            r = _deep_find_list(v, key, _depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _deep_find_list(item, key, _depth + 1)
            if r:
                return r
    return None
