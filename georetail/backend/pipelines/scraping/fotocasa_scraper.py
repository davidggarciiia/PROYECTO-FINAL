"""
pipelines/scraping/fotocasa_scraper.py — Scraper de Fotocasa para locales en alquiler.

Fotocasa usa React + hidratación de JSON incrustado en __NEXT_DATA__.
Estrategia: curl_cffi (TLS impersonation) → extraer JSON de <script id="__NEXT_DATA__">
Si falla → Playwright con stealth.

Anti-bot de Fotocasa: Cloudflare + DataDome.
curl_cffi con impersonate="chrome131" suele ser suficiente para DataDome.
Si hay Cloudflare con JS challenge → ScrapingBee/ZenRows.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional
from urllib.parse import quote

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.fotocasa.es"
_SEARCH_URL = (
    _BASE_URL
    + "/es/alquiler/locales-comerciales/{ciudad}/todas-las-zonas/l/{page}"
)

# Ciudades a escanear (slug de Fotocasa)
_CIUDADES = {
    "barcelona": "barcelona-capital",
    "madrid":    "madrid-capital",
    "valencia":  "valencia-ciudad",
}


class FotocasaScraper:
    """
    Extrae anuncios de locales en alquiler de Fotocasa.

    Ejemplo de uso:
        scraper = FotocasaScraper()
        locales = await scraper.scrape(ciudad="barcelona", max_paginas=5)
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 5) -> list[dict]:
        """Devuelve lista de dicts con datos de locales en alquiler."""
        slug = _CIUDADES.get(ciudad, f"{ciudad}-capital")
        resultados: list[dict] = []

        async with BaseScraper(self.cfg) as scraper:
            for pagina in range(1, max_paginas + 1):
                url = _SEARCH_URL.format(ciudad=slug, page=pagina)
                referer = _SEARCH_URL.format(ciudad=slug, page=pagina - 1) if pagina > 1 else _BASE_URL
                logger.info("Fotocasa scraping pág %d: %s", pagina, url)

                html = await scraper.get(url, referer=referer)

                if not html:
                    # Fallback a Playwright si curl_cffi falló
                    logger.info("Fotocasa: intentando con Playwright para pág %d", pagina)
                    html = await scraper.get_with_playwright(
                        url, wait_selector="[data-testid='CardList']"
                    )

                if not html:
                    logger.warning("Fotocasa: sin respuesta para pág %d — abortando", pagina)
                    break

                items = _parse_fotocasa(html)
                if not items:
                    logger.info("Fotocasa: sin más resultados en pág %d", pagina)
                    break

                resultados.extend(items)
                logger.info("Fotocasa: +%d locales (total %d)", len(items), len(resultados))

        return resultados


def _parse_fotocasa(html: str) -> list[dict]:
    """
    Extrae datos del JSON incrustado en __NEXT_DATA__ de Next.js.
    Fotocasa inyecta todos los listings en el SSR payload.
    """
    # Método 1: __NEXT_DATA__ JSON (el más fiable)
    match = re.search(r'<script id="__NEXT_DATA__" type="application/json">(.*?)</script>', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            items = _extract_from_next_data(data)
            if items:
                return items
        except (json.JSONDecodeError, KeyError):
            pass

    # Método 2: JSON-LD schema.org
    matches = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
    items = []
    for m in matches:
        try:
            data = json.loads(m)
            if isinstance(data, dict) and data.get("@type") in ("Product", "RealEstateListing"):
                item = _parse_jsonld(data)
                if item:
                    items.append(item)
        except json.JSONDecodeError:
            pass
    if items:
        return items

    # Método 3: Regex sobre HTML como último recurso
    return _parse_html_regex(html)


def _extract_from_next_data(data: dict) -> list[dict]:
    """Navega el árbol de __NEXT_DATA__ de Fotocasa para extraer listings."""
    results = []
    try:
        # La estructura cambia entre versiones. Intentar varios paths conocidos.
        props = data.get("props", {}).get("pageProps", {})
        # Path 1: initialProps.results
        listings = (
            props.get("initialProps", {}).get("results", [])
            or props.get("listings", [])
            or props.get("properties", [])
            or _deep_find_list(data, "results")
            or []
        )
        for item in listings:
            parsed = _parse_listing(item)
            if parsed:
                results.append(parsed)
    except Exception as e:
        logger.debug("Error parseando __NEXT_DATA__ Fotocasa: %s", e)
    return results


def _parse_listing(item: dict) -> Optional[dict]:
    """Normaliza un listing de Fotocasa al formato interno."""
    try:
        # Fotocasa tiene varias estructuras según la versión de la app
        price = (
            item.get("price")
            or item.get("priceInfo", {}).get("amount")
            or item.get("transactions", [{}])[0].get("value", [{}])[0].get("amount")
        )
        lat = item.get("latitude") or item.get("coordinates", {}).get("latitude")
        lng = item.get("longitude") or item.get("coordinates", {}).get("longitude")
        m2 = item.get("surface") or item.get("size")
        prop_id = item.get("id") or item.get("propertyCode") or item.get("realEstateCode")
        address = item.get("address") or item.get("location", {}).get("address", "")
        district = item.get("district") or item.get("location", {}).get("district", "")
        neighborhood = item.get("neighborhood") or item.get("location", {}).get("neighborhood", "")

        if not price and not lat:
            return None

        return {
            "id": f"fotocasa_{prop_id}" if prop_id else None,
            "fuente": "fotocasa",
            "precio": float(price) if price else None,
            "m2": float(m2) if m2 else None,
            "precio_m2": round(float(price) / float(m2), 2) if price and m2 and float(m2) > 0 else None,
            "lat": float(lat) if lat else None,
            "lng": float(lng) if lng else None,
            "direccion": address,
            "distrito": district,
            "barrio": neighborhood,
            "url": f"{_BASE_URL}{item.get('url', '')}",
        }
    except Exception as e:
        logger.debug("Error normalizando listing Fotocasa: %s", e)
        return None


def _parse_jsonld(data: dict) -> Optional[dict]:
    """Extrae datos de JSON-LD schema.org."""
    try:
        price_spec = data.get("offers", {}).get("price") or data.get("price")
        geo = data.get("geo", {})
        return {
            "id": f"fotocasa_{data.get('identifier', data.get('url', '')[-20:])}",
            "fuente": "fotocasa",
            "precio": float(price_spec) if price_spec else None,
            "m2": None,
            "precio_m2": None,
            "lat": float(geo.get("latitude")) if geo.get("latitude") else None,
            "lng": float(geo.get("longitude")) if geo.get("longitude") else None,
            "direccion": data.get("address", {}).get("streetAddress", ""),
            "distrito": data.get("address", {}).get("addressLocality", ""),
            "barrio": "",
            "url": data.get("url", ""),
        }
    except Exception:
        return None


def _parse_html_regex(html: str) -> list[dict]:
    """Extracción de último recurso mediante regex sobre el HTML."""
    results = []
    # Buscar patrones de precio como "1.200 €/mes"
    prices = re.findall(r'"price"\s*:\s*(\d+(?:\.\d+)?)', html)
    lats = re.findall(r'"latitude"\s*:\s*([\d.]+)', html)
    lngs = re.findall(r'"longitude"\s*:\s*([\d.]+)', html)

    for i, price in enumerate(prices[:50]):
        results.append({
            "id": f"fotocasa_regex_{i}",
            "fuente": "fotocasa",
            "precio": float(price),
            "m2": None,
            "precio_m2": None,
            "lat": float(lats[i]) if i < len(lats) else None,
            "lng": float(lngs[i]) if i < len(lngs) else None,
            "direccion": "",
            "distrito": "",
            "barrio": "",
            "url": "",
        })
    return results


def _deep_find_list(obj, key: str) -> Optional[list]:
    """Búsqueda recursiva de una clave que contiene una lista."""
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list) and len(obj[key]) > 0:
            return obj[key]
        for v in obj.values():
            result = _deep_find_list(v, key)
            if result:
                return result
    elif isinstance(obj, list):
        for item in obj:
            result = _deep_find_list(item, key)
            if result:
                return result
    return None
