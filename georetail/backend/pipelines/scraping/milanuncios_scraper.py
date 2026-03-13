"""
pipelines/scraping/milanuncios_scraper.py — Scraper de Milanuncios para locales en alquiler.

Milanuncios es propiedad de Adevinta. Usa React con SSR + hidratación.
Anti-bot: rate limiting moderado + User-Agent check. curl_cffi suele ser suficiente.

URL pattern: https://www.milanuncios.com/locales-comerciales-en-alquiler-en-{ciudad}/
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.milanuncios.com"
_SEARCH_URL = _BASE_URL + "/locales-comerciales-en-alquiler-en-{ciudad}/?{params}"

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
}


class MilanunciosScraper:
    """Extrae locales en alquiler de Milanuncios."""

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 5) -> list[dict]:
        ciudad_slug = _CIUDADES.get(ciudad, ciudad)
        resultados: list[dict] = []

        async with BaseScraper(self.cfg) as scraper:
            for pagina in range(1, max_paginas + 1):
                params = f"pagina={pagina}&tipo=5"  # tipo=5 → alquiler
                url = _SEARCH_URL.format(ciudad=ciudad_slug, params=params)
                referer = _SEARCH_URL.format(ciudad=ciudad_slug, params=f"pagina={pagina-1}&tipo=5") if pagina > 1 else _BASE_URL
                logger.info("Milanuncios scraping pág %d: %s", pagina, url)

                html = await scraper.get(url, referer=referer)
                if not html:
                    logger.info("Milanuncios: intentando Playwright para pág %d", pagina)
                    html = await scraper.get_with_playwright(url, wait_selector=".ma-AdCard")

                if not html:
                    logger.warning("Milanuncios: sin respuesta pág %d — abortando", pagina)
                    break

                items = _parse_milanuncios(html)
                if not items:
                    logger.info("Milanuncios: sin más resultados en pág %d", pagina)
                    break

                resultados.extend(items)
                logger.info("Milanuncios: +%d locales (total %d)", len(items), len(resultados))

        return resultados


def _parse_milanuncios(html: str) -> list[dict]:
    """Extrae listings del JSON incrustado en Milanuncios."""
    results = []

    # Método 1: __NEXT_DATA__
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            listings = _extract_ads(data)
            if listings:
                return listings
        except json.JSONDecodeError:
            pass

    # Método 2: window.INITIAL_PROPS
    match = re.search(r'window\.INITIAL_PROPS\s*=\s*({.*?});\s*</script>', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            listings = _extract_ads(data)
            if listings:
                return listings
        except json.JSONDecodeError:
            pass

    # Método 3: Regex sobre HTML
    return _parse_html_fallback(html)


def _extract_ads(data: dict) -> list[dict]:
    """Extrae anuncios del payload de Milanuncios."""
    results = []
    ads = (
        _deep_find(data, "adList")
        or _deep_find(data, "ads")
        or _deep_find(data, "items")
        or []
    )
    for ad in ads:
        if not isinstance(ad, dict):
            continue
        parsed = _parse_ad(ad)
        if parsed:
            results.append(parsed)
    return results


def _parse_ad(ad: dict) -> Optional[dict]:
    """Normaliza un anuncio de Milanuncios."""
    try:
        price = (
            ad.get("price")
            or ad.get("priceLabel", "").replace(".", "").replace("€", "").replace("/mes", "").strip()
            or ad.get("amount")
        )
        try:
            price = float(str(price).replace(".", "").replace(",", ".").strip()) if price else None
        except (ValueError, AttributeError):
            price = None

        # Milanuncios no siempre expone coordenadas; usar ciudad como fallback
        lat = ad.get("latitude") or ad.get("lat") or ad.get("geo", {}).get("latitude")
        lng = ad.get("longitude") or ad.get("lng") or ad.get("geo", {}).get("longitude")

        # Superficie
        m2_raw = ad.get("surface") or ad.get("area") or ad.get("size")
        if not m2_raw:
            # Intentar extraer del título "50 m²"
            title = ad.get("title", "")
            m2_match = re.search(r'(\d+)\s*m[²2]', title, re.IGNORECASE)
            m2_raw = m2_match.group(1) if m2_match else None
        try:
            m2 = float(str(m2_raw)) if m2_raw else None
        except (ValueError, TypeError):
            m2 = None

        ad_id = ad.get("id") or ad.get("adId") or ad.get("itemId")
        slug = ad.get("url") or ad.get("href") or ad.get("slug") or ""
        address = ad.get("location", {}).get("address", "") or ad.get("address", "")
        city = ad.get("location", {}).get("city", "") or ad.get("city", "")

        if not price:
            return None

        return {
            "id": f"milanuncios_{ad_id}" if ad_id else None,
            "fuente": "milanuncios",
            "precio": price,
            "m2": m2,
            "precio_m2": round(price / m2, 2) if price and m2 and m2 > 0 else None,
            "lat": float(lat) if lat else None,
            "lng": float(lng) if lng else None,
            "direccion": address,
            "distrito": city,
            "barrio": ad.get("neighborhood", ""),
            "url": f"{_BASE_URL}{slug}" if slug and not slug.startswith("http") else slug,
        }
    except Exception as e:
        logger.debug("Error normalizando anuncio Milanuncios: %s", e)
        return None


def _parse_html_fallback(html: str) -> list[dict]:
    """Extracción básica mediante regex como último recurso."""
    results = []
    # Buscar patrones de precio "1.200 €/mes" en el HTML
    price_blocks = re.findall(
        r'data-id="(\d+)"[^>]*>.*?<span[^>]*class="[^"]*price[^"]*"[^>]*>([\d.,]+)',
        html, re.DOTALL | re.IGNORECASE
    )
    for ad_id, price_str in price_blocks[:30]:
        try:
            price = float(price_str.replace(".", "").replace(",", "."))
            results.append({
                "id": f"milanuncios_{ad_id}",
                "fuente": "milanuncios",
                "precio": price,
                "m2": None,
                "precio_m2": None,
                "lat": None,
                "lng": None,
                "direccion": "",
                "distrito": "",
                "barrio": "",
                "url": f"{_BASE_URL}/anuncio/{ad_id}",
            })
        except (ValueError, AttributeError):
            pass
    return results


def _deep_find(obj, key: str, _depth: int = 0):
    if _depth > 6:
        return None
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list):
            return obj[key]
        for v in obj.values():
            r = _deep_find(v, key, _depth + 1)
            if r is not None:
                return r
    elif isinstance(obj, list):
        for item in obj:
            r = _deep_find(item, key, _depth + 1)
            if r is not None:
                return r
    return None
