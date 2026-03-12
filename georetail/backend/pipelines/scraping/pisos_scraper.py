"""
pipelines/scraping/pisos_scraper.py — Scraper de Pisos.com para locales en alquiler.

Pisos.com (Grupo Anuntis) usa SSR clásico + paginación simple.
Anti-bot: mínimo. curl_cffi suele funcionar sin problemas.

URL: https://www.pisos.com/local-comercial/alquiler-barcelona/
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.pisos.com"
_SEARCH_URL = _BASE_URL + "/local-comercial/alquiler-{ciudad}/{page}/"

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
}


class PisosScraper:
    """Extrae locales en alquiler de Pisos.com."""

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 5) -> list[dict]:
        ciudad_slug = _CIUDADES.get(ciudad, ciudad)
        resultados: list[dict] = []

        async with BaseScraper(self.cfg) as scraper:
            for pagina in range(1, max_paginas + 1):
                # Pisos.com usa paginación numérica directa en la URL
                url = _SEARCH_URL.format(ciudad=ciudad_slug, page=pagina)
                referer = _SEARCH_URL.format(ciudad=ciudad_slug, page=pagina - 1) if pagina > 1 else _BASE_URL
                logger.info("Pisos.com scraping pág %d: %s", pagina, url)

                html = await scraper.get(url, referer=referer)
                if not html:
                    break

                items = _parse_pisos(html)
                if not items:
                    logger.info("Pisos.com: sin más resultados en pág %d", pagina)
                    break

                resultados.extend(items)
                logger.info("Pisos.com: +%d locales (total %d)", len(items), len(resultados))

        return resultados


def _parse_pisos(html: str) -> list[dict]:
    """Extrae listings de Pisos.com."""
    results = []

    # Método 1: JSON-LD (Pisos.com incluye schema.org en cada listing)
    matches = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
    for m in matches:
        try:
            data = json.loads(m)
            # Puede ser un objeto o una lista
            items_ld = data if isinstance(data, list) else [data]
            for item in items_ld:
                if item.get("@type") in ("Product", "Offer", "RealEstateListing"):
                    parsed = _parse_jsonld(item)
                    if parsed:
                        results.append(parsed)
        except json.JSONDecodeError:
            pass

    if results:
        return results

    # Método 2: Extraer de atributos data-* en tarjetas de anuncio
    # <article data-id="..." data-price="..." data-lat="..." data-lng="..." ...>
    card_matches = re.findall(
        r'<article[^>]+data-id="([^"]+)"[^>]*data-price="([^"]*)"[^>]*(?:data-lat="([^"]*)")?[^>]*(?:data-lng="([^"]*)")?',
        html, re.DOTALL
    )
    for ad_id, price_str, lat_str, lng_str in card_matches:
        try:
            price = float(price_str) if price_str else None
            lat = float(lat_str) if lat_str else None
            lng = float(lng_str) if lng_str else None
            if price:
                results.append({
                    "id": f"pisos_{ad_id}",
                    "fuente": "pisos",
                    "precio": price,
                    "m2": None,
                    "precio_m2": None,
                    "lat": lat,
                    "lng": lng,
                    "direccion": "",
                    "distrito": "",
                    "barrio": "",
                    "url": "",
                })
        except (ValueError, TypeError):
            pass

    if results:
        return results

    # Método 3: Meta tags og:price o micro-datos
    price_matches = re.findall(
        r'<span[^>]+(?:class|itemprop)="[^"]*(?:price|precio)[^"]*"[^>]*>([\d.,]+)',
        html, re.IGNORECASE
    )
    for i, price_str in enumerate(price_matches[:20]):
        try:
            price = float(price_str.replace(".", "").replace(",", "."))
            results.append({
                "id": f"pisos_html_{i}",
                "fuente": "pisos",
                "precio": price,
                "m2": None,
                "precio_m2": None,
                "lat": None,
                "lng": None,
                "direccion": "",
                "distrito": "",
                "barrio": "",
                "url": "",
            })
        except ValueError:
            pass

    return results


def _parse_jsonld(item: dict) -> Optional[dict]:
    try:
        # Buscar precio en distintas estructuras schema.org
        price = None
        if "offers" in item:
            offers = item["offers"]
            price = offers.get("price") if isinstance(offers, dict) else offers[0].get("price") if offers else None
        price = price or item.get("price")

        geo = item.get("geo", {})
        addr = item.get("address", {}) if isinstance(item.get("address"), dict) else {}

        # Superficie en description o floorSize
        m2 = None
        floor_size = item.get("floorSize", {})
        if isinstance(floor_size, dict):
            m2 = floor_size.get("value")
        if not m2:
            desc = item.get("description", "")
            m2_match = re.search(r'(\d+)\s*m[²2]', desc, re.IGNORECASE)
            if m2_match:
                m2 = float(m2_match.group(1))

        url = item.get("url", "")
        ad_id = item.get("identifier") or item.get("sku") or (url.split("/")[-2] if url else "")

        if not price and not geo.get("latitude"):
            return None

        return {
            "id": f"pisos_{ad_id}",
            "fuente": "pisos",
            "precio": float(price) if price else None,
            "m2": float(m2) if m2 else None,
            "precio_m2": round(float(price) / float(m2), 2) if price and m2 and float(m2) > 0 else None,
            "lat": float(geo.get("latitude")) if geo.get("latitude") else None,
            "lng": float(geo.get("longitude")) if geo.get("longitude") else None,
            "direccion": addr.get("streetAddress", ""),
            "distrito": addr.get("addressLocality", ""),
            "barrio": "",
            "url": url,
        }
    except Exception as e:
        logger.debug("Error parseando JSON-LD Pisos.com: %s", e)
        return None
