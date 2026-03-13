"""
pipelines/scraping/idealista_scraper.py — Scraper de Idealista para locales en alquiler.

Reemplaza la integración con la API oficial (requería acceso de pago + aprobación).

Idealista usa Incapsula (Imperva) como anti-bot.
Estrategia:
  1. curl_cffi chrome131 — suele bypassar Incapsula básico
  2. Si responde con challenge JS → ScrapingBee/ZenRows
  3. Los datos están en window.initClientApp (JSON embedded en el HTML)

URL pattern:
  https://www.idealista.com/alquiler-locales/barcelona-barcelona/?shape=...&ordenado-por=precios-asc
  https://www.idealista.com/alquiler-locales/madrid-madrid/
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.idealista.com"
_SEARCH_URLS = {
    "barcelona": _BASE_URL + "/alquiler-locales/barcelona-barcelona/",
    "madrid":    _BASE_URL + "/alquiler-locales/madrid-madrid/",
    "valencia":  _BASE_URL + "/alquiler-locales/valencia-valencia/",
}


class IdealistaScraper:
    """
    Extrae locales en alquiler de Idealista.com.

    Reemplaza la API oficial (requería aprobación + plan de pago).
    Usa curl_cffi para impersonar Chrome y bypassar Incapsula.

    Uso:
        scraper = IdealistaScraper()
        locales = await scraper.scrape(ciudad="barcelona", max_paginas=5)
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 5) -> list[dict]:
        """Devuelve lista de dicts con datos de locales en alquiler."""
        base_url = _SEARCH_URLS.get(ciudad)
        if not base_url:
            base_url = f"{_BASE_URL}/alquiler-locales/{ciudad}-{ciudad}/"

        resultados: list[dict] = []

        async with BaseScraper(self.cfg) as scraper:
            for pagina in range(1, max_paginas + 1):
                url = base_url if pagina == 1 else f"{base_url}pagina-{pagina}.htm"
                referer = base_url if pagina > 1 else _BASE_URL
                logger.info("Idealista scraping pág %d: %s", pagina, url)

                html = await scraper.get(url, referer=referer)

                if not html:
                    logger.info("Idealista: intentando Playwright para pág %d", pagina)
                    html = await scraper.get_with_playwright(
                        url, wait_selector=".item-list"
                    )

                if not html:
                    logger.warning("Idealista: sin respuesta pág %d — abortando", pagina)
                    break

                # Detectar Cloudflare / Incapsula challenge
                if _es_challenge(html):
                    logger.warning(
                        "Idealista: challenge anti-bot detectado. "
                        "Configura SCRAPING_SERVICE=scrapingbee o =zenrows para bypassarlo."
                    )
                    break

                items = _parse_idealista(html)
                if not items:
                    logger.info("Idealista: sin más resultados en pág %d", pagina)
                    break

                resultados.extend(items)
                logger.info("Idealista: +%d locales (total %d)", len(items), len(resultados))

        return resultados


def _es_challenge(html: str) -> bool:
    """Detecta si la respuesta es un challenge de Incapsula/Cloudflare."""
    challenge_signals = [
        "/_Incapsula_Resource",
        "visitorId",
        "cf-challenge",
        "Checking if the site connection is secure",
        "Please Wait... | Cloudflare",
        "incapsula",
        "window._cf_chl_opt",
    ]
    html_lower = html.lower()
    return any(s.lower() in html_lower for s in challenge_signals) and len(html) < 50000


def _parse_idealista(html: str) -> list[dict]:
    """
    Extrae datos del JSON incrustado en el HTML de Idealista.
    Idealista inyecta los datos en window.initClientApp o en atributos data-*.
    """
    # Método 1: window.initClientApp (estructura principal de Idealista)
    match = re.search(r'window\.initClientApp\s*=\s*({.*?});\s*(?:</script>|window\.)', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            items = _extract_from_init_client_app(data)
            if items:
                return items
        except json.JSONDecodeError:
            pass

    # Método 2: JSON en __NEXT_DATA__ (versión React de Idealista)
    match = re.search(r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>', html, re.DOTALL)
    if match:
        try:
            data = json.loads(match.group(1))
            items = _extract_from_next_data(data)
            if items:
                return items
        except json.JSONDecodeError:
            pass

    # Método 3: atributos data-* en cards de anuncios
    items = _extract_from_data_attributes(html)
    if items:
        return items

    # Método 4: JSON-LD schema.org
    matches = re.findall(r'<script type="application/ld\+json">(.*?)</script>', html, re.DOTALL)
    items = []
    for m in matches:
        try:
            data = json.loads(m)
            parsed = _parse_jsonld(data)
            if parsed:
                items.append(parsed)
        except json.JSONDecodeError:
            pass

    return items


def _extract_from_init_client_app(data: dict) -> list[dict]:
    """Extrae listings de window.initClientApp de Idealista."""
    results = []
    try:
        # Paths conocidos en la estructura de Idealista
        listings = (
            data.get("listingItems", {}).get("items", [])
            or data.get("items", [])
            or _deep_find_list(data, "items")
            or []
        )
        for item in listings:
            parsed = _normalize_idealista_item(item)
            if parsed:
                results.append(parsed)
    except Exception as e:
        logger.debug("Error extrayendo initClientApp Idealista: %s", e)
    return results


def _extract_from_next_data(data: dict) -> list[dict]:
    """Extrae listings del __NEXT_DATA__ de la versión React de Idealista."""
    results = []
    try:
        props = data.get("props", {}).get("pageProps", {})
        listings = (
            props.get("listingsFromSearch", {}).get("items", [])
            or props.get("items", [])
            or _deep_find_list(data, "items")
            or []
        )
        for item in listings:
            parsed = _normalize_idealista_item(item)
            if parsed:
                results.append(parsed)
    except Exception as e:
        logger.debug("Error extrayendo __NEXT_DATA__ Idealista: %s", e)
    return results


def _extract_from_data_attributes(html: str) -> list[dict]:
    """
    Extrae datos de los atributos data-* de las cards de Idealista.
    Cada <article class="item" data-id="..." data-price="..." ...>
    """
    results = []

    # Pattern principal: article con data-id y data-price
    pattern = re.compile(
        r'<article[^>]+data-element-id="([^"]+)"[^>]*>',
        re.DOTALL | re.IGNORECASE
    )

    for match in pattern.finditer(html):
        article_start = match.start()
        # Extraer hasta el siguiente </article>
        article_end = html.find("</article>", article_start)
        if article_end == -1:
            continue
        article_html = html[article_start:article_end + 10]

        try:
            price = _extract_attr(article_html, "data-price") or _extract_price_from_html(article_html)
            m2 = _extract_attr(article_html, "data-size") or _extract_m2_from_html(article_html)
            ad_id = _extract_attr(article_html, "data-element-id") or _extract_attr(article_html, "data-id")
            lat = _extract_attr(article_html, "data-latitude")
            lng = _extract_attr(article_html, "data-longitude")
            address = _extract_attr(article_html, "data-address") or _extract_address_from_html(article_html)

            if price or lat:
                results.append({
                    "id": f"idealista_{ad_id}" if ad_id else None,
                    "fuente": "idealista",
                    "precio": float(str(price).replace(".", "").replace(",", ".")) if price else None,
                    "m2": float(str(m2).replace(",", ".")) if m2 else None,
                    "precio_m2": None,
                    "lat": float(lat) if lat else None,
                    "lng": float(lng) if lng else None,
                    "direccion": address or "",
                    "distrito": "",
                    "barrio": "",
                    "url": _extract_url_from_html(article_html),
                })
        except (ValueError, TypeError) as e:
            logger.debug("Error parseando article Idealista: %s", e)

    return results


def _normalize_idealista_item(item: dict) -> Optional[dict]:
    """Normaliza un item de Idealista a formato interno."""
    try:
        price = (
            item.get("price")
            or item.get("priceInfo", {}).get("amount")
            or item.get("suggestedTexts", {}).get("rawPrice")
        )
        lat = item.get("latitude") or item.get("ubication", {}).get("latitude")
        lng = item.get("longitude") or item.get("ubication", {}).get("longitude")
        m2 = item.get("size") or item.get("floor")
        prop_id = item.get("itemId") or item.get("propertyCode") or item.get("id")
        address = item.get("address") or item.get("detail", {}).get("address", "")
        district = item.get("district") or ""
        neighborhood = item.get("neighborhood") or ""
        url_path = item.get("url") or item.get("canonicalUrl") or ""

        if not price and not lat:
            return None

        precio = None
        try:
            precio = float(str(price).replace(".", "").replace(",", ".").replace("€", "").strip())
        except (ValueError, TypeError, AttributeError):
            pass

        return {
            "id": f"idealista_{prop_id}" if prop_id else None,
            "fuente": "idealista",
            "precio": precio,
            "m2": float(m2) if m2 else None,
            "precio_m2": round(precio / float(m2), 2) if precio and m2 and float(m2) > 0 else None,
            "lat": float(lat) if lat else None,
            "lng": float(lng) if lng else None,
            "direccion": address,
            "distrito": district,
            "barrio": neighborhood,
            "url": f"{_BASE_URL}{url_path}" if url_path and not url_path.startswith("http") else url_path,
        }
    except Exception as e:
        logger.debug("Error normalizando item Idealista: %s", e)
        return None


def _parse_jsonld(data: dict) -> Optional[dict]:
    try:
        price = data.get("offers", {}).get("price") or data.get("price")
        geo = data.get("geo", {})
        addr = data.get("address", {}) if isinstance(data.get("address"), dict) else {}
        if not price and not geo.get("latitude"):
            return None
        return {
            "id": f"idealista_{data.get('identifier', '')}",
            "fuente": "idealista",
            "precio": float(price) if price else None,
            "m2": None,
            "precio_m2": None,
            "lat": float(geo.get("latitude")) if geo.get("latitude") else None,
            "lng": float(geo.get("longitude")) if geo.get("longitude") else None,
            "direccion": addr.get("streetAddress", ""),
            "distrito": addr.get("addressLocality", ""),
            "barrio": "",
            "url": data.get("url", ""),
        }
    except Exception:
        return None


# ── Helpers de extracción HTML ─────────────────────────────────────────────────

def _extract_attr(html: str, attr: str) -> Optional[str]:
    match = re.search(rf'{attr}="([^"]+)"', html, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_price_from_html(html: str) -> Optional[str]:
    # "1.200 €/mes" o "1200€" o "1.200 euros"
    match = re.search(r'([\d.]+(?:,\d+)?)\s*(?:€|euros?)(?:/mes)?', html, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_m2_from_html(html: str) -> Optional[str]:
    match = re.search(r'(\d+(?:[.,]\d+)?)\s*m[²2]', html, re.IGNORECASE)
    return match.group(1) if match else None


def _extract_address_from_html(html: str) -> str:
    match = re.search(r'<span[^>]+class="[^"]*item-detail-location[^"]*"[^>]*>([^<]+)<', html, re.IGNORECASE)
    return match.group(1).strip() if match else ""


def _extract_url_from_html(html: str) -> str:
    match = re.search(r'href="(/inmueble/[^"]+)"', html, re.IGNORECASE)
    if match:
        return f"{_BASE_URL}{match.group(1)}"
    return ""


def _deep_find_list(obj, key: str, _depth: int = 0):
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
