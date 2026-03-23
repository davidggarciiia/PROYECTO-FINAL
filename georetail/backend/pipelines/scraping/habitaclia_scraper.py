"""
pipelines/scraping/habitaclia_scraper.py — Scraper de Habitaclia para locales en alquiler.

Habitaclia (Grupo Schibsted) usa Next.js con SSR.
Anti-bot: Cloudflare básico. curl_cffi con chrome131 funciona.

URLs confirmadas (estructura real, verificada en navegador):
  Página 1: https://www.habitaclia.com/alquiler-locales_comerciales-barcelona.htm
  Página 2: https://www.habitaclia.com/alquiler-locales_comerciales-barcelona-2.htm
  Página N: https://www.habitaclia.com/alquiler-locales_comerciales-barcelona-N.htm

Datos que expone Habitaclia con SSR:
  - JSON incrustado en window.__INITIAL_REDUX_STATE__
  - Metadatos en JSON-LD schema.org
  - Microdata (itemprop)

Volumen estimado: ~1.500-2.000 locales en Barcelona.
~30 anuncios por página → 50-65 páginas para cubrir todo.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.habitaclia.com"

# URLs verificadas en el navegador (estructura real de Habitaclia)
_SEARCH_URL_P1 = _BASE_URL + "/alquiler-locales_comerciales-{ciudad}.htm"
_SEARCH_URL_PN = _BASE_URL + "/alquiler-locales_comerciales-{ciudad}-{page}.htm"

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
}


class HabitacliaScraper:
    """
    Extrae locales en alquiler de Habitaclia.

    Habitaclia tiene Cloudflare básico que curl_cffi maneja sin problemas.
    Si curl_cffi falla en alguna página concreta, se reintenta automáticamente.

    Volumen típico Barcelona: ~1.500-2.000 locales.
    ~30 anuncios/página → usar max_paginas=30 para ~900 anuncios.
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 30) -> list[dict]:
        """
        Extrae locales en alquiler de Habitaclia.

        Args:
            ciudad:      ciudad a buscar
            max_paginas: páginas a scrapear (~30 anuncios/página)

        Returns:
            Lista de dicts con datos normalizados.
        """
        ciudad_slug = _CIUDADES.get(ciudad, ciudad)
        resultados: list[dict] = []
        ids_vistos: set[str] = set()

        async with BaseScraper(self.cfg) as scraper:
            for pagina in range(1, max_paginas + 1):
                url = (
                    _SEARCH_URL_P1.format(ciudad=ciudad_slug)
                    if pagina == 1
                    else _SEARCH_URL_PN.format(ciudad=ciudad_slug, page=pagina)
                )
                referer = (
                    _SEARCH_URL_P1.format(ciudad=ciudad_slug)
                    if pagina > 1
                    else _BASE_URL
                )
                logger.info("Habitaclia pág %d: %s", pagina, url)

                html = await scraper.get(url, referer=referer)

                if not html or len(html) < 2000:
                    logger.warning("Habitaclia: sin HTML en pág %d — parando", pagina)
                    break

                # Detectar página vacía o fin de resultados
                if _es_pagina_vacia(html):
                    logger.info("Habitaclia: fin de resultados en pág %d", pagina)
                    break

                items = _parse_habitaclia_html(html)
                if not items:
                    logger.info("Habitaclia: sin items parseados en pág %d", pagina)
                    break

                nuevos = 0
                for item in items:
                    if item.get("id") and item["id"] not in ids_vistos:
                        ids_vistos.add(item["id"])
                        resultados.append(item)
                        nuevos += 1

                logger.info(
                    "Habitaclia: pág %d → %d nuevos (total %d)",
                    pagina, nuevos, len(resultados),
                )

                if nuevos == 0:
                    logger.info("Habitaclia: todos duplicados — parando en pág %d", pagina)
                    break

        logger.info("Habitaclia: total %d locales extraídos", len(resultados))
        return resultados


# ── Parsers ───────────────────────────────────────────────────────────────────

def _es_pagina_vacia(html: str) -> bool:
    """Detecta si Habitaclia devolvió una página sin resultados."""
    lower = html.lower()
    return any([
        "no s'han trobat" in lower,
        "no se han encontrado" in lower,
        "0 anuncios" in lower,
        "sin resultados" in lower,
        # Paginación fuera de rango: Habitaclia redirige a pág 1 o da 404
        'rel="canonical"' in lower and 'page=' not in lower,
    ])


def _parse_habitaclia_html(html: str) -> list[dict]:
    """
    Extrae listings de Habitaclia con múltiples estrategias.
    """
    results = []

    # ── Método 1: window.__INITIAL_REDUX_STATE__ (el más rico) ────────────────
    # Habitaclia inyecta el estado completo de Redux en el HTML
    patterns_redux = [
        r'window\.__INITIAL_REDUX_STATE__\s*=\s*({.*?});\s*</script>',
        r'window\.__REDUX_STATE__\s*=\s*({.*?});\s*</script>',
        r'window\.__STORE_STATE__\s*=\s*({.*?});\s*</script>',
    ]
    for pattern in patterns_redux:
        match = re.search(pattern, html, re.DOTALL)
        if match:
            try:
                data = json.loads(match.group(1))
                items = _extraer_de_redux(data)
                if items:
                    return items
            except (json.JSONDecodeError, Exception) as e:
                logger.debug("Error parseando Redux Habitaclia: %s", e)

    # ── Método 2: __NEXT_DATA__ (si usa Next.js) ───────────────────────────────
    match = re.search(
        r'<script id="__NEXT_DATA__"[^>]*>(.*?)</script>',
        html, re.DOTALL,
    )
    if match:
        try:
            data = json.loads(match.group(1))
            items = _extraer_de_next_data(data)
            if items:
                return items
        except (json.JSONDecodeError, Exception) as e:
            logger.debug("Error parseando __NEXT_DATA__ Habitaclia: %s", e)

    # ── Método 3: JSON-LD schema.org ───────────────────────────────────────────
    jsonld_matches = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    )
    for raw in jsonld_matches:
        try:
            data = json.loads(raw.strip())
            items_ld = data if isinstance(data, list) else [data]
            for item in items_ld:
                if not isinstance(item, dict):
                    continue
                t = item.get("@type", "")
                if t in ("Product", "RealEstateListing", "House", "Apartment") or \
                   "real" in t.lower():
                    parsed = _parse_jsonld_item(item)
                    if parsed:
                        results.append(parsed)
        except (json.JSONDecodeError, Exception):
            pass

    if results:
        return results

    # ── Método 4: BeautifulSoup sobre HTML ────────────────────────────────────
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Habitaclia: cada anuncio en <article> o <div> con clase específica
        cards = (
            soup.find_all("article", class_=re.compile(r"list-item|property|result", re.I))
            or soup.find_all("div", class_=re.compile(r"list-item|property-card|ad-item", re.I))
            or soup.find_all(attrs={"data-id": True})
        )

        for card in cards:
            parsed = _parse_bs4_card(card)
            if parsed:
                results.append(parsed)
    except ImportError:
        pass
    except Exception as e:
        logger.debug("Error BeautifulSoup Habitaclia: %s", e)

    if results:
        return results

    # ── Método 5: Regex sobre microdata ───────────────────────────────────────
    return _parse_microdata_regex(html)


def _extraer_de_redux(data: dict) -> list[dict]:
    """Extrae listings del estado Redux de Habitaclia."""
    results = []
    try:
        # Posibles paths en el árbol Redux de Habitaclia
        listings = (
            _deep_find_list(data, "listings")
            or _deep_find_list(data, "results")
            or _deep_find_list(data, "items")
            or _deep_find_list(data, "properties")
            or _deep_find_list(data, "ads")
            or []
        )
        for item in listings:
            parsed = _parse_habitaclia_item(item)
            if parsed:
                results.append(parsed)
    except Exception as e:
        logger.debug("Error extrayendo de Redux: %s", e)
    return results


def _extraer_de_next_data(data: dict) -> list[dict]:
    """Extrae listings del __NEXT_DATA__ de Habitaclia."""
    results = []
    try:
        props = data.get("props", {}).get("pageProps", {})
        listings = (
            _deep_find_list(props, "listings")
            or _deep_find_list(props, "results")
            or _deep_find_list(props, "properties")
            or _deep_find_list(data, "listings")
            or []
        )
        for item in listings:
            parsed = _parse_habitaclia_item(item)
            if parsed:
                results.append(parsed)
    except Exception as e:
        logger.debug("Error extrayendo de __NEXT_DATA__: %s", e)
    return results


def _parse_habitaclia_item(item: dict) -> Optional[dict]:
    """Normaliza un item del JSON interno de Habitaclia."""
    try:
        if not isinstance(item, dict):
            return None

        # Precio — varios campos posibles
        price = (
            item.get("price")
            or item.get("priceMin")
            or item.get("monthlyCost")
            or item.get("rent")
            or item.get("amount")
            or (item.get("priceInfo", {}) or {}).get("amount")
        )
        if price is None:
            return None
        try:
            price = float(str(price).replace(".", "").replace(",", ".").replace("€", "").strip())
        except (ValueError, TypeError):
            return None

        # Filtro de precio razonable para locales comerciales
        if not (100 <= price <= 50_000):
            return None

        # ID
        prop_id = (
            item.get("id")
            or item.get("propertyId")
            or item.get("code")
            or item.get("listingId")
        )
        if not prop_id:
            return None

        # Coordenadas
        lat = (
            item.get("lat") or item.get("latitude")
            or (item.get("geo", {}) or {}).get("lat")
            or (item.get("location", {}) or {}).get("lat")
            or (item.get("coordinates", {}) or {}).get("latitude")
        )
        lng = (
            item.get("lng") or item.get("longitude")
            or (item.get("geo", {}) or {}).get("lng")
            or (item.get("location", {}) or {}).get("lng")
            or (item.get("coordinates", {}) or {}).get("longitude")
        )

        # Superficie
        m2 = (
            item.get("area") or item.get("surface")
            or item.get("size") or item.get("squareMeters")
            or item.get("usableArea") or item.get("builtArea")
        )
        try:
            m2 = float(m2) if m2 else None
            if m2 and (m2 < 5 or m2 > 10_000):
                m2 = None
        except (ValueError, TypeError):
            m2 = None

        # Localización
        location = item.get("location", {}) or item.get("address", {}) or {}
        if isinstance(location, str):
            address = location
            barrio = ""
            distrito = ""
        else:
            address = (
                location.get("address") or location.get("fullAddress")
                or location.get("street") or item.get("address") or ""
            )
            barrio = (
                location.get("neighborhood") or location.get("zone")
                or item.get("neighborhood") or item.get("zone") or ""
            )
            distrito = (
                location.get("district") or location.get("city")
                or item.get("district") or ""
            )

        # URL
        url_path = item.get("url") or item.get("href") or item.get("slug") or ""
        url = (
            f"{_BASE_URL}{url_path}"
            if url_path and not url_path.startswith("http")
            else url_path
        )
        if not url:
            url = f"{_BASE_URL}/anuncio/{prop_id}.htm"

        return {
            "id":        f"habitaclia_{prop_id}",
            "fuente":    "habitaclia",
            "titulo":    item.get("title") or item.get("name") or "",
            "precio":    price,
            "m2":        m2,
            "precio_m2": round(price / m2, 2) if m2 and m2 > 0 else None,
            "lat":       float(lat) if lat else None,
            "lng":       float(lng) if lng else None,
            "direccion": address,
            "barrio":    barrio,
            "distrito":  distrito,
            "url":       url,
        }
    except Exception as e:
        logger.debug("Error normalizando item Habitaclia: %s", e)
        return None


def _parse_jsonld_item(item: dict) -> Optional[dict]:
    """Parsea un item JSON-LD de Habitaclia."""
    try:
        price = (
            (item.get("offers", {}) or {}).get("price")
            or item.get("price")
        )
        geo = item.get("geo", {}) or {}
        addr = item.get("address", {}) or {}
        url = item.get("url", "")
        prop_id = item.get("identifier") or (url.split("/")[-1].split(".")[0] if url else "")

        return {
            "id":        f"habitaclia_{prop_id}" if prop_id else None,
            "fuente":    "habitaclia",
            "titulo":    item.get("name", ""),
            "precio":    float(price) if price else None,
            "m2":        None,
            "precio_m2": None,
            "lat":       float(geo.get("latitude")) if geo.get("latitude") else None,
            "lng":       float(geo.get("longitude")) if geo.get("longitude") else None,
            "direccion": addr.get("streetAddress", "") if isinstance(addr, dict) else str(addr),
            "barrio":    "",
            "distrito":  addr.get("addressLocality", "") if isinstance(addr, dict) else "",
            "url":       url,
        }
    except Exception:
        return None


def _parse_bs4_card(card) -> Optional[dict]:
    """Extrae datos de una tarjeta HTML de Habitaclia con BeautifulSoup."""
    try:
        ad_id = card.get("data-id") or card.get("id", "").replace("property-", "")
        if not ad_id:
            return None

        price_el = card.find(class_=re.compile(r"price|precio|cost", re.I))
        precio = None
        if price_el:
            nums = re.findall(r"[\d.]+", price_el.get_text().replace(".", ""))
            if nums:
                try:
                    precio = float(nums[0])
                except ValueError:
                    pass

        m2_el = card.find(class_=re.compile(r"surface|surface|m2|area", re.I))
        m2 = None
        if m2_el:
            m = re.search(r"(\d+)", m2_el.get_text())
            if m:
                m2 = float(m.group(1))

        link = card.find("a", href=True)
        url = ""
        if link:
            h = link["href"]
            url = h if h.startswith("http") else f"{_BASE_URL}{h}"

        if not precio and not url:
            return None

        return {
            "id":        f"habitaclia_{ad_id}",
            "fuente":    "habitaclia",
            "titulo":    "",
            "precio":    precio,
            "m2":        m2,
            "precio_m2": round(precio / m2, 2) if precio and m2 else None,
            "lat":       None,
            "lng":       None,
            "direccion": "",
            "barrio":    "",
            "distrito":  "",
            "url":       url,
        }
    except Exception:
        return None


def _parse_microdata_regex(html: str) -> list[dict]:
    """Última opción: regex sobre microdata/itemprop de Habitaclia."""
    results = []
    # Buscar precios con itemprop="price"
    price_blocks = re.finditer(
        r'itemprop="price"[^>]*content="([^"]+)"',
        html, re.IGNORECASE,
    )
    prices = [m.group(1) for m in price_blocks]

    urls = re.findall(r'href="(/anuncio[^"]+\.htm)"', html)
    ids = [re.search(r"/(\d+)\.htm", u).group(1) for u in urls if re.search(r"/(\d+)\.htm", u)]

    for i, (price_str, ad_id) in enumerate(zip(prices[:50], ids[:50])):
        try:
            price = float(price_str.replace(".", "").replace(",", "."))
            if 100 <= price <= 50_000:
                results.append({
                    "id":        f"habitaclia_{ad_id}",
                    "fuente":    "habitaclia",
                    "precio":    price,
                    "m2":        None,
                    "precio_m2": None,
                    "lat":       None,
                    "lng":       None,
                    "direccion": "",
                    "barrio":    "",
                    "distrito":  "",
                    "url":       f"{_BASE_URL}/anuncio/{ad_id}.htm",
                })
        except (ValueError, AttributeError):
            pass
    return results


# ── Utils ─────────────────────────────────────────────────────────────────────

def _deep_find_list(obj, key: str, _depth: int = 0):
    """Búsqueda recursiva de una clave cuyo valor es una lista no vacía."""
    if _depth > 7:
        return None
    if isinstance(obj, dict):
        if key in obj and isinstance(obj[key], list) and len(obj[key]) > 0:
            return obj[key]
        for v in obj.values():
            r = _deep_find_list(v, key, _depth + 1)
            if r:
                return r
    elif isinstance(obj, list):
        for item in obj[:5]:  # no profundizar en listas largas
            r = _deep_find_list(item, key, _depth + 1)
            if r:
                return r
    return None
