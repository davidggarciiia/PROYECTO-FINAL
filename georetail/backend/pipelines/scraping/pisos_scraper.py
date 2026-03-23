"""
pipelines/scraping/pisos_scraper.py — Scraper de Pisos.com para locales en alquiler.

Pisos.com (Grupo Anuntis/Adevinta) usa SSR clásico con HTML puro.
SIN JavaScript obligatorio. SIN protección anti-bot relevante.
Es el portal más fácil de scrapear de todos los que tenemos.

URL confirmada:
  https://www.pisos.com/local-comercial/alquiler-barcelona/
  https://www.pisos.com/local-comercial/alquiler-barcelona/2/  (pág 2)
  https://www.pisos.com/local-comercial/alquiler-barcelona/3/  (pág 3)

Estructura HTML confirmada (SSR, sin JS):
  - Cada anuncio: <div class="ad-preview"> con data-id, data-price...
  - JSON-LD schema.org en <script type="application/ld+json">
  - Campos en microdata (itemprop)
  - ~20 anuncios por página

Estrategia:
  1. BeautifulSoup sobre HTML estático (método principal)
  2. JSON-LD como complemento (da m² y coordenadas)
  3. Regex como fallback final

Volumen estimado: ~1.000-1.500 locales en Barcelona.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Optional

from .base_scraper import BaseScraper, ScrapingConfig

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.pisos.com"

# URL confirmada funcionando para locales en alquiler en Barcelona
_SEARCH_URL_P1 = _BASE_URL + "/local-comercial/alquiler-{ciudad}/"
_SEARCH_URL_PN = _BASE_URL + "/local-comercial/alquiler-{ciudad}/{page}/"

_CIUDADES = {
    "barcelona": "barcelona",
    "madrid":    "madrid",
    "valencia":  "valencia",
}

# Pisos.com no necesita headers especiales, pero los ponemos para ser educados
_HEADERS_PISOS = {
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "es-ES,es;q=0.9,ca;q=0.8",
    "Accept-Encoding": "gzip, deflate, br",
    "Connection": "keep-alive",
    "Upgrade-Insecure-Requests": "1",
    "Cache-Control": "max-age=0",
}


class PisosScraper:
    """
    Extrae locales en alquiler de Pisos.com.

    Pisos.com no tiene anti-bot relevante y sirve HTML puro.
    Funciona con cualquier cliente HTTP sin necesidad de impersonation.

    Volumen típico Barcelona: ~1.000-1.500 locales disponibles.
    ~20 anuncios por página → 50-75 páginas para cubrir todo.
    Usar max_paginas=30 para cubrir ~600 anuncios de forma segura.
    """

    def __init__(self, config: Optional[ScrapingConfig] = None):
        self.cfg = config or ScrapingConfig()

    async def scrape(self, ciudad: str = "barcelona", max_paginas: int = 30) -> list[dict]:
        """
        Extrae locales en alquiler de Pisos.com.

        Args:
            ciudad:      ciudad a buscar (solo "barcelona" tiene sentido para GeoRetail)
            max_paginas: páginas a scrapear (~20 anuncios/página).
                         30 páginas ≈ 600 anuncios. 50 páginas ≈ 1.000 anuncios.

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
                logger.info("Pisos.com pág %d: %s", pagina, url)

                html = await scraper.get(url, referer=referer)
                if not html or len(html) < 1000:
                    logger.info("Pisos.com: sin respuesta en pág %d — parando", pagina)
                    break

                # Verificar que la página existe (Pisos.com devuelve 200 con "No hay resultados")
                if "no hay resultados" in html.lower() or "0 anuncios" in html.lower():
                    logger.info("Pisos.com: sin más resultados en pág %d", pagina)
                    break

                items = _parse_pisos_html(html, ciudad_slug)
                if not items:
                    logger.info("Pisos.com: sin items parseados en pág %d", pagina)
                    break

                nuevos = 0
                for item in items:
                    if item.get("id") and item["id"] not in ids_vistos:
                        ids_vistos.add(item["id"])
                        resultados.append(item)
                        nuevos += 1

                logger.info(
                    "Pisos.com: pág %d → %d nuevos (total %d)",
                    pagina, nuevos, len(resultados),
                )

                if nuevos == 0:
                    logger.info("Pisos.com: todos duplicados — parando en pág %d", pagina)
                    break

        logger.info("Pisos.com: total %d locales extraídos", len(resultados))
        return resultados


# ── Parsers ───────────────────────────────────────────────────────────────────

def _parse_pisos_html(html: str, ciudad: str = "barcelona") -> list[dict]:
    """
    Estrategia multi-capa para extraer listings de Pisos.com.
    Prioriza BeautifulSoup sobre HTML estático (lo más fiable).
    """
    results = []

    # ── Método 1: BeautifulSoup sobre HTML estático (PRINCIPAL) ───────────────
    try:
        from bs4 import BeautifulSoup
        soup = BeautifulSoup(html, "html.parser")

        # Pisos.com usa <div class="ad-preview"> o <article class="ad-preview">
        # Selector probado: tarjetas de anuncio en listado
        cards = (
            soup.find_all("div", class_=re.compile(r"ad[-_]preview|property[-_]card|listing[-_]item", re.I))
            or soup.find_all("article", class_=re.compile(r"ad|property|listing", re.I))
            or soup.find_all(attrs={"data-id": True})  # cualquier elemento con data-id
        )

        for card in cards:
            parsed = _parse_bs4_card(card)
            if parsed:
                results.append(parsed)

        if results:
            return results
    except ImportError:
        logger.debug("BeautifulSoup no disponible, usando regex")
    except Exception as e:
        logger.debug("Error BeautifulSoup en Pisos.com: %s", e)

    # ── Método 2: JSON-LD (schema.org) ─────────────────────────────────────────
    matches = re.findall(
        r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.IGNORECASE,
    )
    for raw in matches:
        try:
            data = json.loads(raw.strip())
            # Puede ser un objeto o array
            items = data if isinstance(data, list) else [data]
            for item in items:
                if isinstance(item, dict) and item.get("@type") in (
                    "Product", "Offer", "RealEstateListing", "House", "Apartment",
                ):
                    parsed = _parse_jsonld(item)
                    if parsed:
                        results.append(parsed)
        except (json.JSONDecodeError, Exception):
            pass

    if results:
        return results

    # ── Método 3: data-attributes en HTML ─────────────────────────────────────
    # <article data-id="..." data-price="..." ...>
    card_attrs = re.finditer(
        r'data-id=["\']([^"\']+)["\'][^>]*data-price=["\']([^"\']*)["\']'
        r'(?:[^>]*data-lat=["\']([^"\']*)["\'])?'
        r'(?:[^>]*data-lng=["\']([^"\']*)["\'])?'
        r'(?:[^>]*data-surface=["\']([^"\']*)["\'])?',
        html, re.DOTALL,
    )
    for m in card_attrs:
        ad_id, price_str, lat_str, lng_str, surface_str = m.groups()
        try:
            price = float(price_str.replace(".", "").replace(",", ".")) if price_str else None
            lat = float(lat_str) if lat_str else None
            lng = float(lng_str) if lng_str else None
            m2 = float(surface_str) if surface_str else None
            if price:
                results.append({
                    "id":       f"pisos_{ad_id}",
                    "fuente":   "pisos",
                    "precio":   price,
                    "m2":       m2,
                    "precio_m2": round(price / m2, 2) if price and m2 else None,
                    "lat":      lat,
                    "lng":      lng,
                    "direccion": "",
                    "barrio":   "",
                    "distrito": "",
                    "url":      "",
                })
        except (ValueError, TypeError):
            pass

    if results:
        return results

    # ── Método 4: Regex de último recurso ─────────────────────────────────────
    return _parse_regex_fallback(html)


def _parse_bs4_card(card) -> Optional[dict]:
    """Extrae datos de una tarjeta de anuncio usando BeautifulSoup."""
    try:
        # ID del anuncio
        ad_id = (
            card.get("data-id")
            or card.get("id", "").replace("ad-", "").replace("property-", "")
        )
        if not ad_id:
            return None

        # Precio — buscar span/div con clase que contenga "price" o "precio"
        price_el = (
            card.find(attrs={"class": re.compile(r"price|precio", re.I)})
            or card.find("span", itemprop="price")
        )
        precio = None
        if price_el:
            price_text = price_el.get_text(strip=True)
            # Limpiar: "1.200 €/mes" → 1200.0
            nums = re.findall(r"[\d.,]+", price_text.replace(".", "").replace(",", "."))
            if nums:
                try:
                    precio = float(nums[0])
                except ValueError:
                    pass

        # Superficie
        m2_el = card.find(attrs={"class": re.compile(r"surface|superficie|area|size", re.I)})
        m2 = None
        if m2_el:
            m2_text = m2_el.get_text(strip=True)
            m2_match = re.search(r"([\d.,]+)", m2_text)
            if m2_match:
                try:
                    m2 = float(m2_match.group(1).replace(".", "").replace(",", "."))
                except ValueError:
                    pass

        # Si no hay m² en el elemento, buscar en el texto general de la tarjeta
        if not m2:
            card_text = card.get_text()
            m2_match = re.search(r"(\d+)\s*m[²2]", card_text, re.IGNORECASE)
            if m2_match:
                try:
                    m2 = float(m2_match.group(1))
                    if m2 > 10_000:  # sanity check
                        m2 = None
                except ValueError:
                    pass

        # URL
        link_el = card.find("a", href=True)
        url = ""
        if link_el:
            href = link_el["href"]
            url = href if href.startswith("http") else f"https://www.pisos.com{href}"

        # Dirección / título
        title_el = (
            card.find(attrs={"class": re.compile(r"title|titulo|heading|name", re.I)})
            or card.find("h2")
            or card.find("h3")
        )
        titulo = title_el.get_text(strip=True) if title_el else ""

        # Zona / barrio (pisos.com lo muestra como "Eixample, Barcelona")
        zone_el = card.find(attrs={"class": re.compile(r"location|zona|zone|address|barrio", re.I)})
        zona_text = zone_el.get_text(strip=True) if zone_el else ""
        partes = [p.strip() for p in zona_text.split(",")]
        barrio = partes[0] if partes else ""
        distrito = partes[1] if len(partes) > 1 else ""

        # Coordenadas desde data-attributes
        lat = card.get("data-lat") or card.get("data-latitude")
        lng = card.get("data-lng") or card.get("data-longitude")

        if not precio and not url:
            return None

        return {
            "id":        f"pisos_{ad_id}",
            "fuente":    "pisos",
            "titulo":    titulo,
            "precio":    precio,
            "m2":        m2,
            "precio_m2": round(precio / m2, 2) if precio and m2 and m2 > 0 else None,
            "lat":       float(lat) if lat else None,
            "lng":       float(lng) if lng else None,
            "direccion": zona_text,
            "barrio":    barrio,
            "distrito":  distrito,
            "url":       url,
        }
    except Exception as e:
        logger.debug("Error parseando card Pisos.com: %s", e)
        return None


def _parse_jsonld(item: dict) -> Optional[dict]:
    """Extrae datos de un item JSON-LD de Pisos.com."""
    try:
        # Precio
        price = None
        offers = item.get("offers")
        if isinstance(offers, dict):
            price = offers.get("price")
        elif isinstance(offers, list) and offers:
            price = offers[0].get("price")
        price = price or item.get("price")

        # Geo
        geo = item.get("geo", {}) or {}
        lat = geo.get("latitude")
        lng = geo.get("longitude")

        # Dirección
        addr = item.get("address", {}) or {}
        if isinstance(addr, str):
            direccion = addr
            barrio = ""
            distrito = ""
        else:
            direccion = addr.get("streetAddress", "")
            barrio = addr.get("addressLocality", "")
            distrito = addr.get("addressRegion", "")

        # Superficie
        m2 = None
        floor_size = item.get("floorSize", {})
        if isinstance(floor_size, dict):
            m2 = floor_size.get("value")
        elif isinstance(floor_size, (int, float)):
            m2 = float(floor_size)

        if not m2:
            desc = item.get("description", "") or item.get("name", "")
            m2_match = re.search(r"(\d+)\s*m[²2]", str(desc), re.IGNORECASE)
            if m2_match:
                m2 = float(m2_match.group(1))

        # URL e ID
        url = item.get("url", "")
        ad_id = (
            item.get("identifier")
            or item.get("sku")
            or item.get("productID")
            or (re.search(r"/(\d+)/?$", url).group(1) if url and re.search(r"/(\d+)/?$", url) else "")
        )

        if not price and not lat:
            return None

        return {
            "id":        f"pisos_{ad_id}" if ad_id else None,
            "fuente":    "pisos",
            "titulo":    item.get("name", ""),
            "precio":    float(price) if price else None,
            "m2":        float(m2) if m2 else None,
            "precio_m2": round(float(price) / float(m2), 2) if price and m2 and float(m2) > 0 else None,
            "lat":       float(lat) if lat else None,
            "lng":       float(lng) if lng else None,
            "direccion": direccion,
            "barrio":    barrio,
            "distrito":  distrito,
            "url":       url,
        }
    except Exception as e:
        logger.debug("Error parseando JSON-LD Pisos.com: %s", e)
        return None


def _parse_regex_fallback(html: str) -> list[dict]:
    """Extracción mínima de último recurso para Pisos.com."""
    results = []

    # Buscar bloques de anuncios con ID y precio
    blocks = re.finditer(
        r'href=["\'](/anuncio[^"\']*)["\'][^>]*>[^<]*<[^>]*>[^<]*'
        r'(?:([0-9.,]+)\s*€)',
        html, re.DOTALL,
    )
    for i, m in enumerate(blocks):
        try:
            url_path, price_str = m.group(1), m.group(2)
            price = float(price_str.replace(".", "").replace(",", "."))
            # Extraer ID de la URL
            id_match = re.search(r"/(\d+)/?", url_path)
            ad_id = id_match.group(1) if id_match else str(i)

            if 100 <= price <= 50_000:
                results.append({
                    "id":        f"pisos_{ad_id}",
                    "fuente":    "pisos",
                    "precio":    price,
                    "m2":        None,
                    "precio_m2": None,
                    "lat":       None,
                    "lng":       None,
                    "direccion": "",
                    "barrio":    "",
                    "distrito":  "",
                    "url":       f"https://www.pisos.com{url_path}",
                })
        except (ValueError, IndexError):
            pass

    return results[:50]  # limitar para no introducir basura
