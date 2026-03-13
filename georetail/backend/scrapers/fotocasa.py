"""
scrapers/fotocasa.py — Scraper para Fotocasa (locales en alquiler, Barcelona).

Protección: Cloudflare + Bot Management
Estrategia: SIEMPRE Playwright + playwright-stealth (nivel 2 directo).
  - Fotocasa usa Cloudflare con JS challenge → httpx retorna 403 invariablemente.
  - El JSON de resultados está embebido en window.__PRELOADED_STATE__ (React/Redux)
    o en __NEXT_DATA__ dependiendo de la versión.
  - Alternativa: endpoint JSON interno de la API pública de Fotocasa.

URL base:
  https://www.fotocasa.es/es/alquiler/locales-comerciales/barcelona-ciudad/todas-las-zonas/l
  Paginación: ?combinedLocationIds=724%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0&page={pagina}

Campos extraídos por listing:
  id, precio, m2, titulo, direccion, barrio, lat, lng, url, planta, caracteristicas
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from scrapers.base import BaseScraper, LocalRaw

logger = logging.getLogger(__name__)

# Fotocasa también expone una API JSON interna (no documentada, pero estable)
# Usar como alternativa al scraping HTML cuando está disponible.
_API_ENDPOINT = (
    "https://www.fotocasa.es/es/alquiler/locales-comerciales/"
    "barcelona-ciudad/todas-las-zonas/l?combinedLocationIds=724%2C0%2C0%2C0%2C0%2C0%2C0%2C0%2C0"
    "&page={pagina}&pageSize=30&sortType=score&sortOrder=desc"
)

_BASE_URL = "https://www.fotocasa.es"


class FotocasaScraper(BaseScraper):
    """
    Scraper de Fotocasa.

    Anti-bot de Fotocasa:
      1. Cloudflare Managed Challenge — bloquea bots sin JS
      2. Bot Score — analiza comportamiento en página
      3. TLS fingerprint — detecta librerías HTTP no-browser
      4. Cookie CF_clearance — requerida tras pasar el challenge

    Mitigaciones aplicadas:
      - FORZAR_PLAYWRIGHT=True → siempre usa Chromium real
      - playwright-stealth → elimina señales de automatización
      - Proxy residencial opcional (SCRAPER_PROXY_URL)
      - Scroll humano antes de extraer
      - ScraperAPI como last-resort (render=true bypasses CF)
    """

    NOMBRE = "fotocasa"
    MAX_PAGINAS = 5
    DELAY_MIN = 4.0     # Fotocasa es agresivo — delays más largos
    DELAY_MAX = 9.0
    FORZAR_PLAYWRIGHT = True  # CF no se puede pasar con httpx

    URL_PLANTILLA = _API_ENDPOINT

    def _parsear_pagina(self, html: str) -> list[LocalRaw]:
        """
        Fotocasa embebe los datos en tres posibles ubicaciones:
          1. window.__PRELOADED_STATE__ (Redux store, versión antigua)
          2. <script id="__NEXT_DATA__"> (versión Next.js)
          3. HTML semántico como fallback

        Probamos en ese orden.
        """
        items = (
            self._parsear_preloaded_state(html)
            or self._parsear_next_data(html)
            or self._parsear_html_semantico(html)
        )
        return items

    # ── Método 1: window.__PRELOADED_STATE__ ──────────────────────────────────

    def _parsear_preloaded_state(self, html: str) -> list[LocalRaw]:
        """Extrae datos del Redux store embebido en el HTML."""
        m = re.search(
            r"window\.__PRELOADED_STATE__\s*=\s*({.*?})(?:;</script>|;\s*window\.)",
            html, re.S,
        )
        if not m:
            return []
        try:
            state = json.loads(m.group(1))
        except json.JSONDecodeError:
            return []

        # Ruta en el store: state.serp.listings o state.realEstates
        listings = (
            _deep_get(state, "serp", "listings")
            or _deep_get(state, "serp", "realEstates")
            or _deep_get(state, "realEstates", "list")
            or []
        )
        return [self._mapear_listing(l) for l in listings if l]

    # ── Método 2: __NEXT_DATA__ ───────────────────────────────────────────────

    def _parsear_next_data(self, html: str) -> list[LocalRaw]:
        """Extrae datos del JSON de Next.js."""
        data = self._extraer_next_data(html)
        if not data:
            return []
        listings = (
            _deep_get(data, "props", "pageProps", "initialProps", "listings", "list")
            or _deep_get(data, "props", "pageProps", "listings")
            or []
        )
        return [self._mapear_listing(l) for l in listings if l]

    # ── Método 3: HTML semántico ──────────────────────────────────────────────

    def _parsear_html_semantico(self, html: str) -> list[LocalRaw]:
        """
        Fallback: parsea el HTML con BeautifulSoup.
        Fotocasa usa data-testid o clases CSS predecibles.
        """
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "lxml")
        articulos = soup.find_all("article", attrs={"data-testid": re.compile(r"card", re.I)})
        if not articulos:
            # Intentar con clase CSS
            articulos = soup.find_all("article", class_=re.compile(r"re-Card|SearchResult", re.I))

        resultados: list[LocalRaw] = []
        for art in articulos:
            try:
                local = self._parsear_articulo_html(art)
                if local:
                    resultados.append(local)
            except Exception:
                continue
        return resultados

    def _parsear_articulo_html(self, art: Any) -> LocalRaw | None:
        """Extrae datos de un <article> individual."""
        from bs4 import Tag

        # ID desde atributo data-id o href
        pid = art.get("data-id") or art.get("id") or ""
        if not pid:
            enlace = art.find("a", href=True)
            if enlace:
                m = re.search(r"/(\d+)/?", enlace["href"])
                if m:
                    pid = m.group(1)

        if not pid:
            return None

        # Precio
        precio_el = art.find(class_=re.compile(r"price|precio", re.I))
        precio_txt = precio_el.get_text(" ", strip=True) if precio_el else ""
        precio = self._float_seguro(re.sub(r"[^\d,.]", "", precio_txt))

        # Superficie
        m2_el = art.find(string=re.compile(r"\d+\s*m²", re.I))
        m2 = None
        if m2_el:
            m2_match = re.search(r"(\d+(?:[.,]\d+)?)\s*m²", m2_el)
            if m2_match:
                m2 = self._float_seguro(m2_match.group(1))

        # Título y dirección
        titulo_el = art.find(["h2", "h3", "h4"], class_=re.compile(r"title|titulo", re.I))
        titulo = titulo_el.get_text(strip=True) if titulo_el else ""

        dir_el = art.find(class_=re.compile(r"location|address|direccion|street", re.I))
        direccion = dir_el.get_text(strip=True) if dir_el else ""

        # URL
        enlace = art.find("a", href=re.compile(r"/local|/anuncio", re.I))
        url = (_BASE_URL + enlace["href"]) if enlace else ""

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"fotocasa_{pid}",
            titulo=titulo,
            direccion=direccion,
            precio=precio,
            m2=m2,
            url=url,
        )

    # ── Mapeador de listado JSON ──────────────────────────────────────────────

    def _mapear_listing(self, l: dict) -> LocalRaw | None:
        """Convierte un objeto JSON de Fotocasa a LocalRaw."""
        if not l:
            return None

        # Precio: puede estar en 'price', 'priceInfo.price', 'rentInfo.price'
        precio = (
            _deep_get(l, "priceInfo", "price")
            or _deep_get(l, "rentInfo", "price")
            or l.get("price")
        )

        # Superficie
        m2 = (
            _deep_get(l, "features", "constructedArea")
            or _deep_get(l, "surface")
            or l.get("size")
        )

        # Coordenadas
        lat = _deep_get(l, "coordinates", "latitude") or l.get("latitude")
        lng = _deep_get(l, "coordinates", "longitude") or l.get("longitude")

        # Dirección y barrio
        direccion = (
            _deep_get(l, "address", "name")
            or l.get("address", "")
        )
        barrio = (
            _deep_get(l, "address", "district")
            or _deep_get(l, "ubication", "district")
            or ""
        )

        # ID y URL
        pid = str(l.get("id") or l.get("propertyCode") or "")
        if not pid:
            return None

        slug = l.get("slug") or l.get("url") or ""
        url = f"{_BASE_URL}{slug}" if slug.startswith("/") else slug

        # Planta
        planta_raw = (
            _deep_get(l, "features", "floor")
            or l.get("floor", "")
        )
        planta = _normalizar_planta(str(planta_raw))

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"fotocasa_{pid}",
            titulo=l.get("title") or l.get("subtitle") or "",
            direccion=str(direccion),
            barrio=str(barrio),
            precio=self._float_seguro(precio),
            m2=self._float_seguro(m2),
            planta=planta,
            lat=self._float_seguro(lat),
            lng=self._float_seguro(lng),
            url=url,
            caracteristicas={
                k: l.get(k)
                for k in ("rooms", "bathrooms", "hasLift", "hasTerrace", "hasParking")
                if l.get(k) is not None
            },
        )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _deep_get(d: dict, *keys: str) -> Any:
    """Accede a rutas anidadas de forma segura."""
    for k in keys:
        if not isinstance(d, dict):
            return None
        d = d.get(k)  # type: ignore[assignment]
    return d


def _normalizar_planta(planta: str) -> str:
    """Normaliza la planta al formato interno (PB, 1, 2, SS, etc.)."""
    planta = planta.strip().upper()
    if planta in ("0", "BAJA", "PLANTA BAJA", "PB", "BAJO", "BAIXA"):
        return "PB"
    if "SEMI" in planta or "SB" in planta:
        return "SS"
    if "SOTA" in planta or "SOTANO" in planta or "SÓTANO" in planta:
        return "S"
    m = re.search(r"\d+", planta)
    return m.group(0) if m else "PB"
