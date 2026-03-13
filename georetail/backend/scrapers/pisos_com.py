"""
scrapers/pisos_com.py — Scraper para pisos.com (locales en alquiler, Barcelona).

pisos.com es un portal del Grupo Prisa/Vocento, ampliamente usado en España.
  - Protección: Ligera. Sin Cloudflare agresivo, sin JS obligatorio para el listing.
  - Renderizado: Parcialmente SSR (Server-Side Rendering) → httpx funciona bien.
  - Datos: Disponibles en HTML semántico Y en JSON-LD (schema.org).

URL base:
  https://www.pisos.com/alquiler/locales-barcelona/
  Paginación: ?pag={pagina}

Estrategia:
  1. httpx + JSON-LD (más limpio, estructura schema.org)
  2. httpx + BeautifulSoup HTML (fallback)
  3. Playwright si bloqueo detectado

Campos extraídos:
  id, precio, m2, titulo, direccion, barrio, lat, lng, url, planta, características

AVISO: pisos.com bloquea si superas ~30 req/hora por IP. El scheduler debe
ejecutar este scraper con delay suficiente respecto a los demás.
"""
from __future__ import annotations

import json
import logging
import re
from typing import Any

from scrapers.base import BaseScraper, LocalRaw

logger = logging.getLogger(__name__)

_BASE_URL = "https://www.pisos.com"
_URL_PLANTILLA = _BASE_URL + "/alquiler/locales-barcelona/?pag={pagina}"


class PisosComScraper(BaseScraper):
    """
    Scraper para pisos.com.

    pisos.com usa SSR parcial → el HTML inicial ya contiene los listings
    en JSON-LD. No requiere Playwright en la mayoría de casos.

    Protección:
      - User-Agent check (bloquea Python-requests, curl)
      - Rate limiting suave (~30-60 req/hora)
      - Sin Cloudflare agresivo en producción reciente

    Si httpx falla (403/429) → Playwright automático via BaseScraper.
    """

    NOMBRE = "pisos_com"
    MAX_PAGINAS = 5
    DELAY_MIN = 3.5
    DELAY_MAX = 8.0
    FORZAR_PLAYWRIGHT = False

    URL_PLANTILLA = _URL_PLANTILLA

    def _parsear_pagina(self, html: str) -> list[LocalRaw]:
        """
        Intenta parsear en este orden:
          1. JSON-LD con ItemList (schema.org)
          2. window.__INITIAL_STATE__ o similar
          3. HTML semántico (BeautifulSoup)
        """
        items = (
            self._parsear_json_ld(html)
            or self._parsear_next_data_pisos(html)
            or self._parsear_html(html)
        )
        return items

    # ── Método 1: JSON-LD (schema.org) ────────────────────────────────────────

    def _parsear_json_ld(self, html: str) -> list[LocalRaw]:
        """
        pisos.com embebe los listings como ItemList en JSON-LD.
        Es la fuente de datos más limpia y estructurada.
        """
        bloques = re.findall(
            r'<script type="application/ld\+json">(.*?)</script>', html, re.S
        )
        resultados: list[LocalRaw] = []
        for bloque in bloques:
            try:
                data = json.loads(bloque)
            except json.JSONDecodeError:
                continue

            items_raw = []
            if data.get("@type") == "ItemList":
                items_raw = [
                    item.get("item", item)
                    for item in data.get("itemListElement", [])
                ]
            elif data.get("@type") in ("Apartment", "RealEstateListing", "Product"):
                items_raw = [data]

            for item in items_raw:
                r = self._mapear_json_ld_item(item)
                if r:
                    resultados.append(r)

        return resultados

    def _mapear_json_ld_item(self, item: dict) -> LocalRaw | None:
        """Mapea un item de JSON-LD a LocalRaw."""
        url = item.get("url") or ""
        # ID desde URL (pisos.com/alquiler/local-barrio/id/)
        m = re.search(r"/(\d+)/?$", url.rstrip("/"))
        pid = m.group(1) if m else str(item.get("identifier") or "")
        if not pid:
            return None

        # Precio (Offers schema)
        precio = None
        offers = item.get("offers") or {}
        if isinstance(offers, list):
            offers = offers[0] if offers else {}
        if isinstance(offers, dict):
            precio = self._float_seguro(offers.get("price"))

        # Superficie (FloorSize)
        area = item.get("floorSize") or {}
        m2 = self._float_seguro(area.get("value") if isinstance(area, dict) else area)

        # Coordenadas
        geo = item.get("geo") or {}
        lat = self._float_seguro(geo.get("latitude"))
        lng = self._float_seguro(geo.get("longitude"))

        # Dirección
        addr = item.get("address") or {}
        if isinstance(addr, dict):
            street = addr.get("streetAddress") or ""
            locality = addr.get("addressLocality") or ""
            barrio = addr.get("addressRegion") or addr.get("neighbourhood") or ""
            direccion = f"{street}, {locality}".strip(", ")
        else:
            direccion = str(addr)
            barrio = ""

        # Planta — pisos.com la incluye a veces en additionalProperty
        planta = "PB"
        for prop in item.get("additionalProperty", []):
            if isinstance(prop, dict) and "floor" in str(prop.get("name", "")).lower():
                planta = _normalizar_planta(str(prop.get("value", "")))
                break

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"pisos_{pid}",
            titulo=item.get("name") or item.get("description") or "",
            direccion=direccion,
            barrio=barrio,
            precio=precio,
            m2=m2,
            planta=planta,
            lat=lat,
            lng=lng,
            url=url,
            caracteristicas={
                k: item.get(k)
                for k in ("numberOfRooms", "numberOfBathroomsTotal", "petsAllowed")
                if item.get(k) is not None
            },
        )

    # ── Método 2: __NEXT_DATA__ / window.__INITIAL_STATE__ ────────────────────

    def _parsear_next_data_pisos(self, html: str) -> list[LocalRaw]:
        """Extrae del JSON de Next.js si está presente."""
        data = self._extraer_next_data(html)
        if not data:
            return []

        # pisos.com Next.js paths posibles
        listings = (
            _deep_get(data, "props", "pageProps", "listings")
            or _deep_get(data, "props", "pageProps", "initialData", "properties")
            or _deep_get(data, "props", "pageProps", "properties")
            or []
        )

        resultados = []
        for l in listings:
            r = self._mapear_listing_pisos(l)
            if r:
                resultados.append(r)
        return resultados

    def _mapear_listing_pisos(self, l: dict) -> LocalRaw | None:
        """Mapea un listing de pisos.com Next.js a LocalRaw."""
        pid = str(l.get("id") or l.get("propertyCode") or "")
        if not pid:
            return None

        precio = self._float_seguro(
            l.get("price") or _deep_get(l, "priceInfo", "price")
        )
        m2 = self._float_seguro(
            l.get("size") or l.get("surface") or _deep_get(l, "features", "constructedArea")
        )
        lat = self._float_seguro(l.get("latitude") or _deep_get(l, "coordinates", "latitude"))
        lng = self._float_seguro(l.get("longitude") or _deep_get(l, "coordinates", "longitude"))

        direccion = str(l.get("address") or _deep_get(l, "location", "address") or "")
        barrio = str(
            l.get("district") or _deep_get(l, "location", "district") or
            l.get("zone") or ""
        )
        slug = l.get("url") or l.get("slug") or f"/anuncio-inmobiliario/{pid}"
        url = f"{_BASE_URL}{slug}" if slug.startswith("/") else slug

        planta = _normalizar_planta(str(l.get("floor") or _deep_get(l, "features", "floor") or ""))

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"pisos_{pid}",
            titulo=l.get("title") or "",
            direccion=direccion,
            barrio=barrio,
            precio=precio,
            m2=m2,
            planta=planta,
            lat=lat,
            lng=lng,
            url=url,
        )

    # ── Método 3: HTML semántico ──────────────────────────────────────────────

    def _parsear_html(self, html: str) -> list[LocalRaw]:
        """Fallback BeautifulSoup para pisos.com."""
        try:
            from bs4 import BeautifulSoup
        except ImportError:
            return []

        soup = BeautifulSoup(html, "lxml")

        # pisos.com usa <article class="ad-preview"> o <div class="property-card">
        cards = (
            soup.find_all("article", class_=re.compile(r"ad-preview|property", re.I))
            or soup.find_all("div", class_=re.compile(r"property-card|listing-card", re.I))
            or soup.find_all("li", class_=re.compile(r"property|ad-item", re.I))
        )

        resultados: list[LocalRaw] = []
        for card in cards:
            try:
                r = self._parsear_card_html(card)
                if r:
                    resultados.append(r)
            except Exception:
                continue
        return resultados

    def _parsear_card_html(self, card: Any) -> LocalRaw | None:
        """Extrae datos de una tarjeta HTML de pisos.com."""
        # ID
        pid = card.get("data-id") or card.get("data-property-id") or ""
        if not pid:
            enlace = card.find("a", href=re.compile(r"/(\d+)"))
            if enlace:
                m = re.search(r"/(\d+)/?$", enlace.get("href", "").rstrip("/"))
                pid = m.group(1) if m else ""
        if not pid:
            return None

        # Precio
        precio_el = card.find(class_=re.compile(r"price|precio|value", re.I))
        precio_txt = precio_el.get_text(" ", strip=True) if precio_el else ""
        precio = self._float_seguro(re.sub(r"[^\d,.]", "", precio_txt) or None)

        # Superficie
        texto = card.get_text()
        m2 = None
        m2_match = re.search(r"(\d+(?:[.,]\d+)?)\s*m²", texto)
        if m2_match:
            m2 = self._float_seguro(m2_match.group(1))

        # Título
        titulo_el = card.find(["h2", "h3", "a"], class_=re.compile(r"title|name", re.I))
        titulo = titulo_el.get_text(strip=True) if titulo_el else ""

        # Dirección
        addr_el = card.find(class_=re.compile(r"location|address|zone|zona", re.I))
        direccion = addr_el.get_text(strip=True) if addr_el else ""

        # URL
        enlace = card.find("a", href=re.compile(r"pisos\.com|/anuncio|/alquiler"))
        href = enlace.get("href", "") if enlace else ""
        url = href if href.startswith("http") else f"{_BASE_URL}{href}"

        return LocalRaw(
            portal=self.NOMBRE,
            portal_id=f"pisos_{pid}",
            titulo=titulo,
            direccion=direccion,
            precio=precio,
            m2=m2,
            url=url,
        )


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _deep_get(d: Any, *keys: Any) -> Any:
    for k in keys:
        if isinstance(d, dict):
            d = d.get(k)
        elif isinstance(d, list) and isinstance(k, int):
            try:
                d = d[k]
            except IndexError:
                return None
        else:
            return None
    return d


def _normalizar_planta(planta: str) -> str:
    planta = planta.strip().upper()
    if planta in ("0", "BAJA", "PLANTA BAJA", "PB", "BAJO", "BAIXA", "GROUND", "G"):
        return "PB"
    if "SEMI" in planta:
        return "SS"
    if "SOTA" in planta or "SOTANO" in planta or "SÓTANO" in planta:
        return "S"
    m = re.search(r"\d+", planta)
    return m.group(0) if m else "PB"
