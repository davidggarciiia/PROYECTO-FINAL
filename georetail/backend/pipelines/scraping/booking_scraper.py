"""
pipelines/scraping/booking_scraper.py — Scraper de hoteles Booking.com en Barcelona.

Antibot activo:
  - curl_cffi impersonate="chrome124"
  - Headers completos (Accept, Accept-Language, Referer, Sec-Fetch-*)
  - Session warming: GET booking.com → pausa → GET booking.com/es → pausa → busca
  - Cookies persistentes en archivo (BOOKING_COOKIES_FILE, default /data/booking_cookies.pkl)
  - Delay 3-6s entre requests
  - Si 429 o CAPTCHA → esperar 30-60 min (configurable con BOOKING_CAPTCHA_WAIT_MIN)

Funcionalidad:
  - URL base: https://www.booking.com/searchresults.es.html?ss=Barcelona&...
  - Extrae: nombre, dirección, lat/lng, estrellas, rating, num_reviews,
            precio_noche, tipo (hotel/hostel/apart), booking_id
  - Paginación con offset=0,25,50... hasta resultado vacío
  - Parsea JSON-LD embebido en la página primero, fallback a selectores DOM

Uso:
    from pipelines.scraping.booking_scraper import BookingScraper
    async with BookingScraper() as s:
        hoteles = await s.scrape_barcelona(max_pages=10)
"""
from __future__ import annotations

import asyncio
import json
import logging
import os
import pickle
import random
import re
from dataclasses import dataclass, field
from datetime import date, timedelta
from pathlib import Path
from typing import Optional
from urllib.parse import urlencode

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_BOOKING_BASE    = "https://www.booking.com"
_RESULTS_URL     = f"{_BOOKING_BASE}/searchresults.es.html"
_COOKIES_FILE    = Path(os.environ.get("BOOKING_COOKIES_FILE", "/data/booking_cookies.pkl"))
_DELAY_MIN       = float(os.environ.get("BOOKING_DELAY_MIN", "3.0"))
_DELAY_MAX       = float(os.environ.get("BOOKING_DELAY_MAX", "6.0"))
_CAPTCHA_WAIT_MIN = int(os.environ.get("BOOKING_CAPTCHA_WAIT_MIN", "30"))
_PAGE_SIZE       = 25  # resultados por página (Booking usa offset múltiplos de 25)
_TIMEOUT_S       = 25

# Tipos de alojamiento de interés y su normalización
_TIPO_MAP = {
    "hotel":      "hotel",
    "hostal":     "hostel",
    "hostel":     "hostel",
    "apartament": "apart",
    "apartamento": "apart",
    "apartment":  "apart",
    "pensió":     "hostel",
    "pensión":    "hostel",
}

# ── User-Agents Chrome 124 (coincide con impersonation) ───────────────────────
_USER_AGENTS_CH124 = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
]


# ═══════════════════════════════════════════════════════════════════════════════
# Modelo de datos
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class HotelBooking:
    booking_id:   str
    nombre:       str
    direccion:    str
    lat:          Optional[float]
    lng:          Optional[float]
    estrellas:    Optional[int]
    rating:       Optional[float]
    num_reviews:  Optional[int]
    precio_noche: Optional[float]
    tipo:         str   # hotel / hostel / apart
    zona_id:      Optional[str] = field(default=None)


# ═══════════════════════════════════════════════════════════════════════════════
# Scraper principal
# ═══════════════════════════════════════════════════════════════════════════════

class BookingScraper:
    """
    Scraper de hoteles Booking.com con antibot multicapa.
    Usar siempre como context manager:
        async with BookingScraper() as s:
            hoteles = await s.scrape_barcelona()
    """

    def __init__(self):
        self._ua = random.choice(_USER_AGENTS_CH124)
        self._session = None    # curl_cffi AsyncSession
        self._cookies: dict = self._cargar_cookies()

    # ── Context manager ────────────────────────────────────────────────────────

    async def __aenter__(self):
        return self

    async def __aexit__(self, *_):
        self._guardar_cookies()
        if self._session:
            try:
                await self._session.close()
            except Exception:
                pass

    # ── Método principal ───────────────────────────────────────────────────────

    async def scrape_barcelona(self, max_pages: int = 40) -> list[HotelBooking]:
        """
        Extrae hoteles de Barcelona.
        max_pages: máximo de páginas a scrapear (25 hoteles/página → 1000 max).
        """
        # Calentar sesión
        await self._session_warmup()

        hoteles: list[HotelBooking] = []
        offset = 0

        for page in range(max_pages):
            logger.info("Booking scraping página %d (offset=%d)", page + 1, offset)
            page_hotels = await self._scrape_pagina(offset)

            if not page_hotels:
                logger.info("Página %d sin resultados — fin de paginación", page + 1)
                break

            hoteles.extend(page_hotels)
            logger.info("Acumulados: %d hoteles", len(hoteles))
            offset += _PAGE_SIZE

            # Delay entre páginas
            await self._delay()

        # Guardar cookies actualizadas
        self._guardar_cookies()
        return hoteles

    # ── Warmup de sesión ───────────────────────────────────────────────────────

    async def _session_warmup(self) -> None:
        """
        Simula un usuario que navega antes de buscar.
        1. GET booking.com (homepage)
        2. Pausa 2-4s
        3. GET booking.com/es (versión española)
        4. Pausa 2-4s
        """
        logger.debug("Iniciando session warmup de Booking.com")
        await self._get(_BOOKING_BASE + "/", referer="https://www.google.com/")
        await asyncio.sleep(random.uniform(2.0, 4.0))
        await self._get(_BOOKING_BASE + "/es/", referer=_BOOKING_BASE + "/")
        await asyncio.sleep(random.uniform(2.0, 4.0))
        logger.debug("Session warmup completado")

    # ── Scraping de una página ─────────────────────────────────────────────────

    async def _scrape_pagina(self, offset: int) -> list[HotelBooking]:
        """Descarga y parsea una página de resultados de Booking.com."""
        # Fechas de check-in/out (+7 días desde hoy, 2 noches)
        checkin  = date.today() + timedelta(days=7)
        checkout = checkin + timedelta(days=2)

        params = {
            "ss":        "Barcelona",
            "ssne":      "Barcelona",
            "ssne_untouched": "Barcelona",
            "checkin":   checkin.strftime("%Y-%m-%d"),
            "checkout":  checkout.strftime("%Y-%m-%d"),
            "group_adults":   "2",
            "no_rooms":       "1",
            "group_children": "0",
            "nflt":      "ht_id%3D204",  # solo hoteles (204 = hotel)
            "offset":    str(offset),
            "rows":      str(_PAGE_SIZE),
            "lang":      "es",
            "sb_travel_purpose": "leisure",
        }
        url = f"{_RESULTS_URL}?{urlencode(params, safe='%')}"
        html = await self._get(url, referer=_BOOKING_BASE + "/es/")

        if not html:
            return []

        # Parsear: JSON-LD primero, fallback DOM
        hoteles = self._parsear_jsonld(html)
        if not hoteles:
            hoteles = self._parsear_dom(html)

        return hoteles

    # ── Parsers ────────────────────────────────────────────────────────────────

    def _parsear_jsonld(self, html: str) -> list[HotelBooking]:
        """
        Extrae datos de los bloques JSON-LD embebidos en el HTML.
        Booking embebe datos estructurados de tipo LodgingBusiness / Hotel.
        """
        hoteles: list[HotelBooking] = []
        # Buscar todos los bloques <script type="application/ld+json">
        pattern = re.compile(
            r'<script[^>]+type=["\']application/ld\+json["\'][^>]*>(.*?)</script>',
            re.DOTALL | re.IGNORECASE,
        )
        for match in pattern.finditer(html):
            try:
                data = json.loads(match.group(1))
                # Puede ser un objeto o una lista
                items = data if isinstance(data, list) else [data]
                for item in items:
                    tipo_ld = item.get("@type", "")
                    if tipo_ld not in ("Hotel", "LodgingBusiness", "Hostel", "Motel"):
                        continue
                    hotel = self._ld_a_hotel(item)
                    if hotel:
                        hoteles.append(hotel)
            except (json.JSONDecodeError, Exception):
                continue

        return hoteles

    def _ld_a_hotel(self, item: dict) -> Optional[HotelBooking]:
        """Convierte un item JSON-LD a HotelBooking."""
        try:
            nombre = item.get("name", "").strip()
            if not nombre:
                return None

            # Dirección
            addr = item.get("address") or {}
            if isinstance(addr, str):
                direccion = addr
            else:
                parts = [
                    addr.get("streetAddress", ""),
                    addr.get("addressLocality", ""),
                ]
                direccion = ", ".join(p for p in parts if p).strip()

            # Coordenadas (pueden estar en geo o en address)
            geo = item.get("geo") or {}
            lat: Optional[float] = None
            lng: Optional[float] = None
            if geo:
                try:
                    lat = float(geo.get("latitude") or 0) or None
                    lng = float(geo.get("longitude") or 0) or None
                except (ValueError, TypeError):
                    pass

            # Rating
            rating_data = item.get("aggregateRating") or {}
            rating: Optional[float] = None
            num_reviews: Optional[int] = None
            if rating_data:
                try:
                    rating     = float(rating_data.get("ratingValue") or 0) or None
                    num_reviews = int(rating_data.get("reviewCount") or 0) or None
                except (ValueError, TypeError):
                    pass

            # Estrellas
            estrellas: Optional[int] = None
            try:
                star_raw = item.get("starRating", {})
                if isinstance(star_raw, dict):
                    estrellas = int(star_raw.get("ratingValue") or 0) or None
            except (ValueError, TypeError):
                pass

            # Tipo de alojamiento
            tipo_ld = (item.get("@type") or "hotel").lower()
            tipo = _TIPO_MAP.get(tipo_ld, "hotel")

            # booking_id desde la URL (si está en url)
            url = item.get("url", "")
            booking_id = _extraer_booking_id(url, nombre)

            return HotelBooking(
                booking_id   = booking_id,
                nombre       = nombre[:300],
                direccion    = direccion[:300],
                lat          = lat,
                lng          = lng,
                estrellas    = estrellas,
                rating       = rating,
                num_reviews  = num_reviews,
                precio_noche = None,  # no disponible en JSON-LD generalmente
                tipo         = tipo,
            )
        except Exception:
            return None

    def _parsear_dom(self, html: str) -> list[HotelBooking]:
        """
        Fallback: extrae datos de la estructura DOM de Booking.com.
        Busca bloques de propiedad con atributos data- específicos.
        """
        hoteles: list[HotelBooking] = []

        # Intentar extraer bloque de datos JSON embebido en el HTML
        # Booking suele tener un bloque window.__INITIAL_STATE__ o similar
        patterns = [
            r'data-hotelid=["\'](\d+)["\'].*?data-hotel-name=["\']([^"\']+)["\']',
            r'"hotel_id"\s*:\s*(\d+).*?"hotel_name"\s*:\s*"([^"]+)"',
        ]

        seen_ids: set[str] = set()
        for pat in patterns:
            for m in re.finditer(pat, html, re.DOTALL):
                bid  = str(m.group(1))
                nom  = m.group(2).replace("\\u0027", "'").strip()[:300]
                if bid in seen_ids:
                    continue
                seen_ids.add(bid)
                hoteles.append(HotelBooking(
                    booking_id   = bid,
                    nombre       = nom,
                    direccion    = "",
                    lat          = None,
                    lng          = None,
                    estrellas    = None,
                    rating       = None,
                    num_reviews  = None,
                    precio_noche = None,
                    tipo         = "hotel",
                ))

        # Intentar extraer precios (€ por noche)
        _enriquecer_precios(html, hoteles)

        return hoteles

    # ── HTTP client con antibot ────────────────────────────────────────────────

    async def _get(self, url: str, referer: str = "") -> str:
        """
        Descarga una URL con curl_cffi (impersonate chrome124).
        Si hay 429 o detección de CAPTCHA, espera _CAPTCHA_WAIT_MIN minutos.
        """
        headers = _booking_headers(self._ua, referer)

        # Intentar con curl_cffi
        try:
            from curl_cffi.requests import AsyncSession
            async with AsyncSession(
                impersonate="chrome124",
                timeout=_TIMEOUT_S,
                cookies=self._cookies,
            ) as session:
                r = await session.get(url, headers=headers, allow_redirects=True)

                # Guardar cookies de la respuesta
                if r.cookies:
                    self._cookies.update(dict(r.cookies))

                if r.status_code == 429:
                    wait = _CAPTCHA_WAIT_MIN * 60
                    logger.warning(
                        "Booking 429 detectado — esperando %d minutos", _CAPTCHA_WAIT_MIN
                    )
                    await asyncio.sleep(wait)
                    return ""

                if r.status_code != 200:
                    logger.warning("Booking HTTP %d para %s", r.status_code, url[:80])
                    return ""

                html = r.text
                if _es_captcha(html):
                    wait = _CAPTCHA_WAIT_MIN * 60
                    logger.warning(
                        "CAPTCHA detectado en Booking — esperando %d minutos",
                        _CAPTCHA_WAIT_MIN,
                    )
                    await asyncio.sleep(wait)
                    return ""

                return html

        except ImportError:
            logger.warning("curl_cffi no disponible — usando httpx para Booking")
            return await self._get_httpx(url, referer)
        except Exception as exc:
            logger.warning("curl_cffi error Booking %s: %s", url[:80], exc)
            return await self._get_httpx(url, referer)

    async def _get_httpx(self, url: str, referer: str) -> str:
        """Fallback httpx con headers de navegador."""
        try:
            import httpx
            headers = _booking_headers(self._ua, referer)
            async with httpx.AsyncClient(
                timeout=_TIMEOUT_S,
                follow_redirects=True,
                http2=True,
            ) as client:
                r = await client.get(url, headers=headers)
                if r.status_code == 200 and not _es_captcha(r.text):
                    return r.text
                if r.status_code == 429:
                    logger.warning("Booking 429 (httpx) — esperando %d min", _CAPTCHA_WAIT_MIN)
                    await asyncio.sleep(_CAPTCHA_WAIT_MIN * 60)
        except Exception as exc:
            logger.warning("httpx error Booking: %s", exc)
        return ""

    # ── Cookies persistentes ───────────────────────────────────────────────────

    def _cargar_cookies(self) -> dict:
        if _COOKIES_FILE.exists():
            try:
                with open(_COOKIES_FILE, "rb") as f:
                    return pickle.load(f) or {}
            except Exception:
                pass
        return {}

    def _guardar_cookies(self) -> None:
        try:
            _COOKIES_FILE.parent.mkdir(parents=True, exist_ok=True)
            with open(_COOKIES_FILE, "wb") as f:
                pickle.dump(self._cookies, f)
        except Exception as exc:
            logger.debug("No se pudo guardar cookies Booking: %s", exc)

    # ── Delay ──────────────────────────────────────────────────────────────────

    async def _delay(self) -> None:
        delay = random.uniform(_DELAY_MIN, _DELAY_MAX)
        if random.random() < 0.08:  # 8% de veces: pausa extra larga
            delay *= random.uniform(2.0, 3.5)
        await asyncio.sleep(delay)


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers
# ═══════════════════════════════════════════════════════════════════════════════

def _booking_headers(ua: str, referer: str = "") -> dict:
    """Headers que imitan Chrome 124 en Booking.com."""
    h = {
        "User-Agent":                ua,
        "Accept":                    "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.7",
        "Accept-Language":           "es-ES,es;q=0.9,ca;q=0.8,en-US;q=0.7,en;q=0.6",
        "Accept-Encoding":           "gzip, deflate, br",
        "DNT":                       "1",
        "Connection":                "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest":            "document",
        "Sec-Fetch-Mode":            "navigate",
        "Sec-Fetch-Site":            "same-origin" if referer else "none",
        "Sec-Fetch-User":            "?1",
        "Sec-CH-UA":                 '"Chromium";v="124", "Google Chrome";v="124", "Not-A.Brand";v="99"',
        "Sec-CH-UA-Mobile":          "?0",
        "Sec-CH-UA-Platform":        '"Windows"',
        "Cache-Control":             "max-age=0",
    }
    if referer:
        h["Referer"] = referer
    return h


def _es_captcha(html: str) -> bool:
    """Detecta si la respuesta es una página de CAPTCHA o bloqueo."""
    indicadores = [
        "captcha",
        "robot",
        "automated access",
        "acceso automatizado",
        "cf-challenge",
        "datadome",
        "px-captcha",
        "distil_",
    ]
    html_lower = html.lower()
    return any(ind in html_lower for ind in indicadores)


def _extraer_booking_id(url: str, nombre: str) -> str:
    """Extrae el booking_id de la URL o genera uno desde el nombre."""
    if url:
        m = re.search(r"/hotel/[^/]+/([^./?#]+)", url)
        if m:
            return m.group(1)[:60]
    # Fallback: slug del nombre
    slug = re.sub(r"[^a-z0-9]+", "_", nombre.lower()).strip("_")[:60]
    return slug or "unknown"


def _enriquecer_precios(html: str, hoteles: list[HotelBooking]) -> None:
    """
    Intenta extraer precios del HTML y los asigna a los hoteles por posición.
    Los precios en Booking suelen aparecer como "€ 85" o "85 €".
    """
    precios_raw = re.findall(
        r'(?:€\s*|EUR\s*)(\d+(?:[.,]\d+)?)|(\d+(?:[.,]\d+)?)\s*(?:€|EUR)',
        html
    )
    precios: list[float] = []
    for p1, p2 in precios_raw:
        val_str = (p1 or p2).replace(",", ".")
        try:
            val = float(val_str)
            if 5 <= val <= 5000:  # rango razonable para una noche
                precios.append(val)
        except ValueError:
            pass

    # Asignar por posición (imperfecto pero útil)
    for i, hotel in enumerate(hoteles):
        if i < len(precios):
            hotel.precio_noche = precios[i]
