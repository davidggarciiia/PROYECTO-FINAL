"""
pipelines/scraping/wallapop_scraper.py — Cliente de la API REST de Wallapop.

Wallapop expone una API JSON pública no oficial en api.wallapop.com/api/v3.
NO es scraping HTML: devuelve JSON directamente, sin Cloudflare, sin bloqueos.
Esta es la misma API que usa su propia app móvil.

Categoría inmobiliaria de Wallapop:
  - Locales comerciales: category_ids 200600 (locales/oficinas)
  - Operación alquiler:  transaction_type = rent

Documentación de la API (no oficial, descubierta por ingeniería inversa):
  Base: https://api.wallapop.com/api/v3/general/search
  Parámetros relevantes:
    category_ids    → 200600 (real estate / locales)
    transaction_type→ rent | sell
    latitude        → lat de Barcelona (41.3851)
    longitude       → lon de Barcelona (2.1734)
    distance        → radio en metros (20000 = toda BCN)
    start           → offset de paginación (0, 40, 80...)
    step            → items por página (40 es el máx que acepta)
    order_by        → closest | most_recent | price_low_to_high | price_high_to_low

Resultado: devuelve JSON con estructura:
  data.section.payload.items  → lista de anuncios
  data.section.payload.meta.next_start → offset para siguiente página

Anti-bloqueo: la API acepta peticiones directas con headers mínimos.
No necesita cookies, sesión, ni Playwright.
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

# ── Coordenadas del centro de Barcelona y radio para cubrir toda la ciudad ────
_BCN_LAT = 41.3851
_BCN_LON = 2.1734
_BCN_RADIUS_M = 20_000  # 20km cubre toda la ciudad y área metropolitana

# ── API de Wallapop ───────────────────────────────────────────────────────────
_API_BASE = "https://api.wallapop.com/api/v3/general/search"

# Categoría 200600 = Inmuebles > Locales y Oficinas en Wallapop
# Se puede verificar en DevTools al buscar "local comercial" en la web de Wallapop
_CATEGORY_LOCALES = 200600

# Headers mínimos que imitan la app mobile de Wallapop
_HEADERS = {
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "es-ES,es;q=0.9",
    "Accept-Encoding": "gzip, deflate, br",
    "X-AppVersion": "82600",
    "X-DeviceOS": "0",
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Referer": "https://es.wallapop.com/",
    "Origin": "https://es.wallapop.com",
}

_STEP = 40         # máximo que acepta la API por página
_MAX_OFFSET = 200  # Wallapop limita a ~200 resultados por búsqueda geográfica


class WallapopScraper:
    """
    Extrae locales en alquiler de Barcelona desde la API JSON de Wallapop.

    No usa scraping HTML. Llama directamente a api.wallapop.com/api/v3,
    la misma API que usa la app móvil de Wallapop.

    Ventajas sobre scraping HTML:
    - Sin Cloudflare, sin DataDome, sin captchas
    - JSON estructurado, sin parsing frágil
    - Coordenadas exactas por anuncio
    - Estable: la API lleva años activa

    Uso:
        scraper = WallapopScraper()
        locales = await scraper.scrape(max_items=200)
    """

    def __init__(self, delay_entre_paginas: float = 1.0):
        """
        Args:
            delay_entre_paginas: segundos entre peticiones (cortesía hacia la API)
        """
        self.delay = delay_entre_paginas

    async def scrape(
        self,
        ciudad: str = "barcelona",  # ignorado, siempre usa coordenadas BCN
        max_paginas: int = 5,
        max_items: Optional[int] = None,
    ) -> list[dict]:
        """
        Extrae locales en alquiler de Barcelona.

        Args:
            ciudad:      compatibilidad con interfaz del pipeline (ignorado)
            max_paginas: páginas de 40 items (5 págs = 200 items máx de la API)
            max_items:   límite absoluto de items (None = sin límite)

        Returns:
            Lista de dicts con formato interno de GeoRetail
        """
        resultados: list[dict] = []
        limit = max_items or (max_paginas * _STEP)
        offset = 0

        logger.info(
            "WallapopScraper: iniciando búsqueda de locales en BCN "
            "(max_items=%d, max_paginas=%d)",
            limit, max_paginas,
        )

        import httpx

        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            http2=True,
        ) as client:
            for pagina in range(1, max_paginas + 1):
                if offset >= _MAX_OFFSET:
                    logger.info("WallapopScraper: alcanzado límite de la API (%d)", _MAX_OFFSET)
                    break

                params = {
                    "category_ids":     _CATEGORY_LOCALES,
                    "transaction_type": "rent",
                    "latitude":         _BCN_LAT,
                    "longitude":        _BCN_LON,
                    "distance":         _BCN_RADIUS_M,
                    "start":            offset,
                    "step":             _STEP,
                    "order_by":         "most_recent",
                    "source":           "search_box",
                    "filters_source":   "default_filters",
                }

                try:
                    r = await client.get(_API_BASE, params=params, headers=_HEADERS)
                    if r.status_code != 200:
                        logger.warning(
                            "WallapopScraper: API devolvió %d en pág %d", r.status_code, pagina
                        )
                        break

                    data = r.json()
                    items_raw = _extraer_items(data)

                    if not items_raw:
                        logger.info("WallapopScraper: sin más items en pág %d", pagina)
                        break

                    for item in items_raw:
                        parsed = _parsear_item(item)
                        if parsed:
                            resultados.append(parsed)
                            if len(resultados) >= limit:
                                break

                    logger.info(
                        "WallapopScraper: pág %d → %d items (total %d)",
                        pagina, len(items_raw), len(resultados),
                    )

                    if len(resultados) >= limit:
                        break

                    # Siguiente offset desde la respuesta de la API
                    next_start = _extraer_next_start(data)
                    if next_start is None:
                        break
                    offset = next_start

                except Exception as exc:
                    logger.error("WallapopScraper: error en pág %d: %s", pagina, exc, exc_info=True)
                    break

                if pagina < max_paginas:
                    await asyncio.sleep(self.delay)

        logger.info("WallapopScraper: finalizó con %d locales", len(resultados))
        return resultados


# ── Parsers de la respuesta JSON ──────────────────────────────────────────────

def _extraer_items(data: dict) -> list[dict]:
    """
    Navega la estructura anidada de la respuesta de Wallapop v3.
    La estructura puede variar pero siempre tiene una lista de items.
    """
    try:
        # Estructura principal: data.section.payload.items
        items = (
            data.get("data", {})
                .get("section", {})
                .get("payload", {})
                .get("items", [])
        )
        if items:
            return items

        # Estructura alternativa: search_objects
        items = (
            data.get("search_objects", [])
            or data.get("items", [])
            or data.get("data", {}).get("items", [])
        )
        return items if isinstance(items, list) else []
    except Exception:
        return []


def _extraer_next_start(data: dict) -> Optional[int]:
    """Extrae el offset para la siguiente página desde la respuesta de la API."""
    try:
        meta = (
            data.get("data", {})
                .get("section", {})
                .get("payload", {})
                .get("meta", {})
        )
        next_start = meta.get("next_start")
        if next_start is not None:
            return int(next_start)
    except Exception:
        pass
    return None


def _parsear_item(item: dict) -> Optional[dict]:
    """
    Convierte un item de la API de Wallapop al formato interno de GeoRetail.

    La API de Wallapop v3 devuelve objetos con esta estructura:
    {
      "id": "abc123",
      "title": "Local comercial en Eixample",
      "description": "...",
      "price": {"amount": 1500, "currency": "EUR"},
      "location": {
        "city": "Barcelona",
        "postal_code": "08015",
        "approximated_latitude": 41.38,
        "approximated_longitude": 2.17,
        "neighborhood_id": "...",
        "district": "Eixample"
      },
      "user": {"id": "...", "kind": "pro"},
      "images": [...],
      "web_slug": "local-comercial-eixample-...",
      "content": {
        "extra_info": [
          {"title": "Superficie", "text": "120 m²"},
          {"title": "Tipo", "text": "Local comercial"}
        ]
      }
    }
    """
    if not item or not isinstance(item, dict):
        return None

    try:
        item_id = item.get("id") or item.get("itemId")
        if not item_id:
            return None

        # Precio
        price_obj = item.get("price", {})
        precio = None
        if isinstance(price_obj, dict):
            precio = price_obj.get("amount") or price_obj.get("value")
        elif isinstance(price_obj, (int, float)):
            precio = float(price_obj)

        # Localización
        loc = item.get("location", {})
        lat = loc.get("approximated_latitude") or loc.get("latitude")
        lon = loc.get("approximated_longitude") or loc.get("longitude")
        barrio = loc.get("neighborhood_name") or loc.get("neighborhood") or ""
        distrito = loc.get("district") or loc.get("district_name") or ""
        ciudad_local = loc.get("city") or "Barcelona"
        cp = loc.get("postal_code") or loc.get("zip_code") or ""
        direccion = loc.get("address") or loc.get("full_address") or ciudad_local

        # Filtrar anuncios fuera de Barcelona
        if lat and lon:
            # Bounding box muy generoso de Barcelona
            if not (41.2 <= float(lat) <= 41.6 and 1.9 <= float(lon) <= 2.4):
                return None

        # Superficie — está en extra_info o content.extra_info
        m2 = _extraer_superficie(item)

        # URL del anuncio
        slug = item.get("web_slug") or item.get("slug") or ""
        url = f"https://es.wallapop.com/item/{slug}" if slug else f"https://es.wallapop.com/item/{item_id}"

        # Número de fotos
        images = item.get("images", []) or item.get("pictures", [])
        num_fotos = len(images) if isinstance(images, list) else 0

        # Tipo de inmueble desde extra_info
        tipo_inmueble = _inferir_tipo_inmueble(item)

        # Si el precio es demasiado bajo (<100€) o alto (>50000€) para un local, descartar
        if precio is not None:
            if float(precio) < 100 or float(precio) > 50_000:
                return None

        return {
            "id":             f"wallapop_{item_id}",
            "fuente":         "wallapop",
            "precio":         float(precio) if precio is not None else None,
            "m2":             m2,
            "precio_m2":      round(float(precio) / m2, 2) if precio and m2 and m2 > 0 else None,
            "lat":            float(lat) if lat else None,
            "lng":            float(lon) if lon else None,
            "titulo":         item.get("title") or "",
            "direccion":      direccion,
            "barrio":         barrio,
            "distrito":       distrito,
            "codigo_postal":  str(cp) if cp else None,
            "url":            url,
            "num_fotos":      num_fotos,
            "tipo_inmueble":  tipo_inmueble,
            "tipo_operacion": "alquiler-locales",
            "es_particular":  _es_particular(item),
        }

    except Exception as exc:
        logger.debug("WallapopScraper: error parseando item %s: %s", item.get("id"), exc)
        return None


def _extraer_superficie(item: dict) -> Optional[float]:
    """Extrae los m² de los extra_info del item."""
    try:
        # Buscar en content.extra_info
        extra_info = (
            item.get("content", {}).get("extra_info", [])
            or item.get("extra_info", [])
            or item.get("attributes", [])
        )
        if isinstance(extra_info, list):
            for field in extra_info:
                if not isinstance(field, dict):
                    continue
                title = str(field.get("title") or field.get("name") or "").lower()
                text = str(field.get("text") or field.get("value") or "")
                if any(k in title for k in ("superficie", "surface", "m²", "m2", "tamaño", "size")):
                    import re
                    m = re.search(r"[\d.,]+", text.replace(".", "").replace(",", "."))
                    if m:
                        val = float(m.group())
                        if 5 < val < 10_000:  # sanity check
                            return val

        # Buscar directamente en el objeto
        for key in ("surface", "area", "size", "m2", "square_meters"):
            val = item.get(key)
            if val:
                try:
                    f = float(val)
                    if 5 < f < 10_000:
                        return f
                except (ValueError, TypeError):
                    pass
    except Exception:
        pass
    return None


def _inferir_tipo_inmueble(item: dict) -> str:
    """Infiere el tipo de inmueble desde el título y descripción."""
    texto = " ".join([
        item.get("title", ""),
        item.get("description", ""),
    ]).lower()

    if any(k in texto for k in ("oficina", "despacho", "coworking")):
        return "oficina"
    if any(k in texto for k in ("nave", "almacén", "almacen", "taller", "garaje")):
        return "nave"
    return "local"


def _es_particular(item: dict) -> bool:
    """Determina si el anunciante es particular (vs agencia/profesional)."""
    try:
        user = item.get("user", {})
        kind = user.get("kind") or user.get("type") or ""
        return str(kind).lower() in ("consumer", "particular", "private")
    except Exception:
        return False
