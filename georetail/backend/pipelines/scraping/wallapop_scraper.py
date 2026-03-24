"""
pipelines/scraping/wallapop_scraper.py — Cliente de la API REST de Wallapop.

CORRECCIONES respecto a versión anterior:
  - Categoría correcta: 200 (Real Estate), NO 200600 (que no existe)
  - Endpoint correcto para inmuebles: /api/v3/general/search con keywords
  - La sección de inmuebles en Wallapop usa vertical_id="real_estate"
  - Para filtrar locales en alquiler usamos keywords + tipo_operacion en atributos
  - Estructura de respuesta: data.search_objects[] (no data.section.payload.items)

Verificado inspeccionando https://api.wallapop.com/api/v3/categories:
  category id=200, name="Real Estate", vertical_id="real_estate"
"""
from __future__ import annotations

import asyncio
import logging
from typing import Optional

logger = logging.getLogger(__name__)

_BCN_LAT  = 41.3851
_BCN_LON  = 2.1734
_BCN_RADIUS_M = 40_000  # 40km — cubre toda el área metropolitana

# Endpoint de búsqueda general
_API_SEARCH  = "https://api.wallapop.com/api/v3/general/search"
# Endpoint específico de inmuebles (vertical real_estate)
_API_RE      = "https://api.wallapop.com/api/v3/real_estate/search"

_CATEGORY_RE = 200   # Real Estate (confirmado desde /api/v3/categories)
_STEP        = 40    # máximo por página

# Headers que imitan la app de Wallapop (mobile)
_HEADERS = {
    "Accept":           "application/json, text/plain, */*",
    "Accept-Language":  "es-ES,es;q=0.9",
    "Accept-Encoding":  "gzip, deflate, br",
    "X-AppVersion":     "82600",
    "X-DeviceOS":       "0",
    "DeviceOSVersion":  "13",
    "User-Agent": (
        "Mozilla/5.0 (Linux; Android 13; Pixel 7) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Mobile Safari/537.36"
    ),
    "Referer": "https://es.wallapop.com/",
    "Origin":  "https://es.wallapop.com",
}

# Keywords para buscar locales comerciales en Wallapop
# Wallapop no tiene subcategorías para tipo de inmueble — usamos keywords
_KEYWORDS_LOCALES = [
    "local comercial alquiler",
    "local en alquiler barcelona",
    "local comercial barcelona",
    "traspaso local",
]


class WallapopScraper:
    """
    Extrae locales en alquiler de Barcelona desde la API JSON de Wallapop.

    Usa category_ids=200 (Real Estate) con keywords para filtrar locales.
    No necesita scraping HTML, proxies, ni Playwright.
    """

    def __init__(self, delay_entre_paginas: float = 1.2):
        self.delay = delay_entre_paginas

    async def scrape(
        self,
        ciudad: str = "barcelona",  # ignorado, usa coordenadas fijas
        max_paginas: int = 5,
        max_items: Optional[int] = None,
    ) -> list[dict]:
        """
        Extrae locales en alquiler de Barcelona.
        Lanza varias búsquedas por keywords para maximizar cobertura.
        """
        todos: dict[str, dict] = {}  # deduplicar por ID

        import httpx
        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            http2=True,
        ) as client:
            for keyword in _KEYWORDS_LOCALES:
                resultados = await self._buscar_keyword(
                    client, keyword, max_paginas
                )
                for item in resultados:
                    if item.get("id") and item["id"] not in todos:
                        todos[item["id"]] = item

                logger.info(
                    "Wallapop '%s': %d items (total acumulado: %d)",
                    keyword, len(resultados), len(todos),
                )
                await asyncio.sleep(self.delay)

        resultado = list(todos.values())
        if max_items:
            resultado = resultado[:max_items]

        logger.info("WallapopScraper: total %d locales únicos", len(resultado))
        return resultado

    async def _buscar_keyword(
        self, client, keyword: str, max_paginas: int
    ) -> list[dict]:
        """Busca una keyword concreta paginando hasta max_paginas."""
        resultados: list[dict] = []
        start = 0

        for pagina in range(1, max_paginas + 1):
            params = {
                "keywords":     keyword,
                "category_ids": _CATEGORY_RE,
                "latitude":     _BCN_LAT,
                "longitude":    _BCN_LON,
                "distance":     _BCN_RADIUS_M,
                "start":        start,
                "step":         _STEP,
                "order_by":     "most_recent",
                "source":       "search_box",
                "filters_source": "default_filters",
            }

            try:
                r = await client.get(_API_SEARCH, params=params, headers=_HEADERS)
                logger.debug("Wallapop '%s' pág %d → status %d", keyword, pagina, r.status_code)

                if r.status_code != 200:
                    logger.warning("Wallapop: status %d para '%s'", r.status_code, keyword)
                    break

                data = r.json()
                items_raw = _extraer_items(data)

                if not items_raw:
                    break

                for item in items_raw:
                    parsed = _parsear_item(item)
                    if parsed:
                        resultados.append(parsed)

                # Siguiente página
                next_start = _extraer_next_start(data)
                if next_start is None or next_start == start:
                    break
                start = next_start

            except Exception as exc:
                logger.error("Wallapop error en '%s' pág %d: %s", keyword, pagina, exc)
                break

            if pagina < max_paginas:
                await asyncio.sleep(self.delay)

        return resultados


# ── Helpers de parseo ─────────────────────────────────────────────────────────

def _extraer_items(data: dict) -> list[dict]:
    """
    Extrae la lista de items de la respuesta.
    Wallapop v3 usa varias estructuras según el endpoint.
    """
    # Estructura más común en /general/search
    items = (
        data.get("search_objects")                                    # list directo
        or data.get("data", {}).get("search_objects")
        or data.get("data", {})
              .get("section", {})
              .get("payload", {})
              .get("items", [])
        or data.get("items", [])
    )
    return items if isinstance(items, list) else []


def _extraer_next_start(data: dict) -> Optional[int]:
    """Extrae el offset para la siguiente página."""
    try:
        # Buscar en varios paths
        for path in [
            lambda d: d.get("meta", {}).get("next_start"),
            lambda d: d.get("data", {}).get("meta", {}).get("next_start"),
            lambda d: d.get("data", {}).get("section", {}).get("payload", {}).get("meta", {}).get("next_start"),
        ]:
            val = path(data)
            if val is not None:
                return int(val)
    except Exception:
        pass
    return None


def _parsear_item(item: dict) -> Optional[dict]:
    """
    Convierte un item de la API de Wallapop al formato interno.

    Estructura de un item de Real Estate en Wallapop v3:
    {
      "id": "abc123",
      "title": "Local comercial en Eixample",
      "description": "...",
      "category_id": "200",
      "price": {"amount": 1500, "currency": "EUR"},
      "location": {
        "city": "Barcelona",
        "postal_code": "08015",
        "approximated_latitude": 41.38,
        "approximated_longitude": 2.17,
        "district": "Eixample"
      },
      "type_attributes": {
        "operation": "rent",      ← alquiler
        "type": "office_space",   ← tipo de inmueble
        "surface": "120",
      },
      "web_slug": "local-comercial-eixample-...",
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
        elif isinstance(price_obj, (int, float, str)):
            try:
                precio = float(price_obj)
            except (ValueError, TypeError):
                pass

        # Localización
        loc = item.get("location", {}) or {}
        lat = loc.get("approximated_latitude") or loc.get("latitude")
        lon = loc.get("approximated_longitude") or loc.get("longitude")

        # Filtrar fuera de Barcelona (bounding box generoso)
        if lat and lon:
            if not (41.1 <= float(lat) <= 41.7 and 1.8 <= float(lon) <= 2.5):
                return None

        # Atributos del inmueble (tipo, operación, superficie)
        attrs = item.get("type_attributes", {}) or item.get("attributes", {}) or {}
        operacion = attrs.get("operation", "")  # "rent" | "sell"
        tipo_raw  = attrs.get("type", "")       # "office_space" | "commercial" | ...
        superficie = attrs.get("surface")

        # Solo queremos alquiler — si está especificado y es venta, saltar
        if operacion and operacion not in ("rent", "alquiler", ""):
            return None

        # Inferir tipo de inmueble
        tipo_inmueble = _inferir_tipo(tipo_raw, item.get("title", ""), item.get("description", ""))

        # Superficie
        m2 = None
        if superficie:
            try:
                m2 = float(str(superficie).replace(",", "."))
                if m2 < 5 or m2 > 20_000:
                    m2 = None
            except (ValueError, TypeError):
                pass

        # URL
        slug = item.get("web_slug") or item.get("slug") or ""
        url  = (
            f"https://es.wallapop.com/item/{slug}"
            if slug else
            f"https://es.wallapop.com/item/{item_id}"
        )

        # Imágenes
        images   = item.get("images") or item.get("pictures") or []
        num_fotos = len(images) if isinstance(images, list) else 0

        # Filtro de precio razonable para locales
        if precio is not None:
            try:
                p = float(precio)
                if p < 100 or p > 60_000:
                    return None
            except (ValueError, TypeError):
                pass

        barrio   = loc.get("neighborhood_name") or loc.get("neighborhood") or ""
        distrito = loc.get("district") or loc.get("district_name") or ""
        cp       = loc.get("postal_code") or loc.get("zip_code") or ""

        return {
            "id":             f"wallapop_{item_id}",
            "fuente":         "wallapop",
            "titulo":         item.get("title") or "",
            "precio":         float(precio) if precio is not None else None,
            "m2":             m2,
            "precio_m2":      round(float(precio) / m2, 2) if precio and m2 else None,
            "lat":            float(lat) if lat else None,
            "lng":            float(lon) if lon else None,
            "direccion":      loc.get("address") or loc.get("city") or "Barcelona",
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
        logger.debug("Wallapop: error parseando item %s: %s", item.get("id"), exc)
        return None


def _inferir_tipo(tipo_attr: str, titulo: str, descripcion: str) -> str:
    """Infiere el tipo de inmueble comercial."""
    texto = f"{tipo_attr} {titulo} {descripcion}".lower()
    if any(k in texto for k in ("oficina", "office", "despacho", "coworking")):
        return "oficina"
    if any(k in texto for k in ("nave", "warehouse", "almacén", "almacen", "taller")):
        return "nave"
    return "local"


def _es_particular(item: dict) -> bool:
    """Determina si el anunciante es particular."""
    try:
        user = item.get("user", {}) or {}
        kind = user.get("kind") or user.get("type") or ""
        return str(kind).lower() in ("consumer", "particular", "private")
    except Exception:
        return False
