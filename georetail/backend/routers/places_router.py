"""
routers/places_router.py — Router de búsqueda de negocios con fallback automático.

Dos APIs:

  1) `buscar_negocios_cercanos(...)` — devuelve la lista de negocios de la PRIMERA
     fuente que responda. Se mantiene para retrocompatibilidad con pipelines como
     `pipelines/entorno/resenas.py` que sólo necesitan una lista.

  2) `consultar_competencia(...)` — consulta TODAS las fuentes disponibles,
     combina sus resultados con deduplicación por (nombre, lat/lng) y devuelve un
     `CompetenciaResult` con `cobertura in {"alta","media","ninguna"}`. Esta API es
     la que debe usar la dimensión de competencia del scorer para poder distinguir
     "0 competidores reales" de "no pudimos consultar".

Cadena de proveedores: Google Places (New API) → Foursquare → Yelp → Overpass OSM
"""
from __future__ import annotations

import json
import logging
import os
import re
import unicodedata
from dataclasses import dataclass, field
from datetime import date
from typing import Literal, Optional

import httpx

from db.redis_client import get_redis

logger = logging.getLogger(__name__)
_TIMEOUT = 10.0

# Si una sola fuente devuelve menos resultados que este umbral, consultamos el resto
# y combinamos. Por debajo de 2 asumimos que Google (o la primera) tiene mala
# cobertura en esa zona.
_N_MIN_RESULTADOS_SOLIDOS = 2

# Tolerancia para deduplicar: dos negocios son el mismo si sus coordenadas
# redondeadas a 4 decimales (~11 m) coinciden y sus nombres normalizados también.
_DEDUP_DECIMALES = 4

# Mapeo de tipos de negocio a categorías de cada API
_TIPOS_GOOGLE = {
    "restauracion": ["restaurant","cafe","bar","bakery","meal_takeaway"],
    "moda":         ["clothing_store","shoe_store","jewelry_store"],
    "estetica":     ["beauty_salon","hair_care","spa","nail_salon"],
    "tatuajes":     ["tattoo_parlor"],
    "shisha_lounge":["bar","night_club"],
}
_CATEGORIAS_FSQ = {
    "restauracion": "13065",  # Food & Drink
    "moda":         "17114",  # Clothing & Apparel
    "estetica":     "11031",  # Beauty Salon
    "tatuajes":     "11055",  # Tattoo Parlor
    "shisha_lounge":"13003",  # Bar
}
_TERMINOS_YELP = {
    "restauracion": "restaurants",
    "moda":         "fashion",
    "estetica":     "beautysvc",
    "tatuajes":     "tattoo",
    "shisha_lounge":"hookah_bars",
}

# Sectores válidos en BD — si el caller pide un sector desconocido, devolvemos
# `sector_codigo=None` en los resultados para no violar la FK de negocios_activos.
_VALID_SECTORES = {
    "restauracion", "moda", "estetica", "tatuajes", "shisha_lounge",
    "alimentacion", "salud", "servicios", "educacion", "deporte",
}


# ─── API de "lista simple" (retrocompat) ──────────────────────────────────────

async def buscar_negocios_cercanos(lat: float, lng: float, sector: str,
                                    radio_m: int = 500, limite: int = 50) -> list[dict]:
    """
    Devuelve los negocios de la PRIMERA fuente que responda con resultados.
    Útil para pipelines que sólo necesitan poblar `negocios_activos` (reseñas,
    listados, etc.) y que no necesitan estimar cobertura.

    Para la dimensión de scoring de competencia usa `consultar_competencia`.
    """
    # Normaliza sector_codigo para evitar FK violations al insertar en BD.
    sector_codigo = sector if sector in _VALID_SECTORES else None

    r = get_redis()
    for prv in ("google", "foursquare", "yelp", "osm"):
        if await _provider_exhausted(r, prv):
            continue
        try:
            negocios = await _llamar_proveedor(prv, lat, lng, sector, radio_m, limite)
            if negocios:
                for neg in negocios:
                    neg["sector_codigo"] = sector_codigo
                return negocios
        except _RateLimitError:
            await _marcar_rate_limited(r, prv, sector)
            continue
        except Exception as e:
            logger.warning("places_router: proveedor=%s error=%s sector=%s",
                           prv, e, sector)
            continue

    logger.error("places_router: todos los proveedores caídos — sector=%s", sector)
    return []


# ─── API de "medida con cobertura" (nueva) ────────────────────────────────────

@dataclass
class CompetenciaResult:
    """Resultado agregado de consultar la competencia en una zona.

    `cobertura` refleja cuántas fuentes independientes respondieron:
      - "alta"   → 2 o más fuentes con al menos un resultado
      - "media"  → exactamente 1 fuente
      - "ninguna"→ 0 fuentes (rate-limit, sin API key, errores)

    El scorer debe imputar `num_competidores` SÓLO cuando `cobertura != 'ninguna'`.
    """
    negocios: list[dict] = field(default_factory=list)
    fuentes_consultadas: list[str] = field(default_factory=list)
    fuentes_con_rate_limit: list[str] = field(default_factory=list)
    fuentes_con_error: list[str] = field(default_factory=list)
    cobertura: Literal["alta", "media", "ninguna"] = "ninguna"

    @property
    def num_competidores(self) -> int:
        return len(self.negocios)

    @property
    def rating_medio(self) -> Optional[float]:
        ratings = [n["rating"] for n in self.negocios if n.get("rating") is not None]
        return round(sum(ratings) / len(ratings), 2) if ratings else None

    @property
    def score_saturacion(self) -> Optional[float]:
        """Normaliza num_competidores al rango 0–100. Sólo devuelve valor si hay
        cobertura real — si no, None para que el consumidor no se engañe."""
        if self.cobertura == "ninguna":
            return None
        # 20 competidores en 300 m = saturación 100
        return round(min(100.0, self.num_competidores * 5.0), 1)


async def consultar_competencia(lat: float, lng: float, sector: str,
                                 radio_m: int = 300, limite: int = 50,
                                 zona_id: Optional[str] = None
                                 ) -> CompetenciaResult:
    """
    Consulta todas las fuentes disponibles y combina resultados con dedup.

    Fuente primaria real: el pipeline batch nocturno `competencia_scrape` (gosom).
    Esta función es el camino interactivo — click del usuario sobre una zona
    que aún no está en BD — y mira primero la caché Redis precalentada por el
    batch. Si hay miss, consulta proveedores externos con política "consulta
    lo que haga falta hasta tener resultados sólidos":
      - Si Google devuelve `>= _N_MIN_RESULTADOS_SOLIDOS` → paramos ahí.
      - Si Google devuelve 0–1 → seguimos con Foursquare/Yelp/OSM y combinamos.
      - Si Google falla → probamos el resto.

    El flag `PLACES_GOOGLE_API_ENABLED` permite desactivar la API oficial de
    Google (no la queremos quemar si ya tenemos scraper de producción).
    """
    r = get_redis()

    # Caché Redis — hit de la última ejecución batch, evita cascada de proveedores.
    if zona_id:
        cached = await _leer_cache_competencia(r, zona_id, sector, radio_m)
        if cached is not None:
            return cached

    resultado = CompetenciaResult()

    for prv in _proveedores_activos():
        if await _provider_exhausted(r, prv):
            resultado.fuentes_con_rate_limit.append(prv)
            continue

        try:
            negocios = await _llamar_proveedor(prv, lat, lng, sector, radio_m, limite)
        except _RateLimitError:
            await _marcar_rate_limited(r, prv, sector)
            resultado.fuentes_con_rate_limit.append(prv)
            continue
        except Exception as e:
            logger.warning("places_router: proveedor=%s error=%s sector=%s",
                           prv, e, sector)
            resultado.fuentes_con_error.append(prv)
            continue

        resultado.fuentes_consultadas.append(prv)
        if negocios:
            resultado.negocios = _dedup_negocios(resultado.negocios + negocios)

        # Si ya tenemos suficientes resultados sólidos, podemos cortar.
        if len(resultado.negocios) >= _N_MIN_RESULTADOS_SOLIDOS and len(resultado.fuentes_consultadas) >= 2:
            break

    fuentes_utiles = [p for p in resultado.fuentes_consultadas
                      if any(n["fuente"] == p for n in resultado.negocios)]
    if len(fuentes_utiles) >= 2:
        resultado.cobertura = "alta"
    elif len(fuentes_utiles) == 1:
        resultado.cobertura = "media"
    else:
        resultado.cobertura = "ninguna"

    if resultado.cobertura == "ninguna":
        logger.warning(
            "places_router: sin cobertura — sector=%s rate_limited=%s errores=%s",
            sector, resultado.fuentes_con_rate_limit, resultado.fuentes_con_error,
        )

    return resultado


def _proveedores_activos() -> tuple[str, ...]:
    """Devuelve la cadena de proveedores habilitada según config.

    Google API oficial sólo entra si `PLACES_GOOGLE_API_ENABLED=true`: en
    producción la fuente primaria es el scraper gosom (ver
    `pipelines/comercio/competencia_scrape.py`) y no queremos quemar cuota de la API
    oficial que ya no pagamos.
    """
    try:
        from config import get_settings
        google_on = bool(get_settings().PLACES_GOOGLE_API_ENABLED)
    except Exception:
        google_on = False
    if google_on:
        return ("google", "foursquare", "yelp", "osm")
    return ("foursquare", "yelp", "osm")


async def _leer_cache_competencia(r, zona_id: str, sector: str, radio_m: int
                                    ) -> Optional[CompetenciaResult]:
    """Devuelve un CompetenciaResult sintético si la clave de caché existe.

    No reconstruye la lista completa de negocios — sólo los agregados que el
    scorer necesita. Si un consumidor pide la lista detallada, el miss forzará
    la cascada de proveedores externos.
    """
    key = f"comp:{zona_id}:{sector}:{radio_m}"
    try:
        raw = await r.get(key)
    except Exception:
        return None
    if not raw:
        return None
    try:
        payload = json.loads(raw) if isinstance(raw, (str, bytes)) else raw
    except json.JSONDecodeError:
        return None

    cobertura = payload.get("cobertura") or "ninguna"
    if cobertura not in ("alta", "media", "ninguna"):
        return None

    # Reconstruimos n negocios "fantasma" sólo para que `num_competidores` del
    # result coincida con el valor cacheado. El consumidor real (scorer) usa
    # ese agregado directamente; no lee la lista.
    n = int(payload.get("num_competidores") or 0)
    fantasma = [{"fuente": "cache", "rating": payload.get("rating_medio")}] * n
    return CompetenciaResult(
        negocios=fantasma,
        fuentes_consultadas=["cache"],
        cobertura=cobertura,  # type: ignore[arg-type]
    )


# ─── Dispatcher interno ───────────────────────────────────────────────────────

class _RateLimitError(Exception):
    pass


async def _llamar_proveedor(prv: str, lat: float, lng: float, sector: str,
                             radio_m: int, limite: int) -> list[dict]:
    if prv == "google":     return await _buscar_google(lat, lng, sector, radio_m, limite)
    if prv == "foursquare": return await _buscar_foursquare(lat, lng, sector, radio_m, limite)
    if prv == "yelp":       return await _buscar_yelp(lat, lng, sector, radio_m, limite)
    if prv == "osm":        return await _buscar_osm(lat, lng, sector, radio_m, limite)
    raise ValueError(f"proveedor desconocido: {prv}")


async def _provider_exhausted(r, prv: str) -> bool:
    try:
        return bool(await r.get(f"places:exhausted:{prv}"))
    except Exception:
        return False


async def _marcar_rate_limited(r, prv: str, sector: str) -> None:
    """Marca el proveedor como agotado durante 1 h y contabiliza el evento."""
    try:
        await r.setex(f"places:exhausted:{prv}", 3600, "1")
        hoy = date.today().isoformat()
        await r.incr(f"places:rl:{prv}:{hoy}")
    except Exception:
        pass
    logger.warning(
        "places_rate_limit",
        extra={"proveedor": prv, "sector": sector},
    )


# ─── Dedup ────────────────────────────────────────────────────────────────────

def _normalizar_nombre(nombre: str) -> str:
    """Colapsa tildes, mayúsculas y símbolos para comparar nombres entre fuentes."""
    if not nombre:
        return ""
    sin_tildes = unicodedata.normalize("NFKD", nombre).encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "", sin_tildes.lower())


def _dedup_negocios(negocios: list[dict]) -> list[dict]:
    """Elimina duplicados por (nombre normalizado, lat redondeada, lng redondeada).
    Prefiere la entrada con más metadatos (rating presente, num_resenas más alto)."""
    agrupados: dict[tuple, dict] = {}
    for n in negocios:
        if n.get("lat") is None or n.get("lng") is None:
            continue
        clave = (
            _normalizar_nombre(n.get("nombre", "")),
            round(float(n["lat"]), _DEDUP_DECIMALES),
            round(float(n["lng"]), _DEDUP_DECIMALES),
        )
        previo = agrupados.get(clave)
        if previo is None or _es_mas_informativo(n, previo):
            agrupados[clave] = n
    return list(agrupados.values())


def _es_mas_informativo(nuevo: dict, previo: dict) -> bool:
    """Prefiere entradas con rating + más reseñas. Google > Foursquare > Yelp > OSM."""
    prioridad = {"google": 3, "foursquare": 2, "yelp": 1, "osm": 0}
    pri_n = prioridad.get(nuevo.get("fuente", ""), -1)
    pri_p = prioridad.get(previo.get("fuente", ""), -1)
    if pri_n != pri_p:
        return pri_n > pri_p
    resenas_n = nuevo.get("num_resenas") or 0
    resenas_p = previo.get("num_resenas") or 0
    return resenas_n > resenas_p


# ─── Google Places ────────────────────────────────────────────────────────────

async def _buscar_google(lat, lng, sector, radio_m, limite) -> list[dict]:
    key = os.environ.get("GOOGLE_MAPS_API_KEY","")
    if not key: raise Exception("Sin GOOGLE_MAPS_API_KEY")

    tipos = _TIPOS_GOOGLE.get(sector, ["store"])
    async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
        r = await c.post(
            "https://places.googleapis.com/v1/places:searchNearby",
            headers={"X-Goog-Api-Key": key,
                     "X-Goog-FieldMask": "places.id,places.displayName,places.location,"
                                         "places.rating,places.userRatingCount,"
                                         "places.priceLevel,places.regularOpeningHours,"
                                         "places.businessStatus"},
            json={"includedTypes": tipos, "maxResultCount": min(limite,20),
                  "locationRestriction": {"circle": {
                      "center": {"latitude": lat, "longitude": lng},
                      "radius": float(radio_m)}}})
        if r.status_code == 429: raise _RateLimitError()
        r.raise_for_status()
        data = r.json()

    negocios = []
    for p in data.get("places", []):
        # Filtrar cerrados permanentemente / temporalmente — si no lo hacemos el
        # scorer cuenta fantasmas y infla la saturación.
        if p.get("businessStatus") and p["businessStatus"] != "OPERATIONAL":
            continue
        loc = p.get("location", {})
        hor = _parsear_horario_google(p.get("regularOpeningHours", {}))
        negocios.append({
            "id": f"gp_{p['id']}",
            "nombre": p.get("displayName", {}).get("text", ""),
            "lat": loc.get("latitude"), "lng": loc.get("longitude"),
            "rating": p.get("rating"), "num_resenas": p.get("userRatingCount"),
            "precio_nivel": _google_price_level(p.get("priceLevel")),
            "sector_codigo": sector, "horario": hor, "fuente": "google",
        })
    return negocios


def _google_price_level(pl: Optional[str]) -> Optional[int]:
    mapping = {
        "PRICE_LEVEL_INEXPENSIVE": 1,
        "PRICE_LEVEL_MODERATE":    2,
        "PRICE_LEVEL_EXPENSIVE":   3,
        "PRICE_LEVEL_VERY_EXPENSIVE": 4,
    }
    return mapping.get(pl) if pl else None


def _parsear_horario_google(h: dict) -> Optional[dict]:
    if not h.get("periods"): return None
    dias = {0:"lun",1:"mar",2:"mie",3:"jue",4:"vie",5:"sab",6:"dom"}
    resultado = {v: None for v in dias.values()}
    for p in h["periods"]:
        dia_key = dias.get(p.get("open",{}).get("day"))
        if not dia_key: continue
        ab = p.get("open",{}).get("hour","?")
        ci = p.get("close",{}).get("hour","?")
        resultado[dia_key] = f"{ab}-{ci}"
    return resultado


# ─── Foursquare ───────────────────────────────────────────────────────────────

async def _buscar_foursquare(lat, lng, sector, radio_m, limite) -> list[dict]:
    key = os.environ.get("FOURSQUARE_API_KEY","")
    if not key: raise Exception("Sin FOURSQUARE_API_KEY")

    cat = _CATEGORIAS_FSQ.get(sector,"13000")
    async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
        r = await c.get(
            "https://api.foursquare.com/v3/places/search",
            headers={"Authorization": key},
            params={"ll": f"{lat},{lng}", "radius": radio_m,
                    "categories": cat, "limit": limite})
        if r.status_code == 429: raise _RateLimitError()
        r.raise_for_status()
        data = r.json()

    return [{
        "id": f"fsq_{p['fsq_id']}",
        "nombre": p.get("name",""),
        "lat": p.get("geocodes",{}).get("main",{}).get("latitude"),
        "lng": p.get("geocodes",{}).get("main",{}).get("longitude"),
        "rating": p.get("rating"), "num_resenas": p.get("stats",{}).get("total_ratings"),
        "precio_nivel": None, "sector_codigo": sector, "horario": None, "fuente": "foursquare",
    } for p in data.get("results", [])]


# ─── Yelp ─────────────────────────────────────────────────────────────────────

def _yelp_price_level(price: Optional[str]) -> Optional[int]:
    """'$'→1 … '$$$$'→4. Devuelve None si no hay dato para no mezclar con ceros."""
    if not price or not isinstance(price, str):
        return None
    n = price.count("$")
    return n if 1 <= n <= 4 else None


async def _buscar_yelp(lat, lng, sector, radio_m, limite) -> list[dict]:
    key = os.environ.get("YELP_API_KEY","")
    if not key: raise Exception("Sin YELP_API_KEY")

    termino = _TERMINOS_YELP.get(sector,"retail")
    async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
        r = await c.get(
            "https://api.yelp.com/v3/businesses/search",
            headers={"Authorization": f"Bearer {key}"},
            params={"term": termino, "latitude": lat, "longitude": lng,
                    "radius": min(radio_m,40000), "limit": min(limite,50)})
        if r.status_code == 429: raise _RateLimitError()
        r.raise_for_status()
        data = r.json()

    return [{
        "id": f"yelp_{b['id']}",
        "nombre": b.get("name",""),
        "lat": b.get("coordinates",{}).get("latitude"),
        "lng": b.get("coordinates",{}).get("longitude"),
        "rating": b.get("rating"), "num_resenas": b.get("review_count"),
        "precio_nivel": _yelp_price_level(b.get("price")),
        "sector_codigo": sector, "horario": None, "fuente": "yelp",
    } for b in data.get("businesses",[])]


# ─── Overpass / OpenStreetMap ─────────────────────────────────────────────────

async def _buscar_osm(lat, lng, sector, radio_m, limite) -> list[dict]:
    """Overpass API — último recurso, sin rate limit estricto pero lento."""
    tag_map = {
        "restauracion": 'amenity~"restaurant|cafe|bar"',
        "moda":         'shop~"clothes|fashion|shoes"',
        "estetica":     'shop~"beauty|hairdresser|cosmetics"',
        "tatuajes":     'shop="tattoo"',
        "shisha_lounge":'amenity~"bar|hookah_lounge"',
    }
    tag = tag_map.get(sector,'shop="yes"')
    # `skel qt` (no `skt`): skel = geometría mínima, qt = ordenado espacial (rápido).
    query = f"""
        [out:json][timeout:20];
        (node[{tag}](around:{radio_m},{lat},{lng});
         way[{tag}](around:{radio_m},{lat},{lng});
         relation[{tag}](around:{radio_m},{lat},{lng}););
        out center;
    """
    async with httpx.AsyncClient(timeout=25.0) as c:
        r = await c.post("https://overpass-api.de/api/interpreter", data={"data": query})
        r.raise_for_status()
        data = r.json()

    result: list[dict] = []
    for el in data.get("elements",[])[:limite]:
        tags = el.get("tags",{})
        # ways/relations vienen con `center`, nodes con lat/lon directos.
        lat_el = el.get("lat") or el.get("center",{}).get("lat")
        lon_el = el.get("lon") or el.get("center",{}).get("lon")
        if lat_el is None or lon_el is None:
            continue
        result.append({
            "id": f"osm_{el.get('type','x')}_{el['id']}",
            "nombre": tags.get("name") or tags.get("brand","") or "",
            "lat": lat_el, "lng": lon_el,
            "rating": None, "num_resenas": None, "precio_nivel": None,
            "sector_codigo": sector, "horario": None, "fuente": "osm",
        })
    return result
