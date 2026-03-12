"""
routers/places_router.py — Router de búsqueda de negocios con fallback automático.

Cadena: Google Places (New API) → Foursquare → Yelp → Overpass OSM
Los resultados se normalizan a un formato común antes de guardarse en negocios_activos.
"""
from __future__ import annotations
import logging, os
from typing import Optional
import httpx
from db.redis_client import get_redis

logger = logging.getLogger(__name__)
_TIMEOUT = 10.0

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


async def buscar_negocios_cercanos(lat: float, lng: float, sector: str,
                                    radio_m: int = 500, limite: int = 50) -> list[dict]:
    """
    Busca negocios en un radio dado alrededor de (lat, lng).
    Devuelve lista normalizada de negocios con campos comunes.
    """
    r = get_redis()
    proveedores = ["google","foursquare","yelp","osm"]

    for prv in proveedores:
        if await r.get(f"places:exhausted:{prv}"):
            continue
        try:
            if prv == "google":
                return await _buscar_google(lat, lng, sector, radio_m, limite)
            if prv == "foursquare":
                return await _buscar_foursquare(lat, lng, sector, radio_m, limite)
            if prv == "yelp":
                return await _buscar_yelp(lat, lng, sector, radio_m, limite)
            if prv == "osm":
                return await _buscar_osm(lat, lng, sector, radio_m, limite)
        except _RateLimitError:
            await r.setex(f"places:exhausted:{prv}", 3600, "1")
            continue
        except Exception as e:
            logger.warning("Places router %s error: %s", prv, e)
            continue

    logger.error("Todos los proveedores de Places caídos para sector=%s", sector)
    return []


class _RateLimitError(Exception): pass


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
                                         "places.priceLevel,places.regularOpeningHours"},
            json={"includedTypes": tipos[:5], "maxResultCount": min(limite,20),
                  "locationRestriction": {"circle": {
                      "center": {"latitude": lat, "longitude": lng},
                      "radius": float(radio_m)}}})
        if r.status_code == 429: raise _RateLimitError()
        r.raise_for_status()
        data = r.json()

    negocios = []
    for p in data.get("places", []):
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
        "precio_nivel": len(b.get("price","$")),
        "sector_codigo": sector, "horario": None, "fuente": "yelp",
    } for b in data.get("businesses",[])]


async def _buscar_osm(lat, lng, sector, radio_m, limite) -> list[dict]:
    """Overpass API — último recurso, sin límite pero lento."""
    tag_map = {
        "restauracion": 'amenity~"restaurant|cafe|bar"',
        "moda":         'shop~"clothes|fashion|shoes"',
        "estetica":     'shop~"beauty|hairdresser|cosmetics"',
        "tatuajes":     'shop="tattoo"',
        "shisha_lounge":'amenity~"bar|hookah_lounge"',
    }
    tag = tag_map.get(sector,'shop="yes"')
    query = f"""
        [out:json][timeout:20];
        (node[{tag}](around:{radio_m},{lat},{lng});
         way[{tag}](around:{radio_m},{lat},{lng}););
        out body; >; out skt qt;
    """
    async with httpx.AsyncClient(timeout=25.0) as c:
        r = await c.post("https://overpass-api.de/api/interpreter", data={"data": query})
        r.raise_for_status()
        data = r.json()

    result = []
    for el in data.get("elements",[])[:limite]:
        tags = el.get("tags",{})
        result.append({
            "id": f"osm_{el['id']}",
            "nombre": tags.get("name") or tags.get("brand",""),
            "lat": el.get("lat") or el.get("center",{}).get("lat"),
            "lng": el.get("lon") or el.get("center",{}).get("lon"),
            "rating": None, "num_resenas": None, "precio_nivel": None,
            "sector_codigo": sector, "horario": None, "fuente": "osm",
        })
    return result
