"""
routers/geocoding_router.py — Geocodificación con caché y fallback.

Cadena: Caché PG → Google Geocoding → Nominatim → OpenCage
"""
from __future__ import annotations
import hashlib, logging, os
from typing import Optional
import httpx
from db.conexion import get_db
from db.redis_client import get_redis

logger = logging.getLogger(__name__)
_TIMEOUT = 8.0


async def geocodificar(direccion: str) -> Optional[tuple[float, float, str]]:
    """
    Devuelve (lat, lng, precision) o None si no se puede geocodificar.
    precision: 'rooftop' | 'street' | 'zone'
    """
    norm = _normalizar(direccion)

    # 1. Caché PostgreSQL
    cached = await _get_cache(norm)
    if cached:
        return cached

    r = get_redis()
    proveedores = ["google","nominatim","opencage"]
    for prv in proveedores:
        if await r.get(f"geo:exhausted:{prv}"):
            continue
        try:
            result = await _llamar(prv, norm)
            if result:
                await _guardar_cache(norm, result[0], result[1], result[2], prv)
                return result
        except Exception as e:
            logger.warning("Geocoding %s error: %s", prv, e)
            continue

    return None


def _normalizar(addr: str) -> str:
    return addr.strip().lower()


async def _get_cache(norm: str) -> Optional[tuple[float, float, str]]:
    try:
        async with get_db() as conn:
            row = await conn.fetchrow(
                "SELECT lat, lng, nivel_precision FROM cache_geocoding WHERE direccion_normalizada=$1", norm)
        return (row["lat"], row["lng"], row["nivel_precision"]) if row else None
    except Exception:
        return None


async def _guardar_cache(norm: str, lat: float, lng: float,
                         precision: str, fuente: str) -> None:
    try:
        async with get_db() as conn:
            await conn.execute(
                "INSERT INTO cache_geocoding(direccion_normalizada,lat,lng,nivel_precision,fuente)"
                " VALUES($1,$2,$3,$4,$5) ON CONFLICT DO NOTHING",
                norm, lat, lng, precision, fuente)
    except Exception:
        pass


async def _llamar(prv: str, addr: str) -> Optional[tuple[float, float, str]]:
    if prv == "google":
        key = os.environ.get("GOOGLE_MAPS_API_KEY","")
        if not key: return None
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.get("https://maps.googleapis.com/maps/api/geocode/json",
                            params={"address": addr+", Barcelona", "key": key})
            r.raise_for_status()
            data = r.json()
        if data.get("results"):
            loc = data["results"][0]["geometry"]["location"]
            lt = data["results"][0]["geometry"]["location_type"]
            prec = "rooftop" if lt=="ROOFTOP" else "street" if "RANGE" in lt else "zone"
            return loc["lat"], loc["lng"], prec
        return None

    if prv == "nominatim":
        async with httpx.AsyncClient(timeout=_TIMEOUT,
                headers={"User-Agent":"GeoRetail/1.0 contact@georetail.es"}) as c:
            r = await c.get("https://nominatim.openstreetmap.org/search",
                            params={"q": addr+", Barcelona", "format":"json",
                                    "limit":1, "countrycodes":"es"})
            r.raise_for_status()
            data = r.json()
        if data:
            return float(data[0]["lat"]), float(data[0]["lon"]), "street"
        return None

    if prv == "opencage":
        key = os.environ.get("OPENCAGE_API_KEY","")
        if not key: return None
        async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
            r = await c.get("https://api.opencagedata.com/geocode/v1/json",
                            params={"q": addr+", Barcelona", "key": key,
                                    "limit":1, "countrycode":"es"})
            r.raise_for_status()
            data = r.json()
        if data.get("results"):
            g = data["results"][0]["geometry"]
            return g["lat"], g["lng"], "street"
        return None

    return None
