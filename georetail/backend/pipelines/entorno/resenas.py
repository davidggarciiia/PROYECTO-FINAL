"""
pipelines/resenas.py — Pipeline diario de reseñas.

Fuentes:
  1. Google Places API → reseñas de negocios_activos (máx 5/negocio)
  2. Foursquare API    → tips de negocios_activos (máx 50/negocio, más completo)
  3. Yelp Fusion API  → reseñas de negocios_activos (máx 3/negocio)

Proceso:
  1. Obtener negocios_activos actualizados (Google/FSQ/OSM) por zona
  2. Para cada negocio, obtener reseñas nuevas desde la última ejecución
  3. Guardar en tabla `resenas` (sin procesar → procesada=FALSE)
  4. Lanzar clasificación NLP + embeddings (hasta 200 reseñas por ejecución)
  5. Regenerar alertas de zonas con reseñas nuevas
"""
from __future__ import annotations
import logging
import os
from datetime import date, timedelta
import httpx
from db.conexion import get_db
from db.redis_client import get_redis

logger = logging.getLogger(__name__)
_TIMEOUT = 10.0


async def ejecutar() -> dict:
    eid = await _registrar_inicio()
    total_ok = 0
    total_err = 0

    try:
        # 1. Actualizar negocios_activos (todos los sectores, todas las zonas)
        await _actualizar_negocios_activos()

        # 2. Obtener negocios activos que necesitan actualización de reseñas
        negocios = await _negocios_a_actualizar(limite=100)
        logger.info("Actualizando reseñas de %d negocios", len(negocios))

        zonas_afectadas = set()
        for negocio in negocios:
            try:
                n = await _obtener_resenas_negocio(negocio)
                total_ok += n
                zonas_afectadas.add(negocio["zona_id"])
            except Exception as e:
                logger.warning("Error reseñas negocio %s: %s", negocio["id"], e)
                total_err += 1

        # 3. Procesar reseñas pendientes (NLP + embeddings)
        from nlp.clasificador import procesar_resenas_pendientes
        procesadas = await procesar_resenas_pendientes(limite=200)
        logger.info("Procesadas con NLP: %d reseñas", procesadas)

        # 4. Regenerar alertas de zonas afectadas
        from nlp.clasificador import generar_alertas_zona
        for zona_id in list(zonas_afectadas)[:20]:  # límite para no sobrecargar
            await generar_alertas_zona(zona_id)

        # 5. Actualizar perfiles embedding de zonas afectadas
        from nlp.embeddings import actualizar_perfil_zona
        for zona_id in list(zonas_afectadas)[:20]:
            try:
                await actualizar_perfil_zona(zona_id)
            except Exception as e:
                logger.warning("Error perfil embedding zona=%s: %s", zona_id, e)

        await _registrar_fin(eid, total_ok, "ok")
        return {"ok": total_ok, "errores": total_err, "zonas_afectadas": len(zonas_afectadas)}

    except Exception as e:
        logger.error("Pipeline resenas error crítico: %s", e, exc_info=True)
        await _registrar_fin(eid, total_ok, "error", str(e))
        raise


async def _actualizar_negocios_activos() -> None:
    """
    Actualiza la tabla negocios_activos para todas las zonas activas.
    Usa Google Places API (primario) con fallback a Foursquare y OSM.
    """
    import os
    from routers.places_router import buscar_negocios_cercanos
    _SECTORES = ["restauracion","moda","estetica","tatuajes","shisha_lounge"]

    async with get_db() as conn:
        zonas = await conn.fetch("""
            SELECT z.id AS zona_id,
                   ST_X(ST_Centroid(z.geometria)) AS lng,
                   ST_Y(ST_Centroid(z.geometria)) AS lat
            FROM zonas z
            WHERE EXISTS (SELECT 1 FROM variables_zona vz WHERE vz.zona_id=z.id
                          AND vz.fecha >= CURRENT_DATE - INTERVAL '60 days')
            ORDER BY z.id
        """)

    for zona in zonas:
        for sector in _SECTORES:
            try:
                negocios = await buscar_negocios_cercanos(
                    lat=zona["lat"], lng=zona["lng"],
                    sector=sector, radio_m=400, limite=30)
                if negocios:
                    await _upsert_negocios(negocios, zona["zona_id"])
            except Exception as e:
                logger.warning("Error actualizar negocios zona=%s sector=%s: %s",
                               zona["zona_id"], sector, e)
            # Respetar límites de API — pequeña pausa
            import asyncio
            await asyncio.sleep(0.2)


async def _upsert_negocios(negocios: list[dict], zona_id: str) -> None:
    """Inserta o actualiza negocios_activos.

    Si el dict del negocio trae `subsector_codigo`, se persiste. Si no, se
    intenta clasificar desde los campos de categoría/tipos que hayamos
    guardado en `n` (varía por fuente). Siempre es optional — el scorer
    sabe tratar subsector=NULL como "macro detectado, sub no".
    """
    from scoring.taxonomia import clasificar_con_fallback

    async with get_db() as conn:
        for n in negocios:
            if not n.get("lat") or not n.get("lng"):
                continue
            subsector = n.get("subsector_codigo")
            if subsector is None:
                # Intentamos derivar desde tipos/categorías, si la fuente las trae.
                _, subsector = clasificar_con_fallback(
                    n.get("categories") if isinstance(n.get("categories"), list) else None,
                    n.get("category") or n.get("primary_type"),
                )
            await conn.execute("""
                INSERT INTO negocios_activos
                    (id, nombre, sector_codigo, subsector_codigo, lat, lng,
                     geometria, zona_id, rating, total_resenas,
                     precio_nivel, horario, es_activo, fuente, updated_at)
                VALUES ($1,$2,$3,$4,$5,$6,
                        ST_SetSRID(ST_MakePoint($6,$5),4326),$7,$8,$9,$10,$11,TRUE,$12,NOW())
                ON CONFLICT (id) DO UPDATE SET
                    subsector_codigo=COALESCE(EXCLUDED.subsector_codigo, negocios_activos.subsector_codigo),
                    rating=EXCLUDED.rating,
                    total_resenas=EXCLUDED.total_resenas,
                    precio_nivel=COALESCE(EXCLUDED.precio_nivel, negocios_activos.precio_nivel),
                    horario=COALESCE(EXCLUDED.horario, negocios_activos.horario),
                    updated_at=NOW()
            """, n["id"], n["nombre"], n["sector_codigo"], subsector,
                n["lat"], n["lng"], zona_id,
                n.get("rating"), n.get("num_resenas"),
                n.get("precio_nivel"), n.get("horario"),
                n.get("fuente","google"))


async def _negocios_a_actualizar(limite: int) -> list[dict]:
    """Devuelve negocios cuyas reseñas no se han actualizado en > 7 días."""
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT na.id, na.nombre, na.zona_id, na.fuente,
                   na.lat, na.lng, na.sector_codigo
            FROM negocios_activos na
            WHERE na.es_activo=TRUE
              AND na.total_resenas > 0
              AND (NOT EXISTS (
                SELECT 1 FROM resenas r WHERE r.negocio_id=na.id
                AND r.fecha >= CURRENT_DATE - INTERVAL '7 days'
              ))
            ORDER BY na.updated_at ASC
            LIMIT $1
        """, limite)
    return [dict(r) for r in rows]


async def _obtener_resenas_negocio(negocio: dict) -> int:
    """Obtiene y guarda reseñas nuevas de un negocio. Devuelve número insertado."""
    fuente = negocio.get("fuente","google")
    nid = negocio["id"]
    n_insertadas = 0

    if fuente == "google" and nid.startswith("gp_"):
        resenas = await _resenas_google(nid.removeprefix("gp_"))
    elif fuente == "foursquare" and nid.startswith("fsq_"):
        resenas = await _resenas_foursquare(nid.removeprefix("fsq_"))
    else:
        return 0

    async with get_db() as conn:
        for r in resenas:
            resultado = await conn.execute("""
                INSERT INTO resenas
                    (id, negocio_id, zona_id, texto, rating, fecha, idioma, fuente, procesada)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,FALSE)
                ON CONFLICT (id) DO NOTHING
            """, r["id"], nid, negocio["zona_id"],
                r["texto"], r.get("rating"), r.get("fecha"),
                r.get("idioma","es"), fuente)
            if resultado != "INSERT 0 0":
                n_insertadas += 1

    return n_insertadas


async def _resenas_google(place_id: str) -> list[dict]:
    """Obtiene las últimas reseñas de un place de Google (máx 5)."""
    key = os.environ.get("GOOGLE_MAPS_API_KEY", "")
    if not key: return []
    async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
        r = await c.post(
            f"https://places.googleapis.com/v1/places/{place_id}",
            headers={"X-Goog-Api-Key": key,
                     "X-Goog-FieldMask": "reviews"},
            json={})
        r.raise_for_status()
        data = r.json()
    result = []
    for rev in data.get("reviews", []):
        result.append({
            "id": f"gr_{rev.get('name','').split('/')[-1]}",
            "texto": rev.get("text",{}).get("text",""),
            "rating": rev.get("rating"),
            "fecha": None,
            "idioma": rev.get("text",{}).get("languageCode","es"),
        })
    return result


async def _resenas_foursquare(fsq_id: str) -> list[dict]:
    """Obtiene los tips de un lugar en Foursquare (máx 50)."""
    key = os.environ.get("FOURSQUARE_API_KEY", "")
    if not key: return []
    async with httpx.AsyncClient(timeout=_TIMEOUT) as c:
        r = await c.get(
            f"https://api.foursquare.com/v3/places/{fsq_id}/tips",
            headers={"Authorization": key},
            params={"limit": 50, "sort": "RECENT"})
        r.raise_for_status()
        data = r.json()
    result = []
    for tip in data:
        result.append({
            "id": f"fsqt_{tip['id']}",
            "texto": tip.get("text",""),
            "rating": None,
            "fecha": tip.get("created_at","")[:10],
            "idioma": "es",
        })
    return result


# ── Control de ejecución ───────────────────────────────────────────────────────

async def _registrar_inicio():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) VALUES('resenas','running') RETURNING id")

async def _registrar_fin(eid, registros, estado, mensaje=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones SET fecha_fin=NOW(),registros=$1,estado=$2,mensaje_error=$3 WHERE id=$4",
            registros, estado, mensaje, eid)
