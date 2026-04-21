"""db/sesiones.py — CRUD de sesiones (Redis primario + PG backup)."""
from __future__ import annotations
import json, logging, re
from datetime import datetime, timezone
from typing import Optional
from redis.exceptions import WatchError
from db.conexion import get_db
from db.redis_client import get_redis

logger = logging.getLogger(__name__)
_TTL = 86400  # 24h
_UUID_RE = re.compile(
    r'^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$', re.IGNORECASE
)


def _valid_session_id(session_id: str) -> bool:
    return bool(session_id and _UUID_RE.match(session_id))


async def crear_sesion(session_id: str, datos: dict, ip_hash: str) -> dict:
    if not _valid_session_id(session_id):
        raise ValueError(f"session_id inválido: {session_id!r}")
    sesion = {"session_id": session_id, "ip_hash": ip_hash,
               "created_at": datetime.now(timezone.utc).isoformat(),
               "perfil": {}, "zonas_actuales": [], **datos}
    r = get_redis()
    await r.setex(f"sesion:{session_id}", _TTL, json.dumps(sesion, ensure_ascii=False))
    try:
        async with get_db() as conn:
            # `perfil` es JSONB: pasamos el dict directamente. El codec de asyncpg
            # registrado en db/conexion.py hace json.dumps una sola vez. Si pasáramos
            # un string JSON, el codec lo re-encodaría y la columna quedaría con un
            # string con backslashes en lugar de un objeto.
            await conn.execute(
                "INSERT INTO sesiones (id, ip_hash, perfil) VALUES ($1,$2,$3) ON CONFLICT DO NOTHING",
                session_id, ip_hash, sesion.get("perfil", {}))
    except Exception as e:
        logger.warning("PG sesion write fail: %s", e)
    return sesion


async def get_sesion(session_id: str) -> Optional[dict]:
    if not _valid_session_id(session_id):
        return None
    r = get_redis()
    raw = await r.get(f"sesion:{session_id}")
    if raw:
        await r.expire(f"sesion:{session_id}", _TTL)
        return json.loads(raw)
    try:
        async with get_db() as conn:
            row = await conn.fetchrow("SELECT id, ip_hash, perfil FROM sesiones WHERE id=$1", session_id)
        if row:
            s = {"session_id": row["id"], "ip_hash": row["ip_hash"] or "", "perfil": row["perfil"] or {}, "zonas_actuales": []}
            await r.setex(f"sesion:{session_id}", _TTL, json.dumps(s, ensure_ascii=False))
            return s
    except Exception as e:
        logger.warning("PG sesion read fail: %s", e)
    return None


async def actualizar_sesion(session_id: str, updates: dict) -> None:
    if not _valid_session_id(session_id):
        return
    key = f"sesion:{session_id}"
    r = get_redis()
    # Actualización atómica con WATCH/MULTI para evitar pérdida de datos
    # en requests concurrentes sobre la misma sesión.
    s = None  # inicializar antes del bucle para evitar NameError si todos los reintentos fallan
    for _attempt in range(3):
        try:
            async with r.pipeline() as pipe:
                await pipe.watch(key)
                raw = await pipe.get(key)
                if not raw:
                    await pipe.unwatch()
                    return
                s = json.loads(raw)
                for k, v in updates.items():
                    if k == "perfil" and isinstance(v, dict) and isinstance(s.get("perfil"), dict):
                        s["perfil"] = {**s["perfil"], **v}
                    else:
                        s[k] = v
                s["updated_at"] = datetime.now(timezone.utc).isoformat()
                pipe.multi()
                pipe.setex(key, _TTL, json.dumps(s, ensure_ascii=False))
                await pipe.execute()
                break
        except WatchError:
            continue
        except Exception as e:
            logger.warning("Redis actualizar_sesion fail (intento %d): %s", _attempt, e)
            # No silenciar el error — hacer fallback directo a PostgreSQL
            # con los updates recibidos, sin depender de `s`.
            try:
                async with get_db() as conn:
                    # Leer el perfil actual desde PG para hacer merge correcto
                    row = await conn.fetchrow(
                        "SELECT perfil FROM sesiones WHERE id=$1", session_id)
                    if row is not None:
                        perfil_actual = row["perfil"] or {}
                        perfil_update = updates.get("perfil", {})
                        if isinstance(perfil_update, dict) and isinstance(perfil_actual, dict):
                            perfil_merged = {**perfil_actual, **perfil_update}
                        else:
                            perfil_merged = perfil_update if perfil_update else perfil_actual
                        # JSONB: pasamos el dict directo — el codec encodea una sola vez.
                        await conn.execute(
                            "UPDATE sesiones SET perfil=$1, updated_at=NOW() WHERE id=$2",
                            perfil_merged, session_id)
                        logger.info(
                            "actualizar_sesion: fallback PG exitoso para sesion %s", session_id)
            except Exception as pg_err:
                logger.error(
                    "actualizar_sesion: fallback PG también falló para sesion %s: %s",
                    session_id, pg_err)
            return

    if s is None:
        return
    try:
        async with get_db() as conn:
            # JSONB: dict directo (ver comentario en crear_sesion).
            await conn.execute(
                "UPDATE sesiones SET perfil=$1, updated_at=NOW() WHERE id=$2",
                s.get("perfil", {}), session_id)
    except Exception as e:
        logger.warning("PG sesion sync fail: %s", e)


async def guardar_busqueda(session_id: str, descripcion: str, filtros: dict,
                           perfil: dict, num_resultados: int) -> None:
    try:
        async with get_db() as conn:
            # `filtros` y `perfil_negocio` son JSONB: dicts directos (el codec
            # registrado en db/conexion.py hace json.dumps una sola vez).
            await conn.execute(
                "INSERT INTO busquedas(session_id,descripcion_original,filtros,perfil_negocio,num_resultados)"
                " VALUES($1,$2,$3,$4,$5)",
                session_id, descripcion, filtros, perfil, num_resultados)
    except Exception as e:
        logger.warning("guardar_busqueda fail: %s", e)


async def get_historial_cuestionario(session_id: str) -> list[dict]:
    try:
        async with get_db() as conn:
            rows = await conn.fetch(
                "SELECT rol, texto FROM mensajes_cuestionario WHERE session_id=$1 ORDER BY orden ASC",
                session_id)
        return [{"role": r["rol"], "content": r["texto"]} for r in rows]
    except Exception as e:
        logger.error("historial cuestionario fail: %s", e)
        return []


async def guardar_mensaje(session_id: str, rol: str, texto: str, orden: int) -> None:
    try:
        async with get_db() as conn:
            await conn.execute(
                "INSERT INTO mensajes_cuestionario(session_id,rol,texto,orden) VALUES($1,$2,$3,$4)",
                session_id, rol, texto[:4000], orden)  # truncar a 4000 chars
    except Exception as e:
        logger.warning("guardar_mensaje fail: %s", e)


# Alias para compatibilidad con api/cuestionario.py
guardar_mensaje_cuestionario = guardar_mensaje
