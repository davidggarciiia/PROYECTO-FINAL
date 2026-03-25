"""
db/exportaciones.py — CRUD (crear, leer, actualizar) de exportaciones PDF.

La tabla `exportaciones` registra cada PDF generado:
  - quién lo pidió (session_id)
  - qué zonas incluye
  - dónde está guardado en disco
  - cuándo expira el enlace de descarga

Usado por: api/exportar.py
"""
from __future__ import annotations

import uuid
from datetime import datetime, timedelta, timezone
from typing import Optional

from db.conexion import get_db


async def registrar_exportacion(
    session_id: str,
    zona_ids: list[str],
    horas_expiracion: int = 1,
) -> str:
    """
    Crea un registro de exportación en estado 'generando' y devuelve su ID (UUID).

    El worker de Celery (workers/tasks.py) actualizará el estado a 'ok'
    cuando el PDF esté listo, y escribirá la ruta en disco.
    """
    pdf_id     = str(uuid.uuid4())
    expires_at = datetime.now(timezone.utc) + timedelta(hours=horas_expiracion)

    async with get_db() as conn:
        await conn.execute(
            """
            INSERT INTO exportaciones (id, session_id, zona_ids, estado, expires_at)
            VALUES ($1, $2, $3, 'generando', $4)
            """,
            pdf_id, session_id, zona_ids, expires_at,
        )

    return pdf_id


async def get_exportacion(pdf_id: str) -> Optional[dict]:
    """
    Devuelve los datos de una exportación por su ID.

    Devuelve None si no existe o si ha expirado.
    """
    async with get_db() as conn:
        row = await conn.fetchrow(
            """
            SELECT id, session_id, zona_ids, estado, ruta_disco,
                   created_at, expires_at, descargado
            FROM exportaciones
            WHERE id = $1
            """,
            pdf_id,
        )

    if not row:
        return None

    datos = dict(row)

    # Comprobar si ha expirado
    if datos["expires_at"] and datetime.now(timezone.utc) > datos["expires_at"]:
        return None

    return datos


async def marcar_completada(pdf_id: str, ruta_disco: str) -> None:
    """
    Actualiza el estado a 'ok' y guarda la ruta del PDF en disco.
    Llamado por el worker Celery cuando termina de generar el PDF.
    """
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE exportaciones
            SET estado = 'ok', ruta_disco = $2
            WHERE id = $1
            """,
            pdf_id, ruta_disco,
        )


async def marcar_error(pdf_id: str, mensaje: str) -> None:
    """Marca la exportación como fallida."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE exportaciones SET estado = 'error', mensaje_error = $2 WHERE id = $1",
            pdf_id, mensaje[:500],
        )


async def marcar_descargado(pdf_id: str) -> None:
    """Marca que el PDF ya fue descargado (para analytics)."""
    async with get_db() as conn:
        await conn.execute(
            "UPDATE exportaciones SET descargado = TRUE WHERE id = $1",
            pdf_id,
        )


async def limpiar_expiradas() -> int:
    """
    Elimina exportaciones expiradas. Llamado por el scheduler semanalmente.
    Devuelve el número de registros eliminados.
    """
    async with get_db() as conn:
        result = await conn.execute(
            "DELETE FROM exportaciones WHERE expires_at < NOW()"
        )
    # asyncpg devuelve "DELETE N" como string
    try:
        return int(result.split()[-1])
    except (IndexError, ValueError):
        return 0
