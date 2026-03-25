"""
pipelines/cens_comercial.py — Construcción de negocios_historico desde Cens Comercial BCN.

Genera las etiquetas de supervivencia para entrenar el modelo XGBoost:
  label=True  (activo_3_anos=True)  → negocio presente en 2022 o 2024
  label=False (activo_3_anos=False) → negocio de 2016 desaparecido en 2022 y 2024

Estrategia de cross-referencia:
  - Base: 2016_cens_comercial.csv → ~60.000 negocios activos (N_PRINCIP='Actiu')
  - 2022_cens_comercial.csv y 2024_cens_comercial.csv tienen columna ID_Bcn_2016
    que enlaza con ID_BCN del censo de 2016.
  - Negocio con ID_BCN en {IDs con ID_Bcn_2016 en 2022 ∪ 2024} → survived=True
  - Resto → survived=False (cerró en algún momento entre 2016 y 2022)

Geocodificación:
  Cada negocio tiene LATITUD/LONGITUD en el CSV de 2016.
  ST_Within para encontrar zona_id. Fallback: ST_DWithin 100m. Si no hay zona, se omite.

Mapeado de sectores (N_GRUPACT → sector_codigo):
  Ver dict _SECTOR_MAP abajo. Los grupos sin sector relevante se omiten.

Rendimiento:
  Batches de 200 inserciones. La geocodificación se hace por zona en una sola
  query agrupada para evitar 60k queries individuales.
"""
from __future__ import annotations

import csv
import logging
import os
from datetime import date
from pathlib import Path
from typing import Optional

from db.conexion import get_db

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_CSV_DIR  = Path(os.environ.get("CSV_DIR", "/data/csv"))
_CSV_2016 = _CSV_DIR / "cens_comercial" / "2016_cens_comercial.csv"
_CSV_2022 = _CSV_DIR / "cens_comercial" / "2022_cens_comercial.csv"
_CSV_2024 = _CSV_DIR / "cens_comercial" / "2024_cens_comercial.csv"

_BATCH_INSERT = 200   # Registros por INSERT batch
_RADIO_FALLBACK_M = 100  # Radio para fallback de geocodificación

# ── Mapeado grupos actividad → sector_codigo ───────────────────────────────────
# Solo se importan los grupos con sector relevante para el modelo.
# Los demás (inmobiliàries, finances, automoció...) se descartan.
_SECTOR_MAP: dict[str, str] = {
    "Restaurants, bars i hotels (Inclòs hostals, pensions i fondes)": "restauracion",
    "Restaurants, bars i hotels":                                       "restauracion",
    "Equipament personal":                                              "moda",
    "Quotidià alimentari":                                              "alimentacion",
    "Quotidià no alimentari":                                           "servicios",
    "Sanitat i assistència":                                            "salud",
    "Parament de la llar":                                              "servicios",
    "Oci i cultura":                                                    "deporte",
    "Equipaments culturals i recreatius":                               "deporte",
    "Ensenyament":                                                      "educacion",
    "Manteniment, neteja i producció":                                  "servicios",
    "Reparacions (Electrodomèstics i automòbils)":                      "servicios",
    "Altres":                                                           "servicios",
}

# Sectores a incluir (filtra "servicios" genérico si se quiere solo verticales clave)
# Dejamos todos para maximizar el dataset de entrenamiento
_SECTORES_INCLUIDOS = set(_SECTOR_MAP.values())


# ═══════════════════════════════════════════════════════════════════════════════
# Punto de entrada
# ═══════════════════════════════════════════════════════════════════════════════

async def ejecutar() -> dict:
    """Ejecuta el pipeline completo y devuelve métricas."""
    eid = await _init("cens_comercial")
    try:
        # 1. Cargar IDs supervivientes desde 2022 y 2024 (fuera de la transacción — solo lectura CSV)
        ids_supervivientes = _cargar_ids_supervivientes()
        logger.info(
            "IDs supervivientes (en 2022 o 2024): %d", len(ids_supervivientes)
        )

        # 2. Leer negocios de 2016
        negocios_2016 = _cargar_negocios_2016(ids_supervivientes)
        logger.info(
            "Negocios 2016 a procesar (con sector mapeado): %d", len(negocios_2016)
        )

        if not negocios_2016:
            await _fin(eid, 0, "ok", "Sin negocios para procesar")
            return {"insertados": 0, "sin_zona": 0}

        # 3. Geocodificar → zona_id
        geocodificados = await _geocodificar_batch(negocios_2016)
        logger.info(
            "Geocodificados con zona: %d / %d",
            sum(1 for r in geocodificados if r["zona_id"]),
            len(geocodificados),
        )

        # 4. DELETE + INSERT en una sola transacción para atomicidad
        # (evita quedar sin datos si el INSERT falla a medias)
        insertados, sin_zona = await _insertar_atomico(geocodificados)

        await _fin(eid, insertados, "ok")
        logger.info(
            "cens_comercial OK — insertados=%d, sin_zona=%d", insertados, sin_zona
        )
        return {"insertados": insertados, "sin_zona": sin_zona}

    except Exception as exc:
        logger.error("Pipeline cens_comercial ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ═══════════════════════════════════════════════════════════════════════════════
# 1. Carga de IDs supervivientes
# ═══════════════════════════════════════════════════════════════════════════════

def _cargar_ids_supervivientes() -> set[str]:
    """
    Lee los CSVs de 2022 y 2024 y devuelve el conjunto de ID_Bcn_2016 presentes.
    Estos son los negocios de 2016 que seguían activos en años posteriores.
    """
    survived: set[str] = set()
    for csv_path in (_CSV_2022, _CSV_2024):
        if not csv_path.exists():
            logger.warning("CSV no encontrado: %s", csv_path)
            continue
        with open(csv_path, newline="", encoding="utf-8-sig") as f:
            reader = csv.DictReader(f)
            for row in reader:
                bid = str(row.get("ID_Bcn_2016", "") or "").strip()
                if bid and bid not in ("0", ""):
                    survived.add(bid)
    return survived


# ═══════════════════════════════════════════════════════════════════════════════
# 2. Carga de negocios 2016
# ═══════════════════════════════════════════════════════════════════════════════

def _cargar_negocios_2016(ids_supervivientes: set[str]) -> list[dict]:
    """
    Lee el CSV de 2016 y devuelve una lista de dicts con los campos necesarios.
    Solo incluye negocios activos (N_PRINCIP='Actiu') con sector mapeado.
    """
    if not _CSV_2016.exists():
        logger.error("CSV 2016 no encontrado: %s", _CSV_2016)
        return []

    negocios = []
    with open(_CSV_2016, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            # Solo activos
            if row.get("N_PRINCIP", "").strip() != "Actiu":
                continue

            # Mapear sector
            grupact = row.get("N_GRUPACT", "").strip()
            sector = _SECTOR_MAP.get(grupact)
            if not sector:
                continue

            # Coordenadas
            try:
                lat = float(row.get("LATITUD", 0) or 0)
                lng = float(row.get("LONGITUD", 0) or 0)
            except (ValueError, TypeError):
                continue
            if not lat or not lng:
                continue

            bid = str(row.get("ID_BCN", "") or "").strip()
            if not bid:
                continue

            # Fecha apertura desde campo DATA (formato YYYY-MM-DD)
            fecha_str = row.get("DATA", "").strip()
            fecha_apertura: Optional[date] = None
            if fecha_str:
                try:
                    fecha_apertura = date.fromisoformat(fecha_str[:10])
                except ValueError:
                    pass

            negocios.append({
                "id_bcn":          bid,
                "nombre":          row.get("N_LOCAL", "").strip() or "Sin nombre",
                "sector":          sector,
                "lat":             lat,
                "lng":             lng,
                "fecha_apertura":  fecha_apertura,
                "activo_3_anos":   bid in ids_supervivientes,
                "zona_id":         None,  # se rellena en paso 3
            })

    return negocios


# ═══════════════════════════════════════════════════════════════════════════════
# 3. Geocodificación → zona_id
# ═══════════════════════════════════════════════════════════════════════════════

async def _geocodificar_batch(negocios: list[dict]) -> list[dict]:
    """
    Asigna zona_id a cada negocio usando PostGIS.

    Estrategia de rendimiento: una sola query que recibe todos los puntos como
    VALUES y hace el ST_Within en bloque. Para los que no caen dentro de ninguna
    zona, se intenta ST_DWithin 100m (zona más cercana) también en bloque.

    Con 60k negocios esto evita 60k round-trips individuales a PostgreSQL.
    """
    if not negocios:
        return negocios

    # Dividir en chunks de 500 para no superar límites de parámetros de pg
    _CHUNK = 500

    async with get_db() as conn:
        sin_zona = list(negocios)  # inicialmente todos sin zona

        # ── Paso 1: ST_Within exacto ──
        for i in range(0, len(sin_zona), _CHUNK):
            chunk = sin_zona[i : i + _CHUNK]
            # Construir VALUES($1,$2,'idx')...
            values_sql = ", ".join(
                f"(${j*2+1}::float, ${j*2+2}::float, {j})"
                for j in range(len(chunk))
            )
            params = []
            for neg in chunk:
                params.extend([neg["lng"], neg["lat"]])

            rows = await conn.fetch(
                f"""
                SELECT pts.idx, z.id AS zona_id
                FROM (VALUES {values_sql}) AS pts(lng, lat, idx)
                JOIN zonas z ON ST_Within(
                    ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326),
                    z.geometria
                )
                """,
                *params,
            )
            for row in rows:
                chunk[row["idx"]]["zona_id"] = row["zona_id"]

        # ── Paso 2: fallback ST_DWithin para los que siguen sin zona ──
        pendientes = [n for n in negocios if not n.get("zona_id")]
        if pendientes:
            logger.debug(
                "Fallback ST_DWithin para %d negocios sin zona", len(pendientes)
            )
        for i in range(0, len(pendientes), _CHUNK):
            chunk = pendientes[i : i + _CHUNK]
            values_sql = ", ".join(
                f"(${j*2+1}::float, ${j*2+2}::float, {j})"
                for j in range(len(chunk))
            )
            params = []
            for neg in chunk:
                params.extend([neg["lng"], neg["lat"]])

            rows = await conn.fetch(
                f"""
                SELECT DISTINCT ON (pts.idx) pts.idx, z.id AS zona_id
                FROM (VALUES {values_sql}) AS pts(lng, lat, idx)
                JOIN zonas z ON ST_DWithin(
                    z.geometria::geography,
                    ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326)::geography,
                    {_RADIO_FALLBACK_M}
                )
                ORDER BY pts.idx,
                    ST_Distance(
                        z.geometria::geography,
                        ST_SetSRID(ST_MakePoint(pts.lng, pts.lat), 4326)::geography
                    )
                """,
                *params,
            )
            for row in rows:
                chunk[row["idx"]]["zona_id"] = row["zona_id"]

    return negocios


# ═══════════════════════════════════════════════════════════════════════════════
# 4. Inserción en negocios_historico
# ═══════════════════════════════════════════════════════════════════════════════

async def _insertar_atomico(negocios: list[dict]) -> tuple[int, int]:
    """
    Borra los registros previos e inserta los nuevos en una sola transacción.
    Si el INSERT falla a medias, el DELETE también se revierte, preservando
    los datos anteriores (sin ventana de datos vacíos).
    Devuelve (insertados, sin_zona).
    """
    registros_con_zona = [n for n in negocios if n["zona_id"]]
    sin_zona = len(negocios) - len(registros_con_zona)

    if not registros_con_zona:
        return 0, sin_zona

    _SQL = """
        INSERT INTO negocios_historico
            (nombre, sector_codigo, zona_id, fecha_apertura,
             activo_3_anos, fuente)
        VALUES ($1, $2, $3, $4, $5, 'cens_comercial_bcn')
    """

    async with get_db() as conn:
        async with conn.transaction():
            # DELETE + INSERT en la misma transacción → atómico
            result = await conn.execute(
                "DELETE FROM negocios_historico WHERE fuente='cens_comercial_bcn'"
            )
            logger.info("Registros previos eliminados: %s", result)

            await conn.executemany(
                _SQL,
                [
                    (
                        r["nombre"],
                        r["sector"],
                        r["zona_id"],
                        r["fecha_apertura"],
                        r["activo_3_anos"],
                    )
                    for r in registros_con_zona
                ],
            )

    return len(registros_con_zona), sin_zona


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers BD
# ═══════════════════════════════════════════════════════════════════════════════

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(
    eid: int, registros: int, estado: str, mensaje: Optional[str] = None
) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
