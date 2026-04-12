"""
pipelines/aforaments.py — Ingesta de flujo de TRÁFICO RODADO desde CSVs locales.

ADVERTENCIA: Este pipeline mide tráfico de VEHÍCULOS y BICICLETAS (IMD vial),
NO peatones a pie. Los sensores de aforament de Barcelona (loop inductivos en
calzada) cuentan unidades de tráfico rodado, no personas paseando.

Para flujo peatonal real (personas a pie), usar pipelines/vianants.py
que consume el dataset "Aforament de persones vianants per trams" de Open Data BCN.

El campo variables_zona.flujo_peatonal_total es sobreescrito por vianants.py
cuando hay datos disponibles (fuente='vianants_bcn' prevalece sobre
fuente='aforadors_csv_2025').

Fuentes:
  - CSV local: /data/csv/aforaments/2025_aforament_detall_valor.csv
    Columnas: Any, Id_aforament, Mes, Codi_tipus_dia, Desc_tipus_dia, Valor_IMD
    Codi_tipus_dia: 1=dilluns, 2=laborables, 3=divendres, 4=dissabte, 5=diumenge
    NOTA: Valor_IMD = Intensitat Mitja Diària de VEHICLES (no personas)

  - CSV local: /data/csv/aforaments/2025_aforament_descripcio.csv
    Columnas: Id_aforament, Desc_aforament, ..., Longitud, Latitud, ...
    (antes se obtenía de la API CKAN — ahora se lee directamente del CSV local)

Tabla destino: variables_zona (columnas flujo_peatonal_*)
  - Se usa como proxy de dinamismo urbano, NO como conteo directo de clientes.
  - Las zonas con datos de vianants.py tienen mayor precisión comercial.

ESTRATEGIA DE ASIGNACIÓN (v2):
  ST_DWithin radio 200m + ponderación por distancia inversa al cuadrado.
  Cada sensor influye a todas las zonas dentro del radio, proporcional
  al inverso del cuadrado de la distancia al centroide de la zona.

  Usamos el IMD de días laborables (Codi_tipus_dia=2) promediado sobre
  todos los meses disponibles como valor representativo anual.

  Distribución IMD → franjas horarias (proporciones empíricas BCN):
    manana (8-14h):  35% del IMD diario
    tarde  (14-20h): 42% del IMD diario
    noche  (20-23h): 23% del IMD diario
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
_CSV_DIR        = Path(os.environ.get("CSV_DIR", "/data/csv"))
_CSV_AFORAMENTS = _CSV_DIR / "aforaments"
_CSV_IMD        = _CSV_AFORAMENTS / "2025_aforament_detall_valor.csv"
_CSV_DESCRIPCIO = _CSV_AFORAMENTS / "2025_aforament_descripcio.csv"

_RADIO_M    = 200   # Radio de influencia de cada sensor (metros)
_MAX_ZONAS  = 8     # Máximo zonas que puede influir un sensor

# Proporciones IMD → franjas (estimación empírica Barcelona)
_FRAC_MANANA = 0.35
_FRAC_TARDE  = 0.42
_FRAC_NOCHE  = 0.23


async def ejecutar() -> dict:
    eid = await _init("aforaments")
    try:
        # 1. Leer IMD desde CSV
        imd_por_sensor = _leer_csv_imd()
        if not imd_por_sensor:
            msg = f"CSV no encontrado o vacío: {_CSV_IMD}"
            logger.warning(msg)
            await _fin(eid, 0, "ok", msg)
            return {"registros": 0}

        logger.info("CSV leído: %d sensores con IMD", len(imd_por_sensor))

        # 2. Leer coordenadas de sensores desde CSV local
        coords_sensor = _leer_coordenadas()
        logger.info("Coordenadas leídas del CSV: %d sensores", len(coords_sensor))

        # 3. Cruzar: solo sensores con IMD Y coordenadas
        sensores = {
            sid: (imd_por_sensor[sid], *coords_sensor[sid])
            for sid in imd_por_sensor
            if sid in coords_sensor
        }
        logger.info("Sensores con datos completos: %d", len(sensores))

        # 4. Asignar flujo a zonas
        fecha_ref = date.today()
        total = await _asignar_zonas(sensores, fecha_ref)

        await _recalcular_totales(fecha_ref)
        await imputar_zonas_sin_cobertura(fecha_ref)

        await _fin(eid, total, "ok")
        logger.info("Aforaments OK — %d sensores procesados", total)
        return {"registros": total}

    except Exception as exc:
        logger.error("Pipeline aforaments ERROR: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


# ─── Lectura CSV ───────────────────────────────────────────────────────────────

def _leer_csv_imd() -> dict[str, float]:
    """
    Lee el CSV y devuelve IMD medio de días laborables (Codi_tipus_dia=2)
    promediado sobre todos los meses disponibles, por sensor.
    """
    if not _CSV_IMD.exists():
        return {}

    acum: dict[str, list[float]] = {}
    with open(_CSV_IMD, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            try:
                tipo = int(row.get("Codi_tipus_dia", -1) or -1)
                if tipo != 2:          # solo laborables
                    continue
                sid   = str(row.get("Id_aforament", "") or "").strip().strip('"')
                valor = float(row.get("Valor_IMD", 0) or 0)
                if sid and valor > 0:
                    acum.setdefault(sid, []).append(valor)
            except (ValueError, KeyError):
                continue

    return {sid: sum(vals) / len(vals) for sid, vals in acum.items() if vals}


# ─── Coordenadas desde CSV local ─────────────────────────────────────────────

def _leer_coordenadas() -> dict[str, tuple[float, float]]:
    """
    Lee las coordenadas de los sensores desde el CSV descriptiu local.
    Devuelve dict {Id_aforament: (lng, lat)}.
    """
    if not _CSV_DESCRIPCIO.exists():
        logger.warning("CSV descripcio no encontrado: %s", _CSV_DESCRIPCIO)
        return {}

    coords: dict[str, tuple[float, float]] = {}
    with open(_CSV_DESCRIPCIO, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        for row in reader:
            sid = str(row.get("Id_aforament", "") or "").strip().strip('"')
            try:
                lng = float(row.get("Longitud", 0) or 0)
                lat = float(row.get("Latitud", 0) or 0)
                if sid and lng is not None and lat is not None and (lng != 0.0 or lat != 0.0):
                    coords[sid] = (lng, lat)
            except (ValueError, TypeError):
                continue

    logger.info("CSV descripcio leído: %d sensores con coordenadas", len(coords))
    return coords


# ─── Asignación espacial ──────────────────────────────────────────────────────

async def _asignar_zonas(
    sensores: dict[str, tuple[float, float, float]],
    fecha: str,
) -> int:
    """
    Para cada sensor (imd, lng, lat), distribuye el flujo diario
    entre las zonas dentro del radio usando ponderación por distancia inversa.
    """
    n = 0
    async with get_db() as conn:
        for sid, (imd, lng, lat) in sensores.items():
            try:
                zonas = await conn.fetch(
                    """
                    SELECT
                        z.id AS zona_id,
                        ST_Distance(
                            z.geometria::geography,
                            ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                        ) AS distancia_m,
                        COALESCE(vc.ratio_locales_comerciales, 0.0) AS ratio_comercial
                    FROM zonas z
                    LEFT JOIN LATERAL (
                        SELECT ratio_locales_comerciales
                        FROM vz_comercial
                        WHERE zona_id = z.id
                        ORDER BY fecha DESC LIMIT 1
                    ) vc ON TRUE
                    WHERE ST_DWithin(
                        z.geometria::geography,
                        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                        $3
                    )
                    ORDER BY distancia_m ASC
                    LIMIT $4
                    """,
                    lng, lat, _RADIO_M, _MAX_ZONAS,
                )

                if not zonas:
                    continue

                pesos      = [1.0 / max(1.0, float(z["distancia_m"])) ** 2 for z in zonas]
                suma_pesos = sum(pesos) or 1.0

                for zona, peso in zip(zonas, pesos):
                    fraccion      = peso / suma_pesos
                    flujo_dia     = imd * fraccion

                    # Multiplicador comercial
                    ratio_com     = float(zona["ratio_comercial"] or 0.0)
                    multiplicador = 1.0 + min(0.35, ratio_com * 0.70)
                    flujo_dia    *= multiplicador

                    manana = round(flujo_dia * _FRAC_MANANA, 1)
                    tarde  = round(flujo_dia * _FRAC_TARDE,  1)
                    noche  = round(flujo_dia * _FRAC_NOCHE,  1)

                    # Anchor en variables_zona (tabla coordinadora delgada)
                    await conn.execute(
                        """
                        INSERT INTO variables_zona (zona_id, fecha, fuente)
                        VALUES ($1, $2, 'aforadors_csv_2025')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET fuente = EXCLUDED.fuente, updated_at = NOW()
                        """,
                        zona["zona_id"], fecha,
                    )
                    # Datos de flujo en tabla satélite vz_flujo
                    await conn.execute(
                        """
                        INSERT INTO vz_flujo
                            (zona_id, fecha,
                             flujo_peatonal_manana, flujo_peatonal_tarde,
                             flujo_peatonal_noche,  fuente)
                        VALUES ($1, $2, $3, $4, $5, 'aforadors_csv_2025')
                        ON CONFLICT (zona_id, fecha) DO UPDATE
                        SET flujo_peatonal_manana = COALESCE(vz_flujo.flujo_peatonal_manana, 0) + $3,
                            flujo_peatonal_tarde  = COALESCE(vz_flujo.flujo_peatonal_tarde,  0) + $4,
                            flujo_peatonal_noche  = COALESCE(vz_flujo.flujo_peatonal_noche,  0) + $5,
                            fuente = 'aforadors_csv_2025',
                            updated_at = NOW()
                        """,
                        zona["zona_id"], fecha, manana, tarde, noche,
                    )
                n += 1

            except Exception as exc:
                logger.warning("Error procesando sensor %s: %s", sid, exc)

    return n


async def _recalcular_totales(desde: date) -> None:
    """Recalcula flujo_peatonal_total = suma de las tres franjas en vz_flujo."""
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE vz_flujo
            SET flujo_peatonal_total = (
                COALESCE(flujo_peatonal_manana, 0) +
                COALESCE(flujo_peatonal_tarde,  0) +
                COALESCE(flujo_peatonal_noche,  0)
            ),
            updated_at = NOW()
            WHERE fecha >= $1
              AND fuente IN ('aforadors_csv_2025', 'aforadors_bcn_v2')
            """,
            desde,
        )
    logger.info("Totales recalculados desde %s", desde)


async def imputar_zonas_sin_cobertura(fecha: date) -> int:
    """
    Para zonas sin aforador a menos de 200m, imputa con la media
    del barrio → distrito → global.
    """
    async with get_db() as conn:
        ids_con_dato = {
            r["zona_id"] for r in await conn.fetch(
                "SELECT zona_id FROM vz_flujo "
                "WHERE fecha=$1 AND flujo_peatonal_total > 0",
                fecha,
            )
        }

        todas = await conn.fetch(
            "SELECT z.id, b.id AS barrio_id, d.id AS distrito_id "
            "FROM zonas z "
            "JOIN barrios b ON z.barrio_id = b.id "
            "JOIN distritos d ON b.distrito_id = d.id"
        )
        sin_dato = [z for z in todas if z["id"] not in ids_con_dato]
        if not sin_dato:
            return 0

        mb = {r["barrio_id"]: r for r in await conn.fetch(
            """
            SELECT b.id AS barrio_id,
                   AVG(f.flujo_peatonal_manana) AS manana,
                   AVG(f.flujo_peatonal_tarde)  AS tarde,
                   AVG(f.flujo_peatonal_noche)  AS noche,
                   AVG(f.flujo_peatonal_total)  AS total
            FROM vz_flujo f
            JOIN zonas z   ON z.id = f.zona_id
            JOIN barrios b ON z.barrio_id = b.id
            WHERE f.fecha = $1 AND f.flujo_peatonal_total > 0
            GROUP BY b.id
            """, fecha,
        )}
        md = {r["distrito_id"]: r for r in await conn.fetch(
            """
            SELECT d.id AS distrito_id,
                   AVG(f.flujo_peatonal_manana) AS manana,
                   AVG(f.flujo_peatonal_tarde)  AS tarde,
                   AVG(f.flujo_peatonal_noche)  AS noche,
                   AVG(f.flujo_peatonal_total)  AS total
            FROM vz_flujo f
            JOIN zonas z   ON z.id = f.zona_id
            JOIN barrios b ON z.barrio_id = b.id
            JOIN distritos d ON b.distrito_id = d.id
            WHERE f.fecha = $1 AND f.flujo_peatonal_total > 0
            GROUP BY d.id
            """, fecha,
        )}

        fallback = {"manana": 297.5, "tarde": 357.0, "noche": 195.5, "total": 850.0}
        n = 0
        for zona in sin_dato:
            m = mb.get(zona["barrio_id"]) or md.get(zona["distrito_id"]) or fallback
            # Anchor en variables_zona
            await conn.execute(
                """
                INSERT INTO variables_zona (zona_id, fecha, fuente)
                VALUES ($1, $2, 'imputado_barrio')
                ON CONFLICT (zona_id, fecha) DO NOTHING
                """,
                zona["id"], fecha,
            )
            # Datos de flujo en tabla satélite vz_flujo
            await conn.execute(
                """
                INSERT INTO vz_flujo
                    (zona_id, fecha, flujo_peatonal_manana, flujo_peatonal_tarde,
                     flujo_peatonal_noche, flujo_peatonal_total, fuente)
                VALUES ($1, $2, $3, $4, $5, $6, 'imputado_barrio')
                ON CONFLICT (zona_id, fecha) DO NOTHING
                """,
                zona["id"], fecha,
                round(float(m["manana"] or fallback["manana"]), 1),
                round(float(m["tarde"]  or fallback["tarde"]),  1),
                round(float(m["noche"]  or fallback["noche"]),  1),
                round(float(m["total"]  or fallback["total"]),  1),
            )
            n += 1

    logger.info("Imputadas %d zonas sin cobertura para %s", n, fecha)
    return n


# ─── Helpers BD ───────────────────────────────────────────────────────────────

async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones (pipeline, estado) "
            "VALUES ($1,'running') RETURNING id",
            pipeline,
        )


async def _fin(eid: int, registros: int, estado: str, mensaje: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid,
        )
