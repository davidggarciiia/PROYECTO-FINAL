"""
pipelines/competencia_scrape.py — Pipeline nocturno que alimenta la dimensión
de Competencia vía el scraper gosom.

Flujo:

  1. Cargar zonas activas + nombre de barrio/distrito.
  2. Generar queries `(zona, sector, término)` y volcar fichero para gosom.
  3. Lanzar `gosom_runner.ejecutar()` (puede tardar horas).
  4. Parsear el JSONL resultante y hacer upsert en `negocios_activos`.
  5. Recalcular `competencia_por_local` agregando por zona/sector/radio con SQL,
     marcando `cobertura_competencia` según haya o no resultados.
  6. Precalentar la caché Redis `comp:{zona_id}:{sector}:{radio_m}` con TTL 24 h
     para que la primera consulta del usuario no tenga que esperar al router.

Ejecución:
  - Cron: 03:00 diario (registrado en `pipelines.scheduler`).
  - CLI:  `python -m pipelines.comercio.competencia_scrape --sectores restauracion --max-zonas 50`.
"""
from __future__ import annotations

import argparse
import asyncio
import json
import logging
from datetime import date
from pathlib import Path
from typing import Iterable, Optional

from config import get_settings
from db.conexion import get_db
from db.redis_client import get_redis
from pipelines.inmobiliario.scraping import gosom_runner

logger = logging.getLogger(__name__)

_SECTORES_DEFAULT = ["restauracion", "moda", "estetica", "tatuajes", "shisha_lounge"]
_RADIOS = [150, 300, 500]  # metros


# ─── API pública ─────────────────────────────────────────────────────────────

async def ejecutar(sectores: Optional[Iterable[str]] = None,
                    max_zonas: Optional[int] = None,
                    output_file: Optional[Path] = None) -> dict:
    """
    Punto de entrada del pipeline. Devuelve un dict con contadores.

    Args:
        sectores: Lista de sectores a consultar. None = todos los estándar.
        max_zonas: Limita a N zonas (útil para smoke-test). None = todas.
        output_file: Sobrescribe `GOSOM_OUTPUT_DIR/out.jsonl` — útil en tests
                     para usar un dump pre-generado sin lanzar el scraper.
    """
    sectores_lista = list(sectores) if sectores else _SECTORES_DEFAULT
    s = get_settings()
    out_dir = Path(s.GOSOM_OUTPUT_DIR)
    output_file = output_file or (out_dir / "out.jsonl")
    queries_file = out_dir / "queries.txt"

    eid = await _registrar_inicio()
    stats: dict = {
        "queries": 0, "rows_parseadas": 0, "upsertadas": 0,
        "zonas": 0, "sectores": len(sectores_lista),
        "gosom_exit": None, "cobertura_alta": 0, "cobertura_ninguna": 0,
    }

    try:
        # 1. Zonas
        zonas = await _cargar_zonas(max_zonas=max_zonas)
        stats["zonas"] = len(zonas)
        if not zonas:
            logger.warning("competencia_scrape: no hay zonas activas")
            await _registrar_fin(eid, 0, "ok")
            return stats

        # 2. Queries
        queries = gosom_runner.generar_queries(zonas, sectores_lista)
        stats["queries"] = len(queries)
        gosom_runner.escribir_queries_file(queries, queries_file)
        logger.info("competencia_scrape: %d queries para %d zonas",
                    len(queries), len(zonas))

        # 3. Lanzar scraper (no-op en modo "disabled")
        gresult = await gosom_runner.ejecutar(queries_file, output_file)
        stats["gosom_exit"] = gresult.exit_code

        # 4. Ingestar
        upsertadas, rows_total = await _ingestar_dump(output_file, sectores_lista)
        stats["rows_parseadas"] = rows_total
        stats["upsertadas"] = upsertadas

        # 5. Recalcular competencia_por_local para todos los radios
        cobertura = await _recalcular_competencia(sectores_lista)
        stats["cobertura_alta"] = cobertura.get("alta", 0)
        stats["cobertura_ninguna"] = cobertura.get("ninguna", 0)

        # 6. Precalentar caché Redis
        await _precalentar_cache(sectores_lista)

        await _registrar_fin(eid, upsertadas, "ok")
        return stats

    except Exception as e:
        logger.exception("competencia_scrape error crítico: %s", e)
        await _registrar_fin(eid, stats["upsertadas"], "error", str(e))
        raise


# ─── Paso 1: zonas ───────────────────────────────────────────────────────────

async def _cargar_zonas(max_zonas: Optional[int]) -> list[dict]:
    """Zonas con variables_zona recientes + nombre de barrio y distrito para la query."""
    limit_clause = "LIMIT $1" if max_zonas else ""
    query = f"""
        SELECT z.id AS zona_id,
               z.nombre AS zona_nombre,
               b.nombre AS nombre_barrio,
               d.nombre AS nombre_distrito
        FROM zonas z
        LEFT JOIN barrios b   ON b.id = z.barrio_id
        LEFT JOIN distritos d ON d.id = b.distrito_id
        WHERE EXISTS (
            SELECT 1 FROM variables_zona vz
            WHERE vz.zona_id = z.id
              AND vz.fecha >= CURRENT_DATE - INTERVAL '60 days'
        )
        ORDER BY z.id
        {limit_clause}
    """
    async with get_db() as conn:
        if max_zonas:
            rows = await conn.fetch(query, max_zonas)
        else:
            rows = await conn.fetch(query)
    return [dict(r) for r in rows]


# ─── Paso 4: ingesta de negocios_activos ─────────────────────────────────────

async def _ingestar_dump(output_file: Path,
                          sectores_admitidos: list[str]) -> tuple[int, int]:
    """Itera el dump y hace upsert en `negocios_activos`. Devuelve (upsert, total_parseadas)."""
    upsertadas = 0
    rows_total = 0

    async with get_db() as conn:
        for negocio in gosom_runner.parsear_dump(output_file, sectores_admitidos):
            rows_total += 1
            try:
                await _upsert_negocio(conn, negocio)
                upsertadas += 1
            except Exception as e:
                logger.warning("competencia_scrape: upsert falló id=%s: %s",
                               negocio.get("id"), e)
    return upsertadas, rows_total


async def _upsert_negocio(conn, n: dict) -> None:
    """
    Upsert en `negocios_activos` — asigna `zona_id` a la primera zona que contenga
    el punto (o NULL si no cae dentro de ninguna, sucede con zonas industriales).

    Si el dict lleva ``_cerrado=True`` (gosom detectó permanently_closed), marcamos
    ``activo=FALSE`` y sellamos ``fecha_cierre_detectada`` con la primera vez que
    lo vimos cerrado (COALESCE: no pisar un cierre ya registrado).
    """
    cerrado = bool(n.get("_cerrado"))
    await conn.execute("""
        INSERT INTO negocios_activos
            (id, nombre, sector_codigo, subsector_codigo, lat, lng, geometria, zona_id,
             rating, total_resenas, precio_nivel, horario, es_activo, fuente,
             created_at, updated_at, fecha_cierre_detectada)
        SELECT $1, $2, $3, $4, $5, $6,
               ST_SetSRID(ST_MakePoint($6, $5), 4326),
               (SELECT id FROM zonas z
                WHERE ST_Contains(z.geometria, ST_SetSRID(ST_MakePoint($6, $5), 4326))
                LIMIT 1),
               $7, $8, $9, $10, $11, $12,
               NOW(), NOW(), $13
        ON CONFLICT (id) DO UPDATE SET
            subsector_codigo = COALESCE(EXCLUDED.subsector_codigo, negocios_activos.subsector_codigo),
            rating           = EXCLUDED.rating,
            total_resenas    = EXCLUDED.total_resenas,
            precio_nivel     = COALESCE(EXCLUDED.precio_nivel, negocios_activos.precio_nivel),
            horario          = COALESCE(EXCLUDED.horario, negocios_activos.horario),
            es_activo        = EXCLUDED.es_activo,
            fecha_cierre_detectada = COALESCE(negocios_activos.fecha_cierre_detectada,
                                               EXCLUDED.fecha_cierre_detectada),
            updated_at   = NOW()
    """, n["id"], n["nombre"], n["sector_codigo"], n.get("subsector_codigo"),
        n["lat"], n["lng"],
        n.get("rating"), n.get("num_resenas"),
        n.get("precio_nivel"),
        # `horario` es JSONB: pasamos el dict directo. El codec de db/conexion.py
        # lo encodea una sola vez (evita double-encoding).
        n.get("horario") if n.get("horario") else None,
        (not cerrado),  # activo
        n.get("fuente", "google_scrape"),
        date.today() if cerrado else None,
    )


# ─── Paso 5: recalcular competencia_por_local ────────────────────────────────

async def _recalcular_competencia(sectores: list[str]) -> dict[str, int]:
    """
    Triple agregación:

      - Pass A (macro): una fila por `(zona, sector, radio)` con
        `subsector_codigo = NULL`. Incluye métricas avanzadas (HHI, num_efectivos,
        edad_media, cerrados_ult_12m, pct_reviews_negativas, cadena_top_cuota)
        además de las básicas (num_competidores, rating_medio, score_saturacion).

      - Pass A' (zonas sin competidores): filas `cobertura_competencia='ninguna'`.
      - Pass B (subsector): una fila por combinación (zona, sector, subsector, radio),
        con las mismas métricas avanzadas para los competidores de ese subsector.

    Las métricas avanzadas se calculan en un único SQL con CTEs:

      - `cuotas`:  share de cada marca normalizada dentro de la agrupación.
      - `hhi`:     Σ(share²).
      - `cadena_top_cuota`: max(share).
      - `num_efectivos`: Σ min(1, rating * reviews / 100) — penaliza desconocidos.
      - `edad_media_dias`: avg de (NOW - created_at) para los negocios del grupo.
      - `cerrados_ult_12m`: count de negocios con fecha_cierre_detectada
        en los últimos 365 días dentro del radio.
      - `pct_reviews_negativas`: de las reseñas asociadas a los negocios del
        grupo (tabla `resenas`), % con rating < 3.
    """
    resumen = {"alta": 0, "ninguna": 0}
    hoy = date.today()

    async with get_db() as conn:
        for radio in _RADIOS:
            for sector in sectores:
                # Pass A — macro con stats avanzadas
                res_alta_macro = await _insert_agregado(
                    conn, sector=sector, radio=radio, hoy=hoy, por_subsector=False,
                )
                # Pass A' — zonas sin competidores ('ninguna')
                res_ninguna_macro = await conn.execute("""
                    INSERT INTO competencia_por_local
                        (zona_id, sector_codigo, subsector_codigo, radio_m,
                         num_competidores, rating_medio, score_saturacion,
                         cobertura_competencia, fecha_calculo)
                    SELECT z.id, $1::varchar, NULL::varchar, $2::int, 0, NULL, NULL,
                           'ninguna', $3::date
                    FROM zonas z
                    WHERE NOT EXISTS (
                        SELECT 1 FROM negocios_activos na
                        WHERE na.sector_codigo = $1 AND na.es_activo = TRUE
                          AND ST_DWithin(na.geometria::geography, z.geometria::geography, $2)
                    )
                    ON CONFLICT (zona_id, sector_codigo, subsector_codigo, radio_m, fecha_calculo)
                    DO UPDATE SET
                        num_competidores      = 0,
                        rating_medio          = NULL,
                        score_saturacion      = NULL,
                        cobertura_competencia = 'ninguna'
                """, sector, radio, hoy)

                resumen["alta"]    += _affected_rows(res_alta_macro)
                resumen["ninguna"] += _affected_rows(res_ninguna_macro)

                # Pass B — por subsector con stats avanzadas
                res_alta_sub = await _insert_agregado(
                    conn, sector=sector, radio=radio, hoy=hoy, por_subsector=True,
                )
                resumen["alta"] += _affected_rows(res_alta_sub)

    logger.info("competencia_scrape: recálculo OK — %s", resumen)
    return resumen


async def _insert_agregado(conn, *, sector: str, radio: int, hoy: date,
                            por_subsector: bool) -> str:
    """SQL de un solo disparo con todas las métricas avanzadas."""
    # Si agrupamos por subsector, añadimos al GROUP BY/SELECT; si no, subsector = NULL
    select_sub = "na.subsector_codigo" if por_subsector else "NULL::varchar"
    where_sub  = "AND na.subsector_codigo IS NOT NULL" if por_subsector else ""
    group_sub  = ", na.subsector_codigo" if por_subsector else ""

    sql = f"""
        WITH candidatos AS (
            SELECT z.id AS zona_id, na.*,
                   lower(regexp_replace(na.nombre, '[^a-z0-9]', '', 'gi')) AS marca_norm
            FROM zonas z
            JOIN negocios_activos na
              ON na.sector_codigo = $1
             AND na.es_activo = TRUE
             {where_sub}
             AND ST_DWithin(na.geometria::geography, z.geometria::geography, $2)
        ),
        cuotas AS (
            SELECT zona_id{group_sub}, marca_norm,
                   COUNT(*)::float /
                     NULLIF(SUM(COUNT(*)) OVER (PARTITION BY zona_id{group_sub}), 0) AS share
            FROM candidatos
            GROUP BY zona_id{group_sub}, marca_norm
        ),
        concentracion AS (
            SELECT zona_id{group_sub},
                   SUM(share * share) AS hhi,
                   MAX(share)         AS cadena_top_cuota
            FROM cuotas
            GROUP BY zona_id{group_sub}
        ),
        cerrados AS (
            SELECT z.id AS zona_id,
                   COUNT(*)::int AS cerrados_12m
            FROM zonas z
            JOIN negocios_activos na
              ON na.sector_codigo = $1
             AND na.es_activo = FALSE
             AND na.fecha_cierre_detectada >= (CURRENT_DATE - INTERVAL '365 days')
             AND ST_DWithin(na.geometria::geography, z.geometria::geography, $2)
            GROUP BY z.id
        ),
        reviews AS (
            SELECT c.zona_id{group_sub},
                   COUNT(*) FILTER (WHERE r.rating < 3)::float
                     / NULLIF(COUNT(*)::float, 0) AS pct_neg
            FROM candidatos c
            JOIN resenas r ON r.negocio_id = c.id
            WHERE r.rating IS NOT NULL
            GROUP BY c.zona_id{group_sub}
        ),
        base AS (
            SELECT zona_id{group_sub},
                   COUNT(*)::int                                          AS n_competidores,
                   NULLIF(AVG(rating), 'NaN'::float)                      AS rating_medio,
                   LEAST(100.0, COUNT(*) * 5.0)                           AS saturacion,
                   SUM(
                       CASE
                           WHEN rating IS NOT NULL AND total_resenas IS NOT NULL
                                THEN LEAST(1.0, (rating * total_resenas) / 100.0)
                           ELSE 0.5
                       END
                   )::float                                               AS num_efectivos,
                   AVG(EXTRACT(EPOCH FROM (NOW() - created_at)) / 86400.0) AS edad_dias
            FROM candidatos
            GROUP BY zona_id{group_sub}
        )
        INSERT INTO competencia_por_local
            (zona_id, sector_codigo, subsector_codigo, radio_m,
             num_competidores, rating_medio, score_saturacion,
             cobertura_competencia, fecha_calculo,
             hhi, cadena_top_cuota, num_efectivos, edad_media_dias,
             cerrados_ult_12m, pct_reviews_negativas)
        SELECT
            b.zona_id,
            $1::varchar,
            {('b.subsector_codigo' if por_subsector else 'NULL::varchar')},
            $2::int,
            b.n_competidores,
            b.rating_medio,
            b.saturacion,
            'alta',
            $3::date,
            c.hhi,
            c.cadena_top_cuota,
            b.num_efectivos,
            b.edad_dias,
            COALESCE(cd.cerrados_12m, 0),
            rv.pct_neg
        FROM base b
        LEFT JOIN concentracion c ON c.zona_id = b.zona_id
             {('AND c.subsector_codigo = b.subsector_codigo' if por_subsector else '')}
        LEFT JOIN cerrados cd     ON cd.zona_id = b.zona_id
        LEFT JOIN reviews  rv     ON rv.zona_id = b.zona_id
             {('AND rv.subsector_codigo = b.subsector_codigo' if por_subsector else '')}
        ON CONFLICT (zona_id, sector_codigo, subsector_codigo, radio_m, fecha_calculo)
        DO UPDATE SET
            num_competidores      = EXCLUDED.num_competidores,
            rating_medio          = EXCLUDED.rating_medio,
            score_saturacion      = EXCLUDED.score_saturacion,
            cobertura_competencia = EXCLUDED.cobertura_competencia,
            hhi                   = EXCLUDED.hhi,
            cadena_top_cuota      = EXCLUDED.cadena_top_cuota,
            num_efectivos         = EXCLUDED.num_efectivos,
            edad_media_dias       = EXCLUDED.edad_media_dias,
            cerrados_ult_12m      = EXCLUDED.cerrados_ult_12m,
            pct_reviews_negativas = EXCLUDED.pct_reviews_negativas
    """
    return await conn.execute(sql, sector, radio, hoy)


def _affected_rows(result: str) -> int:
    """asyncpg devuelve 'INSERT 0 42' o 'UPDATE 42'. Extrae el número."""
    try:
        return int(result.rsplit(" ", 1)[1])
    except (ValueError, IndexError):
        return 0


# ─── Paso 6: precalentar caché Redis ─────────────────────────────────────────

async def _precalentar_cache(sectores: list[str]) -> None:
    """Escribe en Redis el snapshot para que `consultar_competencia` no tenga
    que lanzar proveedores reales en el hit interactivo.

    Dos formatos de key conviviendo:
      - `comp:{zona}:{sector}:{radio}`              — agregado macro (subsector NULL)
      - `comp:{zona}:{sector}:sub:{subsector}:{radio}` — agregado por subsector
    """
    try:
        r = get_redis()
    except RuntimeError:
        logger.info("competencia_scrape: Redis no inicializado, skip precalentado")
        return

    ttl = get_settings().COMPETENCIA_CACHE_TTL_S
    async with get_db() as conn:
        rows = await conn.fetch("""
            SELECT zona_id, sector_codigo, subsector_codigo, radio_m,
                   num_competidores, rating_medio, score_saturacion,
                   cobertura_competencia
            FROM competencia_por_local
            WHERE fecha_calculo = CURRENT_DATE
              AND sector_codigo = ANY($1)
        """, sectores)

    escritas = 0
    for row in rows:
        if row["subsector_codigo"]:
            key = (f"comp:{row['zona_id']}:{row['sector_codigo']}"
                   f":sub:{row['subsector_codigo']}:{row['radio_m']}")
        else:
            key = f"comp:{row['zona_id']}:{row['sector_codigo']}:{row['radio_m']}"
        payload = {
            "num_competidores": row["num_competidores"],
            "rating_medio": float(row["rating_medio"]) if row["rating_medio"] is not None else None,
            "score_saturacion": float(row["score_saturacion"]) if row["score_saturacion"] is not None else None,
            "cobertura": row["cobertura_competencia"],
            "subsector_codigo": row["subsector_codigo"],
        }
        try:
            await r.setex(key, ttl, json.dumps(payload))
            escritas += 1
        except Exception as e:
            logger.warning("competencia_scrape: set caché key=%s: %s", key, e)
    logger.info("competencia_scrape: caché precalentada con %d entradas", escritas)


# ─── Registro de ejecución ───────────────────────────────────────────────────

async def _registrar_inicio():
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline,estado) "
            "VALUES('competencia_scrape','running') RETURNING id")


async def _registrar_fin(eid, registros, estado, mensaje=None):
    async with get_db() as conn:
        await conn.execute(
            "UPDATE pipeline_ejecuciones "
            "SET fecha_fin=NOW(), registros=$1, estado=$2, mensaje_error=$3 "
            "WHERE id=$4",
            registros, estado, mensaje, eid)


# ─── CLI ─────────────────────────────────────────────────────────────────────

async def _main() -> None:
    parser = argparse.ArgumentParser(description="Pipeline nocturno de Competencia (gosom).")
    parser.add_argument("--sectores", nargs="+", default=_SECTORES_DEFAULT,
                        help="Sectores a consultar")
    parser.add_argument("--max-zonas", type=int, default=None,
                        help="Limita el número de zonas (smoke-test)")
    parser.add_argument("--dump", type=str, default=None,
                        help="Usar un dump JSONL ya generado (no lanza scraper)")
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s")

    output = Path(args.dump) if args.dump else None
    stats = await ejecutar(sectores=args.sectores, max_zonas=args.max_zonas,
                            output_file=output)
    print(json.dumps(stats, indent=2, default=str))


if __name__ == "__main__":
    asyncio.run(_main())
