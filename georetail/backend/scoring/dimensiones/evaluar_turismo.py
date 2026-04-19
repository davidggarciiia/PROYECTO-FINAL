"""
scoring/dimensiones/evaluar_turismo.py — Validación del modulo de turismo
contra el ground truth oficial de Open Data BCN (intensitat_turismo_oficial,
3 capas: allotjament / atractius / oci).

Para cada zona:
  1. Computa la intensidad oficial promedio area-weighted por capa
     (ST_Intersection con los poligonos oficiales que solapen con la zona).
  2. Computa nuestro score_turismo combinado llamando a calcular_turismo()
     con los datos actuales de v_variables_zona.
  3. Compara via Spearman (no parametrico, robusto a outliers).

Output:
  - Tabla resumen en stdout con correlaciones por capa y combinada.
  - CSV detallado en /tmp con (zona_id, score_modulo, intensidad_oficial_*).

Uso:
  python -m scoring.dimensiones.evaluar_turismo
"""
from __future__ import annotations

import asyncio
import logging
import os
from pathlib import Path

import asyncpg

from scoring.dimensiones.turismo import calcular_turismo

logger = logging.getLogger(__name__)

_DEFAULT_DB_URL = "postgresql://postgres:password@localhost:5432/georetail"
_OUT_CSV = Path(os.environ.get("TMP", "/tmp")) / "turismo_validacion.csv"


async def _zonas_con_features(conn) -> list[dict]:
    """Devuelve para cada zona los datos consumidos por calcular_turismo."""
    rows = await conn.fetch("""
        SELECT
            z.id AS zona_id,
            vz.airbnb_density_500m,
            vz.airbnb_occupancy_est,
            vz.booking_hoteles_500m,
            vz.eventos_culturales_500m,
            vz.venues_musicales_500m,
            vz.seasonality_summer_lift,
            ST_Distance(
                ST_Centroid(z.geometria)::geography,
                ST_GeomFromText(
                    'LINESTRING(2.1850 41.3740,2.1940 41.3792,2.2030 41.3840,'
                    '2.2130 41.3900,2.2250 41.3970,2.2380 41.4020)', 4326
                )::geography
            )::int AS dist_playa_m
        FROM zonas z
        LEFT JOIN LATERAL (
            SELECT * FROM v_variables_zona WHERE zona_id = z.id
            ORDER BY fecha DESC LIMIT 1
        ) vz ON TRUE
    """)
    return [dict(r) for r in rows]


async def _intensidad_oficial_por_zona(conn, capa: str) -> dict[str, float]:
    """
    Intensidad oficial area-weighted por zona para una capa concreta.

    SUM(intensidad * area_interseccion) / SUM(area_interseccion).
    Zonas sin solape devuelven None -> filtradas del dict.
    """
    rows = await conn.fetch("""
        WITH areas AS (
            SELECT
                z.id AS zona_id,
                ito.intensidad,
                ST_Area(ST_Intersection(z.geometria, ito.geom)::geography) AS area
            FROM zonas z
            JOIN intensitat_turismo_oficial ito
                ON ito.capa = $1 AND ST_Intersects(z.geometria, ito.geom)
        )
        SELECT
            zona_id,
            SUM(intensidad * area) / NULLIF(SUM(area), 0) AS intensidad_pond
        FROM areas
        GROUP BY zona_id
    """, capa)
    return {r["zona_id"]: float(r["intensidad_pond"]) for r in rows if r["intensidad_pond"] is not None}


def _spearman(x: list[float], y: list[float]) -> tuple[float, int]:
    """Correlacion de Spearman pura (rangos + Pearson). Devuelve (rho, n)."""
    n = len(x)
    if n < 3:
        return 0.0, n

    def ranks(vals: list[float]) -> list[float]:
        srt = sorted(range(n), key=lambda i: vals[i])
        r = [0.0] * n
        i = 0
        while i < n:
            j = i
            while j + 1 < n and vals[srt[j + 1]] == vals[srt[i]]:
                j += 1
            avg_rank = (i + j) / 2.0 + 1
            for k in range(i, j + 1):
                r[srt[k]] = avg_rank
            i = j + 1
        return r

    rx, ry = ranks(x), ranks(y)
    mx, my = sum(rx) / n, sum(ry) / n
    num = sum((rx[i] - mx) * (ry[i] - my) for i in range(n))
    dx = sum((rx[i] - mx) ** 2 for i in range(n)) ** 0.5
    dy = sum((ry[i] - my) ** 2 for i in range(n)) ** 0.5
    if dx == 0 or dy == 0:
        return 0.0, n
    return num / (dx * dy), n


def _interpretar(rho: float) -> str:
    if rho >= 0.70:  return "muy alta"
    if rho >= 0.50:  return "alta"
    if rho >= 0.30:  return "moderada"
    if rho >= 0.15:  return "debil"
    return "nula/inversa"


async def main() -> None:
    db_url = os.environ.get("DATABASE_URL", _DEFAULT_DB_URL)
    conn = await asyncpg.connect(db_url)

    try:
        # 1. Datos de zonas + score modulo
        datos_zonas = await _zonas_con_features(conn)
        scores_modulo: dict[str, float] = {}
        for z in datos_zonas:
            zid = z.pop("zona_id")
            try:
                res = calcular_turismo(z)
                scores_modulo[zid] = res["score_turismo"]
            except Exception as exc:
                logger.warning("zona=%s falla calcular_turismo: %s", zid, exc)

        # 2. Intensidad oficial por capa
        capas = ["allotjament", "atractius", "oci"]
        oficial: dict[str, dict[str, float]] = {}
        for capa in capas:
            oficial[capa] = await _intensidad_oficial_por_zona(conn, capa)

        # 3. Combinada oficial = media de las 3 capas (donde existe)
        combinado_oficial: dict[str, float] = {}
        for zid in scores_modulo:
            vals = [oficial[c][zid] for c in capas if zid in oficial[c]]
            if vals:
                combinado_oficial[zid] = sum(vals) / len(vals)

        # 4. Correlaciones
        print()
        print("=" * 64)
        print("VALIDACION DIMENSION TURISMO vs Open Data BCN")
        print("=" * 64)
        print(f"Zonas con score modulo:       {len(scores_modulo)}")
        for capa in capas:
            inter = set(scores_modulo) & set(oficial[capa])
            print(f"Zonas con solape '{capa:11s}': {len(inter)}")
        print(f"Zonas con todas las capas:    {len(combinado_oficial)}")
        print()

        # Por capa
        print(f"{'capa':15s}  {'rho':>7s}  {'n':>4s}  interpretacion")
        print("-" * 64)
        resultados = {}
        for capa in capas:
            x, y, zids = [], [], []
            for zid, sc in scores_modulo.items():
                if zid in oficial[capa]:
                    x.append(sc)
                    y.append(oficial[capa][zid])
                    zids.append(zid)
            rho, n = _spearman(x, y)
            resultados[capa] = (rho, n)
            print(f"{capa:15s}  {rho:>+7.3f}  {n:>4d}  {_interpretar(rho)}")

        # Combinado
        x, y = [], []
        for zid, sc in scores_modulo.items():
            if zid in combinado_oficial:
                x.append(sc)
                y.append(combinado_oficial[zid])
        rho_c, n_c = _spearman(x, y)
        print("-" * 64)
        print(f"{'combinado':15s}  {rho_c:>+7.3f}  {n_c:>4d}  {_interpretar(rho_c)}")
        print()

        # 5. Top 5 mejor / peor alineados (debug)
        diffs = []
        for zid, sc in scores_modulo.items():
            if zid in combinado_oficial:
                diffs.append((zid, sc, combinado_oficial[zid], abs(sc - combinado_oficial[zid])))
        diffs.sort(key=lambda t: -t[3])
        print("Top 5 zonas con MAYOR desalineacion (modulo vs oficial):")
        print(f"  {'zona_id':25s}  {'modulo':>7s}  {'oficial':>7s}  {'|diff|':>7s}")
        for zid, sc, of, df in diffs[:5]:
            print(f"  {zid:25s}  {sc:>7.1f}  {of:>7.1f}  {df:>7.1f}")
        print()
        print("Top 5 zonas con MENOR desalineacion (mejor calibradas):")
        print(f"  {'zona_id':25s}  {'modulo':>7s}  {'oficial':>7s}  {'|diff|':>7s}")
        for zid, sc, of, df in diffs[-5:]:
            print(f"  {zid:25s}  {sc:>7.1f}  {of:>7.1f}  {df:>7.1f}")

        # 6. CSV detallado
        with open(_OUT_CSV, "w", encoding="utf-8") as f:
            f.write("zona_id,score_modulo,oficial_allotjament,oficial_atractius,oficial_oci,oficial_combinado\n")
            for zid, sc in scores_modulo.items():
                a = oficial["allotjament"].get(zid, "")
                t = oficial["atractius"].get(zid, "")
                o = oficial["oci"].get(zid, "")
                c = combinado_oficial.get(zid, "")
                f.write(f"{zid},{sc},{a},{t},{o},{c}\n")
        print()
        print(f"CSV detallado: {_OUT_CSV}")

    finally:
        await conn.close()


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    asyncio.run(main())
