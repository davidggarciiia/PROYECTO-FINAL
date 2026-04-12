"""
pipelines/peatonal/temporalidad.py - Capa temporal de GeoRetail BCN.

Materializa dos familias de señales:
  - Semanales/live: fin de semana, domingo, dayparts y proxy de festivos.
  - Estacionales/modelables: verano, navidad, rebajas, volatilidad y pico.

La filosofía es conservadora:
  - todo sale de datos locales o de tablas ya pobladas por otros pipelines;
  - si una fuente falta, la señal queda en NULL en vez de inventarse;
  - las señales live se usan ya en score/explicación;
  - las estacionales pueden entrar en el modelo cuando exista cobertura real.
"""
from __future__ import annotations

import csv
import json
import logging
import os
from collections import defaultdict
from datetime import date, timedelta
from pathlib import Path
from statistics import median, pstdev
from typing import Iterable

from db.conexion import get_db

logger = logging.getLogger(__name__)

_REPO_ROOT = Path(__file__).resolve().parents[4]
_CSV_ROOT = Path(os.environ.get("CSV_DIR", str(_REPO_ROOT / "CSV")))
_CSV_AFORAMENTS = _CSV_ROOT / "aforaments"
_CSV_IMD = _CSV_AFORAMENTS / "2025_aforament_detall_valor.csv"
_CSV_DESCRIPCIO = _CSV_AFORAMENTS / "2025_aforament_descripcio.csv"

_RADIO_M = 200
_MAX_ZONAS = 8

_DAY_NAMES = [
    "Monday",
    "Tuesday",
    "Wednesday",
    "Thursday",
    "Friday",
    "Saturday",
    "Sunday",
]

_ES_FIXED = {(1, 1), (1, 6), (5, 1), (8, 15), (10, 12), (11, 1), (12, 6), (12, 8), (12, 25)}
_CT_FIXED = {(9, 11), (9, 24), (12, 26)}
_BCN_FIXED = {(9, 24)}


async def ejecutar(start_year: int = 2010, end_year: int = 2035) -> dict:
    eid = await _init("temporalidad")
    fecha_ref = date.today()
    try:
        await _seed_calendar(start_year=start_year, end_year=end_year)

        weekly = await _read_aforaments_weekly_lifts()
        popular = await _popular_times_metrics()
        seasonality = await _seasonality_metrics()
        turismo = await _latest_tourism_scores()
        festive_transport = await _festive_transport_ratio()

        zona_ids = sorted(
            set(weekly)
            | set(popular)
            | set(seasonality)
            | set(turismo)
            | set(festive_transport)
        )

        upserts = 0
        async with get_db() as conn:
            for zona_id in zona_ids:
                payload = {}
                payload.update(weekly.get(zona_id, {}))
                payload.update(popular.get(zona_id, {}))
                payload.update(seasonality.get(zona_id, {}))

                holiday_proxy = _build_holiday_proxy(
                    weekend_lift=payload.get("weekend_lift"),
                    weekend_evening_share=payload.get("weekend_evening_share"),
                    score_turismo=turismo.get(zona_id),
                    festive_transport_ratio=festive_transport.get(zona_id),
                )
                if holiday_proxy is not None:
                    payload["holiday_proxy_score"] = holiday_proxy

                payload["temporal_confianza"] = _build_temporal_confidence(payload)
                if not payload:
                    continue

                await _upsert_temporal_row(conn, zona_id, fecha_ref, payload)
                upserts += 1

        await _fin(eid, upserts, "ok")
        logger.info("Temporalidad BCN OK - %d zonas actualizadas", upserts)
        return {"zonas_actualizadas": upserts, "fecha": fecha_ref.isoformat()}
    except Exception as exc:
        logger.error("Pipeline temporalidad error: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


async def materializar_historico(
    *,
    start_year: int | None = None,
    end_year: int | None = None,
) -> dict:
    """
    Materializa snapshots históricos de seasonality_* usando solo meses previos
    a cada fecha de snapshot. No intenta historificar señales weekly/live.
    """
    eid = await _init("temporalidad_historica")
    try:
        await _seed_calendar(
            start_year=start_year or 2010,
            end_year=end_year or 2035,
        )
        snapshot_dates = await _available_seasonality_snapshot_dates(
            start_year=start_year,
            end_year=end_year,
        )
        upserts = 0
        async with get_db() as conn:
            for snapshot_date in snapshot_dates:
                seasonality = await _seasonality_metrics(as_of_date=snapshot_date)
                for zona_id, payload in seasonality.items():
                    payload = dict(payload)
                    payload["temporal_confianza"] = _build_temporal_confidence(payload)
                    await _upsert_temporal_row(conn, zona_id, snapshot_date, payload)
                    upserts += 1

        await _fin(eid, upserts, "ok")
        logger.info("Temporalidad histórica OK - %d snapshots materializados", upserts)
        return {"snapshots_materializados": upserts, "fechas": len(snapshot_dates)}
    except Exception as exc:
        logger.error("Temporalidad histórica error: %s", exc, exc_info=True)
        await _fin(eid, 0, "error", str(exc))
        raise


async def _seed_calendar(start_year: int, end_year: int) -> None:
    rows = list(_iter_calendar_rows(start_year, end_year))
    if not rows:
        return

    sql = """
        INSERT INTO dim_calendario_bcn (
            fecha, anio, mes, dia,
            is_weekend, is_saturday, is_sunday,
            is_public_holiday_es, is_public_holiday_ct, is_public_holiday_bcn,
            is_school_holiday, is_bridge_day,
            is_summer, is_christmas, is_rebajas_winter, is_rebajas_summer,
            season
        )
        VALUES (
            $1, $2, $3, $4,
            $5, $6, $7,
            $8, $9, $10,
            $11, $12,
            $13, $14, $15, $16,
            $17
        )
        ON CONFLICT (fecha) DO UPDATE SET
            anio = EXCLUDED.anio,
            mes = EXCLUDED.mes,
            dia = EXCLUDED.dia,
            is_weekend = EXCLUDED.is_weekend,
            is_saturday = EXCLUDED.is_saturday,
            is_sunday = EXCLUDED.is_sunday,
            is_public_holiday_es = EXCLUDED.is_public_holiday_es,
            is_public_holiday_ct = EXCLUDED.is_public_holiday_ct,
            is_public_holiday_bcn = EXCLUDED.is_public_holiday_bcn,
            is_school_holiday = EXCLUDED.is_school_holiday,
            is_bridge_day = EXCLUDED.is_bridge_day,
            is_summer = EXCLUDED.is_summer,
            is_christmas = EXCLUDED.is_christmas,
            is_rebajas_winter = EXCLUDED.is_rebajas_winter,
            is_rebajas_summer = EXCLUDED.is_rebajas_summer,
            season = EXCLUDED.season
    """
    async with get_db() as conn:
        await conn.executemany(sql, rows)
        await conn.execute(
            """
            UPDATE dim_calendario_bcn c
            SET is_bridge_day = (
                NOT c.is_weekend
                AND NOT (c.is_public_holiday_es OR c.is_public_holiday_ct OR c.is_public_holiday_bcn)
                AND (
                    EXISTS (
                        SELECT 1
                        FROM dim_calendario_bcn p
                        WHERE p.fecha = c.fecha - 1
                          AND (p.is_public_holiday_es OR p.is_public_holiday_ct OR p.is_public_holiday_bcn)
                    )
                    OR EXISTS (
                        SELECT 1
                        FROM dim_calendario_bcn n
                        WHERE n.fecha = c.fecha + 1
                          AND (n.is_public_holiday_es OR n.is_public_holiday_ct OR n.is_public_holiday_bcn)
                    )
                )
            )
            WHERE c.fecha BETWEEN $1 AND $2
            """,
            date(start_year, 1, 1),
            date(end_year, 12, 31),
        )


def _iter_calendar_rows(start_year: int, end_year: int) -> Iterable[tuple]:
    start = date(start_year, 1, 1)
    finish = date(end_year, 12, 31)
    current = start
    while current <= finish:
        easter = _easter_sunday(current.year)
        public_es = _is_public_holiday_es(current, easter)
        public_ct = _is_public_holiday_ct(current, easter)
        public_bcn = _is_public_holiday_bcn(current)
        season = _season_name(current)
        row = (
            current,
            current.year,
            current.month,
            current.day,
            current.weekday() >= 5,
            current.weekday() == 5,
            current.weekday() == 6,
            public_es,
            public_ct,
            public_bcn,
            _is_school_holiday(current, easter),
            False,  # se completa en una segunda pasada
            _is_summer(current),
            _is_christmas(current),
            _is_rebajas_winter(current),
            _is_rebajas_summer(current),
            season,
        )
        yield row
        current += timedelta(days=1)


def _easter_sunday(year: int) -> date:
    a = year % 19
    b = year // 100
    c = year % 100
    d = b // 4
    e = b % 4
    f = (b + 8) // 25
    g = (b - f + 1) // 3
    h = (19 * a + b - d - g + 15) % 30
    i = c // 4
    k = c % 4
    l = (32 + 2 * e + 2 * i - h - k) % 7
    m = (a + 11 * h + 22 * l) // 451
    month = (h + l - 7 * m + 114) // 31
    day = ((h + l - 7 * m + 114) % 31) + 1
    return date(year, month, day)


def _is_public_holiday_es(current: date, easter: date) -> bool:
    if (current.month, current.day) in _ES_FIXED:
        return True
    return current in {
        easter - timedelta(days=2),  # Viernes Santo
    }


def _is_public_holiday_ct(current: date, easter: date) -> bool:
    if (current.month, current.day) in _CT_FIXED:
        return True
    return current in {
        easter + timedelta(days=1),  # Lunes de Pascua
    }


def _is_public_holiday_bcn(current: date) -> bool:
    if (current.month, current.day) in _BCN_FIXED:
        return True
    if current.month == 5 and current.weekday() == 1 and 20 <= current.day <= 26:
        return True  # Santa Eulalia / fiestas locales aproximadas
    return False


def _is_school_holiday(current: date, easter: date) -> bool:
    if _is_summer(current) or _is_christmas(current):
        return True
    easter_week_start = easter - timedelta(days=6)
    easter_week_end = easter + timedelta(days=1)
    return easter_week_start <= current <= easter_week_end


def _is_summer(current: date) -> bool:
    return (current.month == 6 and current.day >= 22) or current.month in {7, 8} or (current.month == 9 and current.day <= 10)


def _is_christmas(current: date) -> bool:
    return (current.month == 12 and current.day >= 22) or (current.month == 1 and current.day <= 7)


def _is_rebajas_winter(current: date) -> bool:
    return current.month in {1, 2}


def _is_rebajas_summer(current: date) -> bool:
    return current.month in {7, 8}


def _season_name(current: date) -> str:
    if current.month in {12, 1, 2}:
        return "winter"
    if current.month in {3, 4, 5}:
        return "spring"
    if current.month in {6, 7, 8}:
        return "summer"
    return "autumn"


async def _read_aforaments_weekly_lifts() -> dict[str, dict[str, float]]:
    if not _CSV_IMD.exists() or not _CSV_DESCRIPCIO.exists():
        logger.info("Temporalidad: sin CSVs de aforaments, omitiendo weekend_lift")
        return {}

    values_by_sensor = defaultdict(lambda: defaultdict(list))
    with _CSV_IMD.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                tipo = int(row.get("Codi_tipus_dia", -1) or -1)
                if tipo not in {2, 4, 5}:
                    continue
                sensor_id = str(row.get("Id_aforament", "") or "").strip().strip('"')
                value = float(row.get("Valor_IMD", 0) or 0)
                if sensor_id and value > 0:
                    values_by_sensor[sensor_id][tipo].append(value)
            except (TypeError, ValueError):
                continue

    coords = {}
    with _CSV_DESCRIPCIO.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            try:
                sensor_id = str(row.get("Id_aforament", "") or "").strip().strip('"')
                lng = float(row.get("Longitud", 0) or 0)
                lat = float(row.get("Latitud", 0) or 0)
                if sensor_id and (lng != 0.0 or lat != 0.0):
                    coords[sensor_id] = (lng, lat)
            except (TypeError, ValueError):
                continue

    sensor_profiles = {}
    for sensor_id, day_values in values_by_sensor.items():
        if sensor_id not in coords:
            continue
        laborable = _avg(day_values.get(2))
        sabado = _avg(day_values.get(4))
        domingo = _avg(day_values.get(5))
        if laborable is None:
            continue
        sensor_profiles[sensor_id] = {
            "lng": coords[sensor_id][0],
            "lat": coords[sensor_id][1],
            "laborable": laborable,
            "sabado": sabado,
            "domingo": domingo,
        }

    if not sensor_profiles:
        return {}

    return await _project_sensor_profiles_to_zones(sensor_profiles)


def _avg(values: list[float] | None) -> float | None:
    if not values:
        return None
    return float(sum(values) / len(values))


async def _project_sensor_profiles_to_zones(sensor_profiles: dict[str, dict[str, float]]) -> dict[str, dict[str, float]]:
    results: dict[str, dict[str, float]] = defaultdict(lambda: {"laborable": 0.0, "sabado": 0.0, "domingo": 0.0, "peso": 0.0})
    async with get_db() as conn:
        for profile in sensor_profiles.values():
            rows = await conn.fetch(
                """
                SELECT
                    z.id AS zona_id,
                    ST_Distance(
                        z.geometria::geography,
                        ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography
                    )::float AS distancia_m
                FROM zonas z
                WHERE ST_DWithin(
                    z.geometria::geography,
                    ST_SetSRID(ST_MakePoint($1, $2), 4326)::geography,
                    $3
                )
                ORDER BY distancia_m ASC
                LIMIT $4
                """,
                profile["lng"],
                profile["lat"],
                _RADIO_M,
                _MAX_ZONAS,
            )
            if not rows:
                continue

            weights = [1.0 / max(1.0, float(row["distancia_m"])) ** 2 for row in rows]
            weight_sum = sum(weights) or 1.0
            for row, weight in zip(rows, weights):
                zona_id = str(row["zona_id"])
                frac = weight / weight_sum
                agg = results[zona_id]
                agg["laborable"] += profile["laborable"] * frac
                if profile["sabado"] is not None:
                    agg["sabado"] += profile["sabado"] * frac
                if profile["domingo"] is not None:
                    agg["domingo"] += profile["domingo"] * frac
                agg["peso"] += frac

    formatted: dict[str, dict[str, float]] = {}
    for zona_id, agg in results.items():
        laborable = agg["laborable"] or None
        sabado = agg["sabado"] or None
        domingo = agg["domingo"] or None
        if laborable is None:
            continue
        weekend_lift = None
        if sabado is not None or domingo is not None:
            weekend_values = [value for value in (sabado, domingo) if value is not None]
            if weekend_values:
                weekend_lift = (sum(weekend_values) / len(weekend_values)) / laborable
        sunday_lift = (domingo / laborable) if domingo is not None else None
        formatted[zona_id] = {
            "weekend_lift": round(weekend_lift, 4) if weekend_lift is not None else None,
            "sunday_lift": round(sunday_lift, 4) if sunday_lift is not None else None,
        }
    return formatted


async def _popular_times_metrics() -> dict[str, dict[str, float]]:
    from pipelines.inmobiliario.scraping.gosom_client import popular_times_to_weekly_profile

    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT zona_id, popular_times
            FROM negocios_activos
            WHERE activo = TRUE
              AND popular_times IS NOT NULL
            """
        )

    by_zone: dict[str, list[dict[str, float]]] = defaultdict(list)
    for row in rows:
        pt = row["popular_times"]
        if isinstance(pt, str):
            try:
                pt = json.loads(pt)
            except json.JSONDecodeError:
                continue
        if not isinstance(pt, dict):
            continue
        metrics = _popular_times_dayparts(pt)
        if metrics:
            metrics["weekly_profile"] = popular_times_to_weekly_profile(pt)
            by_zone[str(row["zona_id"])].append(metrics)

    aggregated: dict[str, dict[str, float]] = {}
    for zona_id, metrics_list in by_zone.items():
        aggregated[zona_id] = {
            "weekday_midday_share": round(_safe_mean(item.get("weekday_midday_share") for item in metrics_list), 4),
            "weekend_evening_share": round(_safe_mean(item.get("weekend_evening_share") for item in metrics_list), 4),
            "late_night_share": round(_safe_mean(item.get("late_night_share") for item in metrics_list), 4),
        }
    return aggregated


def _popular_times_dayparts(popular_times: dict) -> dict[str, float]:
    totals = {"all": 0.0, "weekday_midday": 0.0, "weekend_evening": 0.0, "late_night": 0.0}

    for day_name in _DAY_NAMES:
        hours = popular_times.get(day_name)
        if not isinstance(hours, dict):
            continue
        for raw_hour, raw_value in hours.items():
            try:
                hour = int(raw_hour)
                value = float(raw_value)
            except (TypeError, ValueError):
                continue
            if value < 0:
                continue
            totals["all"] += value
            if day_name in _DAY_NAMES[:5] and 11 <= hour < 15:
                totals["weekday_midday"] += value
            if day_name in {"Saturday", "Sunday"} and 18 <= hour < 23:
                totals["weekend_evening"] += value
            if day_name in {"Friday", "Saturday"} and (hour >= 22 or hour <= 2):
                totals["late_night"] += value

    total = totals["all"] or 0.0
    if total <= 0:
        return {}
    return {
        "weekday_midday_share": totals["weekday_midday"] / total,
        "weekend_evening_share": totals["weekend_evening"] / total,
        "late_night_share": totals["late_night"] / total,
    }


async def _available_seasonality_snapshot_dates(
    *,
    start_year: int | None = None,
    end_year: int | None = None,
) -> list[date]:
    conditions = ["any IS NOT NULL", "mes IS NOT NULL", "mes BETWEEN 1 AND 12"]
    params: list[int] = []
    if start_year is not None:
        params.append(start_year)
        conditions.append(f"any >= ${len(params)}")
    if end_year is not None:
        params.append(end_year)
        conditions.append(f"any <= ${len(params)}")

    query = f"""
        SELECT DISTINCT make_date(any, mes, 1) AS snapshot_date
        FROM vianants_trams
        WHERE {' AND '.join(conditions)}
        ORDER BY snapshot_date
    """
    async with get_db() as conn:
        rows = await conn.fetch(query, *params)
    return [row["snapshot_date"] for row in rows if row["snapshot_date"] is not None]


async def _seasonality_metrics(
    *,
    as_of_date: date | None = None,
) -> dict[str, dict[str, float]]:
    params: list[date] = []
    filter_sql = ""
    if as_of_date is not None:
        params.append(as_of_date)
        filter_sql = "AND make_date(any, mes, 1) < $1"

    query = f"""
        SELECT zona_id, any, mes, AVG(intensitat)::float AS intensidad
        FROM vianants_trams
        WHERE zona_id IS NOT NULL
          AND intensitat IS NOT NULL
          AND intensitat > 0
          {filter_sql}
        GROUP BY zona_id, any, mes
        ORDER BY zona_id, any, mes
    """
    async with get_db() as conn:
        rows = await conn.fetch(query, *params)

    by_zone: dict[str, list[tuple[int, int, float]]] = defaultdict(list)
    for row in rows:
        by_zone[str(row["zona_id"])].append((int(row["any"]), int(row["mes"]), float(row["intensidad"])))

    result: dict[str, dict[str, float]] = {}
    for zona_id, series in by_zone.items():
        values = [item[2] for item in series if item[2] > 0]
        if len(values) < 6:
            continue
        ref = median(values) or 1.0
        indexed = [(year, month, value / ref) for year, month, value in series if value > 0]
        if not indexed:
            continue

        summer = [idx for _, month, idx in indexed if month in {6, 7, 8, 9}]
        christmas = [idx for _, month, idx in indexed if month in {12, 1}]
        rebajas = [idx for _, month, idx in indexed if month in {1, 2, 7, 8}]
        idx_values = [idx for _, _, idx in indexed]
        mean_idx = sum(idx_values) / len(idx_values)
        result[zona_id] = {
            "seasonality_summer_lift": round(_safe_mean(summer), 4) if summer else None,
            "seasonality_christmas_lift": round(_safe_mean(christmas), 4) if christmas else None,
            "seasonality_rebajas_lift": round(_safe_mean(rebajas), 4) if rebajas else None,
            "seasonality_volatility": round(float(pstdev(idx_values)), 4) if len(idx_values) > 1 else 0.0,
            "seasonality_peak_concentration": round(float(max(idx_values) / max(mean_idx, 1e-6)), 4),
        }
    return result


async def _latest_tourism_scores() -> dict[str, float]:
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT DISTINCT ON (zona_id)
                zona_id,
                score_turismo
            FROM vz_turismo
            ORDER BY zona_id, fecha DESC
            """
        )
    return {
        str(row["zona_id"]): float(row["score_turismo"])
        for row in rows
        if row["score_turismo"] is not None
    }


async def _festive_transport_ratio() -> dict[str, float]:
    try:
        async with get_db() as conn:
            rows = await conn.fetch(
                """
                SELECT
                    z.id AS zona_id,
                    AVG(CASE WHEN ft.dia_tipo = 'laborable' THEN 1.0 / NULLIF(ft.frecuencia_min, 0) END)::float AS laborable_servicio,
                    AVG(CASE WHEN ft.dia_tipo = 'festivo' THEN 1.0 / NULLIF(ft.frecuencia_min, 0) END)::float AS festivo_servicio
                FROM zonas z
                JOIN paradas_transporte pt
                  ON ST_DWithin(pt.geometria::geography, ST_Centroid(z.geometria)::geography, 500)
                JOIN paradas_lineas pl ON pl.parada_id = pt.id
                JOIN frecuencias_transporte ft ON ft.linea_id = pl.linea_id
                GROUP BY z.id
                """
            )
    except Exception as exc:  # pragma: no cover - depende de tablas externas
        logger.info("Temporalidad: ratio festivo transporte no disponible: %s", exc)
        return {}

    ratios: dict[str, float] = {}
    for row in rows:
        laborable = row["laborable_servicio"]
        festivo = row["festivo_servicio"]
        if laborable and festivo:
            ratios[str(row["zona_id"])] = round(float(festivo / laborable), 4)
    return ratios


def _build_holiday_proxy(
    *,
    weekend_lift: float | None,
    weekend_evening_share: float | None,
    score_turismo: float | None,
    festive_transport_ratio: float | None,
) -> float | None:
    components: list[tuple[float, float]] = []
    if weekend_lift is not None:
        components.append((0.45, _clip01((weekend_lift - 0.70) / 0.80)))
    if score_turismo is not None:
        components.append((0.25, _clip01(score_turismo / 100.0)))
    if weekend_evening_share is not None:
        components.append((0.15, _clip01(weekend_evening_share / 0.35)))
    if festive_transport_ratio is not None:
        components.append((0.15, _clip01(festive_transport_ratio)))

    if not components:
        return None

    total_weight = sum(weight for weight, _ in components)
    score = sum(weight * value for weight, value in components) / max(total_weight, 1e-6)
    return round(score * 100.0, 1)


def _build_temporal_confidence(payload: dict[str, float | None]) -> float:
    availability = [
        payload.get("weekend_lift") is not None,
        payload.get("weekday_midday_share") is not None,
        payload.get("seasonality_summer_lift") is not None,
        payload.get("seasonality_volatility") is not None,
    ]
    if not availability:
        return 0.0
    return round(sum(1.0 for flag in availability if flag) / len(availability), 3)


async def _upsert_temporal_row(conn, zona_id: str, fecha_ref: date, payload: dict[str, float | None]) -> None:
    await conn.execute(
        """
        INSERT INTO variables_zona (zona_id, fecha, fuente)
        VALUES ($1, $2, 'temporalidad_bcn')
        ON CONFLICT (zona_id, fecha) DO UPDATE
        SET fuente = EXCLUDED.fuente,
            updated_at = NOW()
        """,
        zona_id,
        fecha_ref,
    )

    await conn.execute(
        """
        INSERT INTO vz_flujo (
            zona_id, fecha, fuente,
            weekend_lift, sunday_lift,
            weekday_midday_share, weekend_evening_share, late_night_share,
            holiday_proxy_score, temporal_confianza,
            seasonality_summer_lift, seasonality_christmas_lift,
            seasonality_rebajas_lift, seasonality_volatility,
            seasonality_peak_concentration
        )
        VALUES (
            $1, $2, 'temporalidad_bcn',
            $3, $4,
            $5, $6, $7,
            $8, $9,
            $10, $11,
            $12, $13,
            $14
        )
        ON CONFLICT (zona_id, fecha) DO UPDATE SET
            fuente = EXCLUDED.fuente,
            weekend_lift = COALESCE(EXCLUDED.weekend_lift, vz_flujo.weekend_lift),
            sunday_lift = COALESCE(EXCLUDED.sunday_lift, vz_flujo.sunday_lift),
            weekday_midday_share = COALESCE(EXCLUDED.weekday_midday_share, vz_flujo.weekday_midday_share),
            weekend_evening_share = COALESCE(EXCLUDED.weekend_evening_share, vz_flujo.weekend_evening_share),
            late_night_share = COALESCE(EXCLUDED.late_night_share, vz_flujo.late_night_share),
            holiday_proxy_score = COALESCE(EXCLUDED.holiday_proxy_score, vz_flujo.holiday_proxy_score),
            temporal_confianza = COALESCE(EXCLUDED.temporal_confianza, vz_flujo.temporal_confianza),
            seasonality_summer_lift = COALESCE(EXCLUDED.seasonality_summer_lift, vz_flujo.seasonality_summer_lift),
            seasonality_christmas_lift = COALESCE(EXCLUDED.seasonality_christmas_lift, vz_flujo.seasonality_christmas_lift),
            seasonality_rebajas_lift = COALESCE(EXCLUDED.seasonality_rebajas_lift, vz_flujo.seasonality_rebajas_lift),
            seasonality_volatility = COALESCE(EXCLUDED.seasonality_volatility, vz_flujo.seasonality_volatility),
            seasonality_peak_concentration = COALESCE(EXCLUDED.seasonality_peak_concentration, vz_flujo.seasonality_peak_concentration),
            updated_at = NOW()
        """,
        zona_id,
        fecha_ref,
        payload.get("weekend_lift"),
        payload.get("sunday_lift"),
        payload.get("weekday_midday_share"),
        payload.get("weekend_evening_share"),
        payload.get("late_night_share"),
        payload.get("holiday_proxy_score"),
        payload.get("temporal_confianza"),
        payload.get("seasonality_summer_lift"),
        payload.get("seasonality_christmas_lift"),
        payload.get("seasonality_rebajas_lift"),
        payload.get("seasonality_volatility"),
        payload.get("seasonality_peak_concentration"),
    )


def _safe_mean(values: Iterable[float | None]) -> float:
    clean = [float(value) for value in values if value is not None]
    if not clean:
        return 0.0
    return sum(clean) / len(clean)


def _clip01(value: float) -> float:
    return max(0.0, min(1.0, float(value)))


async def _init(pipeline: str) -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            """
            INSERT INTO pipeline_ejecuciones (pipeline, estado)
            VALUES ($1, 'running')
            RETURNING id
            """,
            pipeline,
        )


async def _fin(eid: int, registros: int, estado: str, mensaje: str | None = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE pipeline_ejecuciones
            SET fecha_fin = NOW(),
                registros = $1,
                estado = $2,
                mensaje_error = $3
            WHERE id = $4
            """,
            registros,
            estado,
            mensaje,
            eid,
        )
