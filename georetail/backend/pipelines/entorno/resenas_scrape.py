"""
Backfill de resenas para negocios_activos usando gosom/google-maps-scraper.

El pipeline procesa negocios activos pendientes en lotes, lanza gosom con
--extra-reviews, inserta resenas deduplicadas y marca el estado por negocio.
"""
from __future__ import annotations

import asyncio
import logging
import math
import re
import unicodedata
from datetime import date
from pathlib import Path
from typing import Any, Iterable, Optional

from config import get_settings
from db.conexion import get_db
from pipelines.inmobiliario.scraping import gosom_runner

logger = logging.getLogger(__name__)

_ESTADO_PENDIENTE = "pendiente"
_ESTADO_OK = "ok"
_ESTADO_SIN_RESENAS = "sin_resenas"
_ESTADO_SIN_MATCH = "sin_match"
_ESTADO_ERROR = "error"


async def ejecutar(
    max_negocios: Optional[int] = None,
    batch_size: Optional[int] = None,
    pausa_s: Optional[float] = None,
    output_file: Optional[str | Path] = None,
) -> dict[str, Any]:
    """Ejecuta el backfill. max_negocios=0/None usa el valor de settings."""
    settings = get_settings()
    effective_max = settings.RESENAS_SCRAPE_MAX_NEGOCIOS if max_negocios is None else max_negocios
    effective_batch = batch_size or settings.RESENAS_SCRAPE_BATCH_SIZE or 50
    effective_pause = settings.RESENAS_SCRAPE_PAUSA_S if pausa_s is None else pausa_s
    fixture_output = Path(output_file) if output_file else None

    eid = await _registrar_inicio()
    stats = {
        "negocios_intentados": 0,
        "negocios_con_resenas": 0,
        "resenas_insertadas": 0,
        "sin_match": 0,
        "sin_resenas": 0,
        "errores": 0,
        "batches": 0,
        "procesadas_nlp": 0,
    }

    try:
        if getattr(settings, "GOSOM_MODE", "disabled") == "disabled" and fixture_output is None:
            logger.info("resenas_scrape: GOSOM_MODE=disabled; no se modifican estados")
            stats["skipped"] = True
            await _registrar_fin(eid, 0, "ok")
            return stats

        while True:
            restante = None if not effective_max else max(effective_max - stats["negocios_intentados"], 0)
            if restante == 0:
                break

            limit = min(effective_batch, restante) if restante is not None else effective_batch
            negocios = await _negocios_pendientes(limit)
            if not negocios:
                break

            stats["batches"] += 1
            stats["negocios_intentados"] += len(negocios)

            try:
                batch_stats = await _procesar_batch(
                    negocios,
                    batch_num=stats["batches"],
                    output_file=fixture_output,
                    settings=settings,
                )
                _sumar_stats(stats, batch_stats)
            except Exception as exc:
                logger.exception("resenas_scrape: batch %s fallo: %s", stats["batches"], exc)
                stats["errores"] += len(negocios)
                for negocio in negocios:
                    await _marcar_estado(negocio["id"], _ESTADO_ERROR, str(exc)[:500])

            if effective_pause and fixture_output is None:
                await asyncio.sleep(effective_pause)

        stats["procesadas_nlp"] = await _procesar_nlp_pendiente()
        await _registrar_fin(eid, stats["resenas_insertadas"], "ok")
        return stats

    except Exception as exc:
        await _registrar_fin(eid, stats["resenas_insertadas"], "error", str(exc)[:1000])
        raise


async def _procesar_batch(
    negocios: list[dict[str, Any]],
    *,
    batch_num: int,
    output_file: Optional[Path],
    settings: Any,
) -> dict[str, int]:
    stats = {
        "negocios_con_resenas": 0,
        "resenas_insertadas": 0,
        "sin_match": 0,
        "sin_resenas": 0,
        "errores": 0,
    }

    out_dir = Path(settings.GOSOM_OUTPUT_DIR)
    out_dir.mkdir(parents=True, exist_ok=True)
    queries_file = out_dir / f"resenas_scrape_batch_{batch_num}.txt"
    queries_file.write_text("\n".join(_query_negocio(n) for n in negocios), encoding="utf-8")

    target_output = output_file or (out_dir / f"resenas_scrape_batch_{batch_num}.jsonl")
    if output_file is None:
        result = await gosom_runner.ejecutar(queries_file, target_output, extra_reviews=True)
        if result.exit_code not in (0, -1):
            raise RuntimeError(result.stderr_tail or f"gosom exit_code={result.exit_code}")

    rows = list(gosom_runner.parsear_reviews_dump(target_output))
    if not rows:
        raise RuntimeError(f"gosom no produjo dump de resenas en {target_output}")

    matches = _match_rows(negocios, rows)
    for negocio in negocios:
        row = matches.get(negocio["id"])
        if row is None:
            await _marcar_estado(negocio["id"], _ESTADO_SIN_MATCH)
            stats["sin_match"] += 1
            continue

        reviews = row.get("reviews") or []
        if not reviews:
            await _actualizar_negocio_scrape(negocio, row, _ESTADO_SIN_RESENAS)
            stats["sin_resenas"] += 1
            continue

        try:
            insertadas = await _insertar_reviews(negocio, row, reviews)
            await _actualizar_negocio_scrape(negocio, row, _ESTADO_OK)
            stats["negocios_con_resenas"] += 1
            stats["resenas_insertadas"] += insertadas
        except Exception as exc:
            logger.warning("resenas_scrape: negocio=%s error insertando: %s", negocio["id"], exc)
            await _marcar_estado(negocio["id"], _ESTADO_ERROR, str(exc)[:500])
            stats["errores"] += 1

    return stats


async def _negocios_pendientes(limite: int) -> list[dict[str, Any]]:
    async with get_db() as conn:
        rows = await conn.fetch(
            """
            SELECT id, nombre, zona_id, lat, lng, google_place_id, rating, total_resenas, review_count
            FROM negocios_activos
            WHERE es_activo = TRUE
              AND COALESCE(resenas_scrape_estado, 'pendiente') = 'pendiente'
            ORDER BY COALESCE(total_resenas, review_count, 0) DESC, updated_at ASC
            LIMIT $1
            """,
            limite,
        )
    return [dict(r) for r in rows]


async def _insertar_reviews(negocio: dict[str, Any], row: dict[str, Any], reviews: list[dict[str, Any]]) -> int:
    insertadas = 0
    async with get_db() as conn:
        for review in reviews:
            texto = (review.get("texto") or "").strip()
            if not texto:
                continue
            rid = gosom_runner.review_stable_id(negocio["id"], review)
            result = await conn.execute(
                """
                INSERT INTO resenas
                    (id, negocio_id, zona_id, texto, rating, fecha, idioma, fuente,
                     autor, url, metadata, scraped_at, procesada)
                VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,NOW(),FALSE)
                ON CONFLICT (id) DO NOTHING
                """,
                rid,
                negocio["id"],
                negocio.get("zona_id"),
                texto,
                _safe_rating(review.get("rating")),
                _safe_date(review.get("fecha")),
                review.get("idioma") or "es",
                review.get("fuente") or "google_scrape",
                review.get("autor"),
                review.get("url"),
                _metadata(row, review),
            )
            if result != "INSERT 0 0":
                insertadas += 1
    return insertadas


async def _actualizar_negocio_scrape(negocio: dict[str, Any], row: dict[str, Any], estado: str) -> None:
    rating = _safe_rating(row.get("rating"))
    total = _safe_int(row.get("num_resenas"))
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE negocios_activos SET
                rating = COALESCE($2, rating),
                total_resenas = GREATEST(COALESCE(total_resenas, 0), COALESCE($3, 0)),
                review_count = GREATEST(COALESCE(review_count, 0), COALESCE($3, 0)),
                google_place_id = COALESCE($4, google_place_id),
                resenas_scrape_estado = $5,
                resenas_scrape_at = NOW(),
                resenas_scrape_error = NULL,
                updated_at = NOW()
            WHERE id = $1
            """,
            negocio["id"],
            rating,
            total,
            row.get("place_id"),
            estado,
        )


async def _marcar_estado(negocio_id: str, estado: str, error: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE negocios_activos SET
                resenas_scrape_estado = $2,
                resenas_scrape_at = NOW(),
                resenas_scrape_error = $3,
                updated_at = NOW()
            WHERE id = $1
            """,
            negocio_id,
            estado,
            error,
        )


def _match_rows(negocios: list[dict[str, Any]], rows: list[dict[str, Any]]) -> dict[str, dict[str, Any]]:
    by_id: dict[str, dict[str, Any]] = {}
    for row in rows:
        for key in _row_ids(row):
            by_id[key] = row

    matches: dict[str, dict[str, Any]] = {}
    used_rows: set[int] = set()
    for negocio in negocios:
        for key in _negocio_ids(negocio):
            row = by_id.get(key)
            if row is not None:
                matches[negocio["id"]] = row
                used_rows.add(id(row))
                break

    for negocio in negocios:
        if negocio["id"] in matches:
            continue
        best_row: Optional[dict[str, Any]] = None
        best_score = 0.0
        for row in rows:
            if id(row) in used_rows:
                continue
            score = _match_score(negocio, row)
            if score > best_score:
                best_score = score
                best_row = row
        if best_row is not None and best_score >= 0.68:
            matches[negocio["id"]] = best_row
            used_rows.add(id(best_row))
    return matches


def _match_score(negocio: dict[str, Any], row: dict[str, Any]) -> float:
    name_score = _name_similarity(negocio.get("nombre"), row.get("nombre"))
    dist = _distance_m(negocio.get("lat"), negocio.get("lng"), row.get("lat"), row.get("lng"))
    if dist is None:
        return name_score * 0.9
    if dist <= 80:
        return min(1.0, name_score + 0.3)
    if dist <= 180:
        return name_score * 0.8
    return name_score * 0.45


def _row_ids(row: dict[str, Any]) -> set[str]:
    ids = set()
    for key in ("source_id", "place_id", "cid", "data_id"):
        value = row.get(key)
        if value:
            raw = str(value)
            ids.add(raw)
            ids.add(f"gs_{raw}")
            ids.add(f"gp_{raw}")
    return ids


def _negocio_ids(negocio: dict[str, Any]) -> set[str]:
    ids = set()
    for key in ("id", "google_place_id"):
        value = negocio.get(key)
        if value:
            raw = str(value)
            ids.add(raw)
            ids.add(raw.removeprefix("gs_").removeprefix("gp_"))
            ids.add(f"gs_{raw}")
            ids.add(f"gp_{raw}")
    return ids


def _query_negocio(negocio: dict[str, Any]) -> str:
    nombre = str(negocio.get("nombre") or "").strip()
    lat = negocio.get("lat")
    lng = negocio.get("lng")
    if lat is not None and lng is not None:
        return f"{nombre}, {lat}, {lng}"
    return f"{nombre}, Barcelona, Spain"


def _name_similarity(a: Optional[str], b: Optional[str]) -> float:
    ta = set(_tokens(a))
    tb = set(_tokens(b))
    if not ta or not tb:
        return 0.0
    return len(ta & tb) / len(ta | tb)


def _tokens(text: Optional[str]) -> list[str]:
    if not text:
        return []
    normalized = unicodedata.normalize("NFKD", text)
    normalized = "".join(ch for ch in normalized if not unicodedata.combining(ch)).lower()
    return [t for t in re.split(r"[^a-z0-9]+", normalized) if len(t) > 1]


def _distance_m(lat1: Any, lng1: Any, lat2: Any, lng2: Any) -> Optional[float]:
    try:
        lat1_f, lng1_f, lat2_f, lng2_f = map(float, (lat1, lng1, lat2, lng2))
    except (TypeError, ValueError):
        return None
    radius = 6371000.0
    p1 = math.radians(lat1_f)
    p2 = math.radians(lat2_f)
    dp = math.radians(lat2_f - lat1_f)
    dl = math.radians(lng2_f - lng1_f)
    a = math.sin(dp / 2) ** 2 + math.cos(p1) * math.cos(p2) * math.sin(dl / 2) ** 2
    return radius * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def _metadata(row: dict[str, Any], review: dict[str, Any]) -> dict[str, Any]:
    meta = review.get("metadata") if isinstance(review.get("metadata"), dict) else {}
    return {
        **meta,
        "scrape_place_id": row.get("place_id"),
        "scrape_source_id": row.get("source_id"),
        "scrape_nombre": row.get("nombre"),
    }


def _safe_rating(value: Any) -> Optional[float]:
    try:
        rating = float(value)
    except (TypeError, ValueError):
        return None
    return rating if 0 <= rating <= 5 else None


def _safe_int(value: Any) -> Optional[int]:
    try:
        return int(value) if value is not None else None
    except (TypeError, ValueError):
        return None


def _safe_date(value: Any) -> Optional[date | str]:
    if value is None:
        return None
    if isinstance(value, date):
        return value
    text = str(value).strip()
    if re.match(r"^\d{4}-\d{2}-\d{2}$", text):
        return text
    return None


def _sumar_stats(destino: dict[str, int], parcial: dict[str, int]) -> None:
    for key, value in parcial.items():
        destino[key] = destino.get(key, 0) + value


async def _procesar_nlp_pendiente() -> int:
    try:
        from nlp.clasificador import procesar_resenas_pendientes

        return await procesar_resenas_pendientes(limite=1000)
    except Exception as exc:
        logger.warning("resenas_scrape: NLP pendiente no se pudo procesar: %s", exc)
        return 0


async def _registrar_inicio() -> int:
    async with get_db() as conn:
        return await conn.fetchval(
            "INSERT INTO pipeline_ejecuciones(pipeline, estado) VALUES('resenas_scrape', 'running') RETURNING id"
        )


async def _registrar_fin(eid: int, registros: int, estado: str, mensaje: Optional[str] = None) -> None:
    async with get_db() as conn:
        await conn.execute(
            """
            UPDATE pipeline_ejecuciones
            SET fecha_fin = NOW(), registros = $1, estado = $2, mensaje_error = $3
            WHERE id = $4
            """,
            registros,
            estado,
            mensaje,
            eid,
        )
