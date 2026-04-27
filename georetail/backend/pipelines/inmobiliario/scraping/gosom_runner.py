"""
pipelines/scraping/gosom_runner.py — Wrapper de `gosom/google-maps-scraper`.

El scraper es un binario Go (o imagen Docker); lo invocamos por subprocess,
esperamos a que termine y parseamos el JSONL que deja en disco. No intentamos
controlar rate-limits ni anti-bot — eso es responsabilidad del propio binario.

Tres piezas públicas:

  - `generar_queries(zonas, sectores)`  → fichero que espera gosom como input.
  - `ejecutar(queries_file, output_file)` → lanza el proceso y devuelve exit code.
  - `parsear_dump(output_file, sectores)` → itera el JSONL y emite dicts
     normalizados al mismo shape que `routers.places_router._buscar_google`,
     con `fuente="google_scrape"` para poder auditar la procedencia.

La lógica de ingesta (upsert en `negocios_activos`, recálculo de
`competencia_por_local`) vive en `pipelines.comercio.competencia_scrape`.
"""
from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import os
import shlex
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable, Iterator, Optional

from config import get_settings
from scoring.taxonomia import clasificar, clasificar_con_fallback

logger = logging.getLogger(__name__)


# ─── Términos que gosom mete en el buscador ──────────────────────────────────

# Términos de input para el binario gosom — en español + inglés para maximizar
# cobertura de Google Maps. El mapeo inverso (categoría → sector/subsector) vive
# en `scoring/taxonomia.py`, no aquí, para que la taxonomía sea única en todo
# el codebase.
_TERMINOS_BUSQUEDA: dict[str, list[str]] = {
    "restauracion":  ["restaurante", "cafetería", "bar"],
    "moda":          ["tienda de ropa", "zapatería"],
    "estetica":      ["peluquería", "salón de belleza"],
    "tatuajes":      ["estudio de tatuajes"],
    "shisha_lounge": ["shisha lounge", "salón de shisha"],
}


def sector_desde_categoria(categoria: Optional[str]) -> Optional[str]:
    """Mantenido por retrocompatibilidad con tests/consumidores antiguos.
    Delega en `scoring.taxonomia.clasificar`."""
    sector, _ = clasificar(categoria)
    return sector


# ─── Generación del fichero de queries ───────────────────────────────────────

@dataclass
class QueryZona:
    zona_id: str
    sector: str
    termino: str
    # El anclaje geográfico lo resolvemos con el barrio/distrito en el propio
    # texto; gosom también admite `--geo lat,lng,zoom` pero sólo global por
    # ejecución, así que por query usamos texto.
    ubicacion_texto: str


def generar_queries(zonas: Iterable[dict], sectores: Iterable[str]) -> list[QueryZona]:
    """
    Crea una `QueryZona` por (zona, sector, término). Una zona con 5 sectores
    y 3 términos por sector genera 15 queries. Gosom dedupe al escribirlas al
    fichero de entrada.

    Cada zona debe traer al menos `zona_id` y `nombre_barrio` o
    `ubicacion_texto` listos para pegar en el buscador.
    """
    queries: list[QueryZona] = []
    for zona in zonas:
        zid = zona["zona_id"]
        ubicacion = (
            zona.get("ubicacion_texto")
            or _componer_ubicacion(zona.get("nombre_barrio"), zona.get("nombre_distrito"))
        )
        if not ubicacion:
            logger.warning("gosom: zona %s sin texto de ubicación → salto", zid)
            continue
        for sector in sectores:
            for termino in _TERMINOS_BUSQUEDA.get(sector, []):
                queries.append(QueryZona(
                    zona_id=zid, sector=sector, termino=termino,
                    ubicacion_texto=ubicacion,
                ))
    return queries


def _componer_ubicacion(barrio: Optional[str], distrito: Optional[str]) -> Optional[str]:
    # Sólo generamos la query si tenemos barrio o distrito — "Barcelona, Spain"
    # a secas es demasiado amplio para Maps y devuelve ruido.
    partes = [p for p in (barrio, distrito) if p]
    if not partes:
        return None
    partes.append("Barcelona, Spain")
    return ", ".join(partes)


def escribir_queries_file(queries: list[QueryZona], ruta: Path) -> Path:
    """Volcado a fichero — una línea por query en el formato que espera gosom."""
    ruta.parent.mkdir(parents=True, exist_ok=True)
    ruta.write_text("\n".join(f"{q.termino}, {q.ubicacion_texto}" for q in queries),
                    encoding="utf-8")
    return ruta


# ─── Ejecución del binario/Docker ────────────────────────────────────────────

@dataclass
class GosomResult:
    exit_code: int
    stdout_tail: str
    stderr_tail: str
    output_path: Path


async def ejecutar(queries_file: Path,
                    output_file: Optional[Path] = None,
                    *,
                    extra_reviews: bool = False) -> GosomResult:
    """
    Lanza gosom y bloquea hasta que termine.

    Modo disabled → no ejecuta nada; devuelve exit_code=-1. Así los tests y el
    entorno dev sin binario pueden correr el pipeline de ingesta contra un
    dump pre-existente.
    """
    s = get_settings()
    if s.GOSOM_MODE == "disabled":
        logger.info("gosom: GOSOM_MODE=disabled — no se lanza el scraper")
        return GosomResult(exit_code=-1, stdout_tail="", stderr_tail="",
                           output_path=output_file or Path(s.GOSOM_OUTPUT_DIR) / "out.jsonl")

    output_file = output_file or (Path(s.GOSOM_OUTPUT_DIR) / "out.jsonl")
    output_file.parent.mkdir(parents=True, exist_ok=True)

    cmd = _construir_comando(s, queries_file, output_file, extra_reviews=extra_reviews)
    logger.info("gosom: lanzando: %s", " ".join(shlex.quote(c) for c in cmd))

    try:
        proc = await asyncio.create_subprocess_exec(
            *cmd,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        timeout = s.GOSOM_TIMEOUT_S or None
        try:
            stdout, stderr = await asyncio.wait_for(proc.communicate(), timeout=timeout)
        except asyncio.TimeoutError:
            proc.kill()
            await proc.communicate()
            raise

        exit_code = proc.returncode if proc.returncode is not None else -2
    except FileNotFoundError as e:
        logger.error("gosom: binario no encontrado: %s", e)
        return GosomResult(exit_code=127, stdout_tail="", stderr_tail=str(e),
                           output_path=output_file)

    stdout_str = (stdout or b"").decode(errors="replace")
    stderr_str = (stderr or b"").decode(errors="replace")
    if exit_code != 0:
        logger.warning("gosom: exit=%s stderr=%s", exit_code, stderr_str[-500:])
    else:
        logger.info("gosom: OK — dump en %s", output_file)

    return GosomResult(
        exit_code=exit_code,
        stdout_tail=stdout_str[-500:],
        stderr_tail=stderr_str[-500:],
        output_path=output_file,
    )


def _construir_comando(
    s,
    queries_file: Path,
    output_file: Path,
    *,
    extra_reviews: bool = False,
) -> list[str]:
    """Arma la línea de comando según GOSOM_MODE. Los flags son los documentados
    en el README del proyecto gosom/google-maps-scraper."""
    flags = [
        "--input", str(queries_file),
        "--results", str(output_file),
        "--json",
        "-c", str(s.GOSOM_CONCURRENCY),
        "--depth", str(s.GOSOM_DEPTH),
        "--lang", "es",
        "--exit-on-inactivity", "3m",
    ]
    if extra_reviews:
        flags.append("--extra-reviews")
    if s.GOSOM_MODE == "bin":
        return [s.GOSOM_BIN_PATH, *flags]
    if s.GOSOM_MODE == "docker":
        vol = s.GOSOM_OUTPUT_DIR
        return [
            "docker", "run", "--rm",
            "-v", f"{vol}:{vol}",
            s.GOSOM_DOCKER_IMAGE,
            *flags,
        ]
    raise ValueError(f"GOSOM_MODE desconocido: {s.GOSOM_MODE!r}")


# ─── Parseo del dump JSONL ───────────────────────────────────────────────────

def parsear_dump(output_file: Path,
                  sectores_admitidos: Optional[Iterable[str]] = None
                  ) -> Iterator[dict]:
    """
    Itera el JSONL de gosom y emite dicts normalizados. Cada dict tiene el mismo
    shape que el resto del pipeline (ver `routers.places_router._buscar_google`):

      id, nombre, lat, lng, rating, num_resenas, precio_nivel,
      sector_codigo, horario, fuente="google_scrape"

    Filtra:
      - Entradas sin coordenadas.
      - Entradas con `permanently_closed = true`.
      - Entradas cuya categoría no mapea a ningún sector admitido.
    """
    sectores_set = set(sectores_admitidos) if sectores_admitidos else None
    if not output_file.exists():
        logger.warning("gosom: dump %s no existe", output_file)
        return

    with output_file.open("r", encoding="utf-8") as fh:
        for n_linea, linea in enumerate(fh, start=1):
            linea = linea.strip()
            if not linea:
                continue
            try:
                row = json.loads(linea)
            except json.JSONDecodeError as e:
                logger.warning("gosom: línea %d JSON inválido: %s", n_linea, e)
                continue

            normalizado = _normalizar_row(row)
            if normalizado is None:
                continue
            if sectores_set and normalizado["sector_codigo"] not in sectores_set:
                continue
            yield normalizado


def parsear_reviews_dump(output_file: Path) -> Iterator[dict]:
    """
    Itera un JSONL de gosom con `user_reviews` / `user_reviews_extended`.

    Devuelve una fila por negocio scrapeado:
      source_id, place_id, nombre, lat, lng, rating, num_resenas, reviews[].
    `reviews[]` queda normalizado para poder insertarlo en `resenas`.
    """
    if not output_file.exists():
        logger.warning("gosom: dump de reseÃ±as %s no existe", output_file)
        return

    with output_file.open("r", encoding="utf-8") as fh:
        for n_linea, linea in enumerate(fh, start=1):
            linea = linea.strip()
            if not linea:
                continue
            try:
                row = json.loads(linea)
            except json.JSONDecodeError as e:
                logger.warning("gosom: lÃ­nea %d JSON invÃ¡lido: %s", n_linea, e)
                continue

            normalizado = normalizar_reviews_row(row)
            if normalizado is not None:
                yield normalizado


def normalizar_reviews_row(row: dict) -> Optional[dict]:
    """Normaliza la parte de reseÃ±as de una fila cruda de gosom."""
    place_id = row.get("place_id") or row.get("cid") or row.get("data_id") or row.get("id")
    nombre = (row.get("title") or row.get("name") or "").strip()
    reviews = _extract_reviews(row)
    if not place_id and not nombre and not reviews:
        return None

    return {
        "source_id": f"gs_{place_id}" if place_id else None,
        "place_id": place_id,
        "cid": row.get("cid"),
        "data_id": row.get("data_id"),
        "nombre": nombre,
        "lat": _safe_float(row.get("latitude") or row.get("lat") or row.get("location", {}).get("lat")),
        "lng": _safe_float(row.get("longitude") or row.get("longtitude") or row.get("lng") or row.get("location", {}).get("lng")),
        "rating": _safe_float(row.get("rating") or row.get("review_rating")),
        "num_resenas": _safe_int(row.get("review_count") or row.get("reviews")),
        "reviews": reviews,
    }


def review_stable_id(negocio_id: str, review: dict) -> str:
    """ID estable de reseÃ±a dentro de los 50 chars de `resenas.id`."""
    base = "|".join([
        negocio_id,
        str(review.get("texto") or ""),
        str(review.get("rating") or ""),
        str(review.get("fecha") or ""),
        str(review.get("autor") or ""),
    ])
    return "gsr_" + hashlib.sha1(base.encode("utf-8")).hexdigest()[:32]


def _extract_reviews(row: dict) -> list[dict]:
    raw_items: list = []
    for key in ("user_reviews_extended", "user_reviews", "reviews"):
        value = row.get(key)
        if isinstance(value, str) and value.strip():
            try:
                value = json.loads(value)
            except json.JSONDecodeError:
                value = []
        if isinstance(value, dict):
            value = value.get("reviews") or value.get("items") or []
        if isinstance(value, list):
            raw_items.extend(value)

    seen: set[tuple[str, str, str]] = set()
    reviews: list[dict] = []
    for raw in raw_items:
        review = _normalizar_review(raw)
        if review is None:
            continue
        key = (
            str(review.get("texto") or ""),
            str(review.get("rating") or ""),
            str(review.get("fecha") or ""),
        )
        if key in seen:
            continue
        seen.add(key)
        reviews.append(review)
    return reviews


def _normalizar_review(raw: object) -> Optional[dict]:
    if isinstance(raw, str):
        texto = raw.strip()
        return {"texto": texto, "fuente": "google_scrape"} if texto else None
    if not isinstance(raw, dict):
        return None

    texto = (
        raw.get("text")
        or raw.get("texto")
        or raw.get("review_text")
        or raw.get("snippet")
        or raw.get("comment")
        or ""
    )
    texto = str(texto).strip()
    if not texto:
        return None

    user = raw.get("user") if isinstance(raw.get("user"), dict) else {}
    autor = raw.get("author") or raw.get("author_name") or raw.get("user_name") or user.get("name")
    fecha = raw.get("date") or raw.get("fecha") or raw.get("published_at") or raw.get("time") or raw.get("timestamp")
    url = raw.get("url") or raw.get("link") or raw.get("review_url")

    return {
        "texto": texto,
        "rating": _safe_float(raw.get("rating") or raw.get("stars")),
        "fecha": str(fecha)[:10] if fecha else None,
        "idioma": raw.get("language") or raw.get("idioma") or raw.get("language_code"),
        "autor": str(autor).strip()[:200] if autor else None,
        "url": str(url).strip() if url else None,
        "fuente": "google_scrape",
        "metadata": {
            k: v
            for k, v in raw.items()
            if k not in {"text", "texto", "review_text", "snippet", "comment"}
        },
    }


def _normalizar_row(row: dict) -> Optional[dict]:
    """
    Adapta un registro de gosom al shape común. Devuelve None si la entrada no
    es utilizable (sin coordenadas, sin ID estable, sector no mapeable).

    ``permanently_closed``/``closed`` NO descarta la entrada — a partir de la
    migración 008 conservamos la fila con el flag ``_cerrado=True`` para que
    el ingestor la marque como inactiva y registre ``fecha_cierre_detectada``.
    Esto alimenta el label del XGBoost sin depender del Registre Mercantil.
    """
    lat = row.get("latitude") or row.get("lat") or row.get("location", {}).get("lat")
    lng = row.get("longitude") or row.get("lng") or row.get("location", {}).get("lng")
    if lat is None or lng is None:
        return None

    cat_principal = row.get("category")
    cats_lista = row.get("categories") if isinstance(row.get("categories"), list) else None
    sector, subsector = clasificar_con_fallback(cats_lista, cat_principal)
    if sector is None:
        return None

    place_id = row.get("place_id") or row.get("cid") or row.get("id")
    if not place_id:
        # Sin ID estable no podemos hacer upsert — preferimos descartar a
        # duplicar por reinicios del scraper.
        return None

    cerrado = bool(row.get("permanently_closed") or row.get("closed"))

    return {
        "id": f"gs_{place_id}",
        "nombre": (row.get("title") or row.get("name") or "").strip(),
        "lat": float(lat),
        "lng": float(lng),
        "rating": _safe_float(row.get("rating")),
        "num_resenas": _safe_int(row.get("review_count") or row.get("reviews")),
        "precio_nivel": _gosom_price_level(row.get("price") or row.get("price_range")),
        "sector_codigo": sector,
        "subsector_codigo": subsector,
        "horario": row.get("hours") or row.get("open_hours"),
        "fuente": "google_scrape",
        "_cerrado": cerrado,
    }


def _safe_float(v) -> Optional[float]:
    try:
        return float(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _safe_int(v) -> Optional[int]:
    try:
        return int(v) if v is not None else None
    except (TypeError, ValueError):
        return None


def _gosom_price_level(v) -> Optional[int]:
    """Gosom devuelve '€' / '€€' / '$$' / '$$$$' o similar. Lo colapsamos a 1-4."""
    if not v:
        return None
    if isinstance(v, int):
        return v if 1 <= v <= 4 else None
    if isinstance(v, str):
        # Cuenta cualquier símbolo de moneda común.
        n = sum(1 for ch in v if ch in "€$£¥")
        return n if 1 <= n <= 4 else None
    return None
