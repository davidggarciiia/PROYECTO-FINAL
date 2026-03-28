"""
gosom_client.py — Cliente Python para gosom/google-maps-scraper (Go).

Modo de uso:
  El scraper Go se levanta como servicio Docker en localhost:8080.
  Este cliente llama su REST API para buscar negocios por zona y
  extraer popular_times.

Alternativa sin Docker: subprocess + CLI binary (ver _run_cli_fallback).

API real del servicio (gosom/google-maps-scraper v1):
  POST /api/v1/scrape        → envía un job asíncrono, devuelve {job_id, status}
  GET  /api/v1/jobs/{job_id} → consulta estado + resultados cuando "completed"
  GET  /api/v1/jobs          → lista jobs (paginado, filtrable por state)
  GET  /api/v1/health        → {"status":"ok"}

  Auth: header "X-API-Key: <key>" o "Authorization: Bearer <key>".
  Si no se configura API key en el servicio Go, el middleware la exige
  igualmente — usar GOSOM_API_KEY en variables de entorno o pasar api_key="".

Nota: La API es asíncrona (job queue). search() sondea hasta que el job
termina o se agota el timeout.
"""
from __future__ import annotations

import asyncio
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_GOSOM_BASE_URL = os.environ.get("GOSOM_BASE_URL", "http://localhost:8080")
_GOSOM_API_KEY  = os.environ.get("GOSOM_API_KEY", "")
_GOSOM_POLL_INTERVAL_S = float(os.environ.get("GOSOM_POLL_INTERVAL_S", "3"))
_GOSOM_JOB_TIMEOUT_S   = int(os.environ.get("GOSOM_JOB_TIMEOUT_S", "120"))


# ═══════════════════════════════════════════════════════════════════════════════
# Dataclasses de datos
# ═══════════════════════════════════════════════════════════════════════════════

@dataclass
class GosomEntry:
    """Negocio extraído por el scraper gosom.

    Los campos coinciden con el struct Entry de gmaps/entry.go.
    popular_times: {day_name: {hour_0_23: traffic_0_100}}
    Nota: el JSON de gosom usa "longtitude" (typo original conservado).
    """
    title: str
    latitude: float
    longitude: float
    review_count: int
    review_rating: float
    popular_times: Optional[dict]   # {day: {hour: traffic_pct}}  ej: {"Monday": {9: 45, 10: 72, ...}}
    place_id: str
    category: str
    address: str
    # Campos adicionales disponibles en el JSON gosom
    phone: str = ""
    website: str = ""
    status: str = ""
    cid: str = ""
    data_id: str = ""
    open_hours: Optional[dict] = field(default=None)
    price_range: str = ""
    timezone: str = ""

    @classmethod
    def from_dict(cls, d: dict) -> "GosomEntry":
        """Construye GosomEntry desde el dict JSON devuelto por la API gosom."""
        return cls(
            title=d.get("title", ""),
            # El struct Go tiene "longtitude" (typo), el JSON lo preserva
            latitude=float(d.get("latitude", 0.0) or 0.0),
            longitude=float(d.get("longtitude", d.get("longitude", 0.0)) or 0.0),
            review_count=int(d.get("review_count", 0) or 0),
            review_rating=float(d.get("review_rating", 0.0) or 0.0),
            popular_times=_parse_popular_times(d.get("popular_times")),
            place_id=d.get("place_id", "") or "",
            category=d.get("category", "") or "",
            address=d.get("address", "") or "",
            phone=d.get("phone", "") or "",
            website=d.get("web_site", "") or "",
            status=d.get("status", "") or "",
            cid=d.get("cid", "") or "",
            data_id=d.get("data_id", "") or "",
            open_hours=d.get("open_hours"),
            price_range=d.get("price_range", "") or "",
            timezone=d.get("timezone", "") or "",
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Cliente principal
# ═══════════════════════════════════════════════════════════════════════════════

class GosomClient:
    """
    Cliente async para la REST API del gosom/google-maps-scraper (Go).

    El scraper expone una API de job queue asíncrona:
      1. POST /api/v1/scrape  → devuelve job_id
      2. GET  /api/v1/jobs/{job_id}  → sondear hasta status == "completed"
      3. El resultado está en jobs/{job_id}.results (array de Entry JSON)

    Autenticación: X-API-Key header. Si el servicio se levanta sin auth
    configurada aún requiere la cabecera — pasar api_key="" o el valor real.
    """

    def __init__(
        self,
        base_url: str = _GOSOM_BASE_URL,
        api_key: str = _GOSOM_API_KEY,
        timeout: int = 30,
        poll_interval: float = _GOSOM_POLL_INTERVAL_S,
        job_timeout: int = _GOSOM_JOB_TIMEOUT_S,
    ) -> None:
        self.base_url   = base_url.rstrip("/")
        self.api_key    = api_key
        self.timeout    = timeout
        self.poll_interval = poll_interval
        self.job_timeout   = job_timeout

        headers = {"Content-Type": "application/json"}
        if self.api_key:
            headers["X-API-Key"] = self.api_key

        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            headers=headers,
            timeout=self.timeout,
        )

    # ── API pública ────────────────────────────────────────────────────────────

    async def search(
        self,
        query: str,
        lat: float,
        lng: float,
        zoom: int = 15,
        depth: int = 1,
        lang: str = "es",
        fast_mode: bool = False,
    ) -> list[GosomEntry]:
        """
        Envía un job POST /api/v1/scrape y sondea hasta que completa.

        Args:
            query:      Texto de búsqueda (ej: "restaurante en Gràcia Barcelona").
            lat, lng:   Coordenadas geográficas del centro de búsqueda.
            zoom:       Zoom del mapa (1-21, default 15 ≈ barrio).
            depth:      max_depth para paginación (1 = primera página, ~20 resultados).
            lang:       Idioma de resultados ("es", "en", etc.).
            fast_mode:  Modo rápido (stealth HTTP sin browser).
                        Requiere geo_coordinates + zoom.

        Returns:
            Lista de GosomEntry. Vacía si no hay resultados o hay error.

        Raises:
            GosomServiceUnavailable: si el servicio no está corriendo.
            GosomJobFailed:          si el job termina con error.
            GosomJobTimeout:         si el job supera job_timeout segundos.
        """
        payload: dict = {
            "keyword":         query,
            "lang":            lang,
            "max_depth":       depth,
            "geo_coordinates": f"{lat},{lng}",
            "zoom":            zoom,
            "fast_mode":       fast_mode,
            "timeout":         min(self.job_timeout, 300),
        }

        # 1. Enviar job
        try:
            resp = await self._client.post("/api/v1/scrape", json=payload)
            resp.raise_for_status()
        except httpx.ConnectError as exc:
            raise GosomServiceUnavailable(f"No se puede conectar a {self.base_url}: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            logger.error("gosom scrape error HTTP %s: %s", exc.response.status_code, exc.response.text)
            return []

        job_id = resp.json().get("job_id", "")
        if not job_id:
            logger.error("gosom: respuesta sin job_id: %s", resp.text)
            return []

        logger.debug("gosom: job creado job_id=%s para query='%s'", job_id, query)

        # 2. Sondear hasta que el job termine
        return await self._poll_job(job_id)

    async def search_zona(
        self,
        zona_nombre: str,
        categoria: str,
        lat: float,
        lng: float,
        zoom: int = 15,
        depth: int = 1,
    ) -> list[GosomEntry]:
        """
        Wrapper convenience: busca '{categoria} en {zona_nombre} Barcelona'.

        Ejemplo:
            entries = await client.search_zona("Gràcia", "restaurante", 41.4036, 2.1538)
        """
        query = f"{categoria} en {zona_nombre} Barcelona"
        return await self.search(query=query, lat=lat, lng=lng, zoom=zoom, depth=depth)

    def is_available(self) -> bool:
        """
        GET /api/v1/health — comprueba si el servicio Go está corriendo.

        Usa httpx síncrono para poder llamarse desde contextos síncronos
        (p.ej. al inicio del pipeline antes de crear el event loop).

        Returns:
            True si el servicio responde con {"status": "ok"}.
        """
        try:
            headers = {}
            if self.api_key:
                headers["X-API-Key"] = self.api_key
            resp = httpx.get(
                f"{self.base_url}/api/v1/health",
                headers=headers,
                timeout=5,
            )
            if resp.status_code == 200:
                data = resp.json()
                return data.get("status") == "ok"
            return False
        except Exception as exc:
            logger.debug("gosom no disponible: %s", exc)
            return False

    async def delete_job(self, job_id: str) -> bool:
        """DELETE /api/v1/jobs/{job_id} — elimina un job y sus resultados."""
        try:
            resp = await self._client.delete(f"/api/v1/jobs/{job_id}")
            return resp.status_code in (200, 202, 204)
        except Exception as exc:
            logger.debug("gosom delete_job error: %s", exc)
            return False

    async def close(self) -> None:
        """Cierra el cliente HTTP subyacente."""
        await self._client.aclose()

    async def __aenter__(self) -> "GosomClient":
        return self

    async def __aexit__(self, *_: object) -> None:
        await self.close()

    # ── Internos ───────────────────────────────────────────────────────────────

    async def _poll_job(self, job_id: str) -> list[GosomEntry]:
        """
        Sondea GET /api/v1/jobs/{job_id} hasta que status == "completed" o
        hasta agotar job_timeout. Limpia el job tras obtener resultados.
        """
        elapsed = 0.0
        while elapsed < self.job_timeout:
            await asyncio.sleep(self.poll_interval)
            elapsed += self.poll_interval

            try:
                resp = await self._client.get(f"/api/v1/jobs/{job_id}")
            except httpx.ConnectError as exc:
                raise GosomServiceUnavailable(f"Conexión perdida durante polling: {exc}") from exc

            if resp.status_code == 404:
                logger.warning("gosom: job_id=%s no encontrado (¿ya expiró?)", job_id)
                return []

            if resp.status_code != 200:
                logger.error("gosom poll HTTP %s para job_id=%s", resp.status_code, job_id)
                return []

            data = resp.json()
            status = data.get("status", "")
            logger.debug(
                "gosom poll job_id=%s status=%s elapsed=%.1fs",
                job_id, status, elapsed,
            )

            if status == "completed":
                results_raw = data.get("results") or []
                entries = _parse_results(results_raw)
                logger.info(
                    "gosom: job_id=%s completado — %d resultados en %.1fs",
                    job_id, len(entries), elapsed,
                )
                # Limpiar job para no acumular en cola
                await self.delete_job(job_id)
                return entries

            if status in ("failed", "cancelled", "discarded"):
                error_msg = data.get("error", "sin detalle")
                logger.error("gosom: job_id=%s terminó con status=%s error=%s", job_id, status, error_msg)
                raise GosomJobFailed(f"Job {job_id} falló con status={status}: {error_msg}")

            # status: pending / running / retryable / scheduled → seguir esperando

        raise GosomJobTimeout(
            f"Job {job_id} no completó en {self.job_timeout}s"
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Fallback CLI (sin Docker)
# ═══════════════════════════════════════════════════════════════════════════════

def _run_cli_fallback(
    query: str,
    lat: float,
    lng: float,
    binary_path: str = "google-maps-scraper",
    zoom: int = 15,
    depth: int = 1,
    lang: str = "es",
) -> list[GosomEntry]:
    """
    Alternativa sin Docker: ejecuta el binario gosom directamente por subprocess.

    Requiere que el binario esté compilado y en PATH (o pasar binary_path).

    Uso:
        entries = _run_cli_fallback(
            "restaurante en Gràcia Barcelona",
            lat=41.4036, lng=2.1538,
            binary_path="/usr/local/bin/google-maps-scraper",
        )

    El binario acepta un archivo de queries (una por línea) con -input
    y escribe resultados JSON con -json -output /dev/stdout.
    """
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write(query + "\n")
            input_file = f.name

        with tempfile.NamedTemporaryFile(
            mode="r", suffix=".json", delete=False,
        ) as out_f:
            output_file = out_f.name

        cmd = [
            binary_path,
            "-input",  input_file,
            "-output", output_file,
            "-json",
            "-lang",   lang,
            "-depth",  str(depth),
            "-geo",    f"{lat},{lng}",
            "-zoom",   str(zoom),
        ]

        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120,
        )

        if result.returncode != 0:
            logger.error("gosom CLI error: %s", result.stderr)
            return []

        import json as _json
        with open(output_file, encoding="utf-8") as fh:
            raw = _json.load(fh)

        return _parse_results(raw if isinstance(raw, list) else [raw])

    except FileNotFoundError:
        logger.error("gosom binary no encontrado en: %s", binary_path)
        return []
    except subprocess.TimeoutExpired:
        logger.error("gosom CLI timeout para query='%s'", query)
        return []
    except Exception as exc:
        logger.error("gosom CLI fallback error: %s", exc)
        return []
    finally:
        import os as _os
        for f in (input_file, output_file):  # type: ignore[possibly-undefined]
            try:
                _os.unlink(f)
            except Exception:
                pass


# ═══════════════════════════════════════════════════════════════════════════════
# Funciones de análisis popular_times
# ═══════════════════════════════════════════════════════════════════════════════

def popular_times_to_peak_score(popular_times: dict) -> float:
    """
    Convierte popular_times {day: {hour: pct}} → peak_score 0-100.
    Peak = max de todos los valores de todos los días.

    Args:
        popular_times: dict con estructura {"Monday": {9: 45, 10: 72, ...}, ...}

    Returns:
        float en [0, 100]. 0.0 si popular_times está vacío o es None.

    Ejemplo:
        score = popular_times_to_peak_score({"Monday": {12: 90, 13: 95}, "Friday": {21: 80}})
        # → 95.0
    """
    if not popular_times:
        return 0.0

    max_val = 0
    for hours_map in popular_times.values():
        if not isinstance(hours_map, dict):
            continue
        for val in hours_map.values():
            try:
                v = int(val)
                if v > max_val:
                    max_val = v
            except (TypeError, ValueError):
                continue

    return float(max_val)


def popular_times_to_busy_fraction(popular_times: dict, threshold: int = 50) -> float:
    """
    Fracción de horas comerciales (9-21h) con busyness > threshold.
    Indica si la zona es activa durante el día laboral.

    Args:
        popular_times: dict {day: {hour: pct}}.
        threshold:     porcentaje mínimo para considerar una hora "ocupada" (default 50).

    Returns:
        float en [0.0, 1.0].
        1.0 = todas las horas comerciales de todos los días superan el umbral.
        0.0 = ninguna hora lo supera o popular_times está vacío.

    Ejemplo:
        frac = popular_times_to_busy_fraction(pt, threshold=50)
        # → 0.67 si 8 de 12 horas (9-21h) superan el 50% en promedio semanal
    """
    if not popular_times:
        return 0.0

    commercial_hours = range(9, 21)  # 9h a 20h inclusive → 12 franjas
    total_slots = 0
    busy_slots = 0

    for hours_map in popular_times.values():
        if not isinstance(hours_map, dict):
            continue
        for hour in commercial_hours:
            val = hours_map.get(hour)
            if val is None:
                # Si no hay dato para esa hora, no contar como slot disponible
                continue
            total_slots += 1
            try:
                if int(val) > threshold:
                    busy_slots += 1
            except (TypeError, ValueError):
                continue

    if total_slots == 0:
        return 0.0

    return round(busy_slots / total_slots, 4)


def popular_times_to_weekly_profile(popular_times: dict) -> dict:
    """
    Devuelve {day: avg_busyness} — perfil semanal de actividad.
    Útil para distinguir zonas de semana vs fin de semana.

    Args:
        popular_times: dict {day_name: {hour: pct}}.

    Returns:
        Dict {day_name: float} con la media de busyness de las 24h del día.
        Los días sin datos no aparecen en el resultado.

    Ejemplo:
        profile = popular_times_to_weekly_profile(pt)
        # → {"Monday": 32.5, "Friday": 58.1, "Saturday": 71.3, "Sunday": 65.0}
    """
    if not popular_times:
        return {}

    profile: dict[str, float] = {}
    day_order = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

    for day in day_order:
        hours_map = popular_times.get(day)
        if not isinstance(hours_map, dict) or not hours_map:
            continue
        values = []
        for val in hours_map.values():
            try:
                values.append(int(val))
            except (TypeError, ValueError):
                continue
        if values:
            profile[day] = round(sum(values) / len(values), 2)

    return profile


# ═══════════════════════════════════════════════════════════════════════════════
# Excepciones
# ═══════════════════════════════════════════════════════════════════════════════

class GosomError(Exception):
    """Base para errores del cliente gosom."""


class GosomServiceUnavailable(GosomError):
    """El servicio Go no está corriendo o no es accesible."""


class GosomJobFailed(GosomError):
    """El job de scraping terminó con error."""


class GosomJobTimeout(GosomError):
    """El job no completó en el tiempo máximo configurado."""


# ═══════════════════════════════════════════════════════════════════════════════
# Helpers privados
# ═══════════════════════════════════════════════════════════════════════════════

def _parse_popular_times(raw: object) -> Optional[dict]:
    """
    Normaliza popular_times del JSON gosom.

    El JSON de gosom serializa map[string]map[int]int como
    {"Monday": {"9": 45, "10": 72}} — las claves de hora son strings.
    Convertimos a {str_day: {int_hour: int_pct}}.
    """
    if not raw or not isinstance(raw, dict):
        return None

    result: dict = {}
    for day, hours in raw.items():
        if not isinstance(hours, dict):
            continue
        normalized_hours: dict[int, int] = {}
        for h, v in hours.items():
            try:
                normalized_hours[int(h)] = int(v)
            except (TypeError, ValueError):
                continue
        if normalized_hours:
            result[day] = normalized_hours

    return result if result else None


def _parse_results(results_raw: object) -> list[GosomEntry]:
    """
    Parsea el array de resultados devuelto por GET /api/v1/jobs/{job_id}.
    Acepta lista de dicts o None.
    """
    if not results_raw or not isinstance(results_raw, list):
        return []

    entries: list[GosomEntry] = []
    for item in results_raw:
        if not isinstance(item, dict):
            continue
        try:
            entry = GosomEntry.from_dict(item)
            if entry.title:  # Descartar entradas sin título
                entries.append(entry)
        except Exception as exc:
            logger.debug("gosom: error parseando entrada: %s — %s", exc, item)

    return entries
