"""
gosom_client.py — Cliente Python para gosom/google-maps-scraper (Go), modo -web.

Modo de uso:
  El scraper Go se levanta como servicio Docker en localhost:8080 con la flag -web.
  Este cliente llama su REST API para buscar negocios por zona y
  extraer popular_times.

Nota sobre el modo -web:
  El modo -web usa el paquete web/web.go, que es DISTINTO del paquete api/api.go.
  No requiere ninguna API key — no hay middleware de auth en el servidor web.
  El flag -api-key y el paquete api/api.go son solo para el modo -database (SaaS).

API del modo -web (web/web.go):
  POST /api/v1/jobs        → envía job, devuelve {id}
  GET  /api/v1/jobs/{id}   → consulta estado del job
  GET  /api/v1/jobs/{id}/download → descarga resultados en CSV
  DELETE /api/v1/jobs/{id} → elimina job
  GET  /                   → interfaz web (HTML)

  Auth: NINGUNA. El servidor web no tiene middleware de autenticación.

Diferencias clave respecto al modo -database:
  - POST endpoint: /api/v1/jobs  (no /api/v1/scrape)
  - Respuesta POST: {"id": "uuid..."}  (no {"job_id": ...})
  - Status completado: "ok"  (no "completed")
  - Status trabajando: "working"  (no "running")
  - Resultados: CSV via /api/v1/jobs/{id}/download  (no JSON inline)
  - Campo keywords: array []string  (no keyword: string)
  - max_time: entero en segundos  (no timeout)
  - Requiere campo "name" obligatorio en el request

Nota: La API es asíncrona (job queue). search() sondea hasta que el job
termina o se agota el timeout. Los resultados llegan en CSV y se parsean
internamente a GosomEntry.
"""
from __future__ import annotations

import asyncio
import csv
import io
import logging
import os
import subprocess
import tempfile
from dataclasses import dataclass, field
from datetime import datetime
from typing import Optional

import httpx

logger = logging.getLogger(__name__)

# ── Configuración ──────────────────────────────────────────────────────────────
_GOSOM_BASE_URL = os.environ.get("GOSOM_BASE_URL", "http://gosom-scraper:8080")
# GOSOM_API_KEY solo se usa si el usuario configura explícitamente el modo
# -database con auth habilitada. El modo -web (el que usamos) no requiere
# ninguna API key — si la variable está vacía o ausente, no se envía header.
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
    Nota: el JSON/CSV de gosom usa "longtitude" (typo original conservado).
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
    # Campos adicionales disponibles en el CSV/JSON gosom
    phone: str = ""
    website: str = ""
    status: str = ""
    cid: str = ""
    data_id: str = ""
    open_hours: Optional[dict] = field(default=None)
    price_range: str = ""
    timezone: str = ""
    categories: list = field(default_factory=list)   # lista de categorías del lugar
    description: str = ""
    thumbnail: str = ""
    reviews_link: str = ""
    complete_address: Optional[dict] = field(default=None)  # {street, city, postal_code, state, country}

    @classmethod
    def from_dict(cls, d: dict) -> "GosomEntry":
        """Construye GosomEntry desde el dict JSON/CSV devuelto por la API gosom."""
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
            website=d.get("web_site", d.get("website", "")) or "",
            status=d.get("status", "") or "",
            cid=d.get("cid", "") or "",
            data_id=d.get("data_id", "") or "",
            open_hours=_parse_open_hours(d.get("open_hours")),
            price_range=d.get("price_range", "") or "",
            timezone=d.get("timezone", "") or "",
            categories=_parse_categories(d.get("categories")),
            description=d.get("description", "") or "",
            thumbnail=d.get("thumbnail", "") or "",
            reviews_link=d.get("reviews_link", "") or "",
            complete_address=d.get("complete_address") if isinstance(d.get("complete_address"), dict) else None,
        )


# ═══════════════════════════════════════════════════════════════════════════════
# Cliente principal
# ═══════════════════════════════════════════════════════════════════════════════

class GosomClient:
    """
    Cliente async para la REST API del gosom/google-maps-scraper (Go), modo -web.

    El scraper expone una API de job queue asíncrona (modo -web, sin auth):
      1. POST /api/v1/jobs                → devuelve {"id": "uuid"}
      2. GET  /api/v1/jobs/{id}           → sondear hasta status == "ok"
      3. GET  /api/v1/jobs/{id}/download  → descargar CSV con resultados

    El modo -web NO requiere API key. Si por algún motivo se usa el modo
    -database con auth, se puede pasar api_key o establecer GOSOM_API_KEY.
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
        # Enviar X-API-Key solo si está configurada — el modo -web no la necesita
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
        max_time_secs: int = 300,
        job_name: str = "",
    ) -> list[GosomEntry]:
        """
        Envía un job POST /api/v1/jobs y sondea hasta que completa.

        Usa la API del modo -web (web/web.go), que es diferente al modo -database.

        Args:
            query:         Texto de búsqueda (ej: "restaurante en Gràcia Barcelona").
            lat, lng:      Coordenadas geográficas del centro de búsqueda.
            zoom:          Zoom del mapa (1-21, default 15 ≈ barrio).
            depth:         Profundidad de paginación (1 = primera página, ~20 resultados).
            lang:          Idioma de resultados ("es", "en", etc.).
            fast_mode:     Modo rápido (stealth HTTP sin browser).
                           Requiere lat/lng no nulos.
            max_time_secs: Tiempo máximo del job en segundos (mínimo 180).
            job_name:      Nombre descriptivo del job (requerido por la API web).

        Returns:
            Lista de GosomEntry. Vacía si no hay resultados o hay error.

        Raises:
            GosomServiceUnavailable: si el servicio no está corriendo.
            GosomJobFailed:          si el job termina con error.
            GosomJobTimeout:         si el job supera job_timeout segundos.
        """
        # El modo -web requiere max_time >= 180s
        actual_max_time = max(180, min(max_time_secs, self.job_timeout))

        # El campo "name" es obligatorio en el modo -web (Validate() lo exige)
        if not job_name:
            ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
            job_name = f"georetail_{ts}"

        # API web: keywords es array, lat/lon son strings separados
        payload: dict = {
            "name":      job_name,
            "keywords":  [query],
            "lang":      lang,
            "zoom":      zoom,
            "lat":       str(lat),
            "lon":       str(lng),
            "fast_mode": fast_mode,
            "radius":    10000,          # 10 km
            "depth":     depth,
            "email":     False,
            "max_time":  actual_max_time,  # segundos (la API lo multiplica por time.Second)
        }

        # 1. Enviar job
        try:
            resp = await self._client.post("/api/v1/jobs", json=payload)
            resp.raise_for_status()
        except httpx.ConnectError as exc:
            raise GosomServiceUnavailable(f"No se puede conectar a {self.base_url}: {exc}") from exc
        except httpx.HTTPStatusError as exc:
            logger.error("gosom scrape error HTTP %s: %s", exc.response.status_code, exc.response.text)
            return []

        # La API web devuelve {"id": "uuid"} (no {"job_id": ...})
        job_id = resp.json().get("id", "")
        if not job_id:
            logger.error("gosom: respuesta sin id: %s", resp.text)
            return []

        logger.debug("gosom: job creado id=%s para query='%s'", job_id, query)

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

        El formato de query sigue el patrón de example-queries.txt de gosom:
          "restaurant in athens" → aquí usamos "restaurante en Gràcia Barcelona".

        Ejemplo:
            entries = await client.search_zona("Gràcia", "restaurante", 41.4036, 2.1538)
            # query enviada: "restaurante en Gràcia Barcelona"
        """
        query = f"{categoria} en {zona_nombre} Barcelona"
        return await self.search(
            query=query,
            lat=lat,
            lng=lng,
            zoom=zoom,
            depth=depth,
            lang="es",
            job_name=f"georetail_{zona_nombre}_{categoria}",
        )

    def is_available(self) -> bool:
        """
        GET / — comprueba si el servicio Go (modo -web) está corriendo.

        El modo -web no expone /api/v1/health. Usamos GET / que devuelve
        la interfaz HTML con status 200 cuando el servidor está activo.
        Si la respuesta tiene status 200, el servicio está listo.

        Usa httpx síncrono para poder llamarse desde contextos síncronos
        (p.ej. al inicio del pipeline antes de crear el event loop).

        Returns:
            True si el servicio responde con HTTP 200.
        """
        try:
            headers = {}
            if self.api_key:
                headers["X-API-Key"] = self.api_key
            # El modo -web sirve interfaz HTML en GET /
            resp = httpx.get(
                f"{self.base_url}/",
                headers=headers,
                timeout=5,
            )
            return resp.status_code == 200
        except Exception as exc:
            logger.debug("gosom no disponible: %s", exc)
            return False

    async def delete_job(self, job_id: str) -> bool:
        """DELETE /api/v1/jobs/{id} — elimina un job y sus resultados."""
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
        Sondea GET /api/v1/jobs/{id} hasta que status == "ok" o
        hasta agotar job_timeout. Descarga CSV y limpia el job tras obtener resultados.

        Status del modo -web (web/job.go):
          "pending"  → en cola
          "working"  → procesando
          "ok"       → completado con éxito  (≠ "completed" del modo -database)
          "failed"   → error
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
            # El modo -web devuelve el objeto Job con campo "Status" (capitalizado en Go,
            # pero json.Encode lo serializa como "Status" o "status" según struct tags)
            # JobData struct no tiene json tags, así que va capitalizado: "Status"
            status = data.get("Status", data.get("status", ""))
            logger.debug(
                "gosom poll job_id=%s status=%s elapsed=%.1fs",
                job_id, status, elapsed,
            )

            if status == "ok":
                # Descargar CSV de resultados
                entries = await self._download_csv_results(job_id)
                logger.info(
                    "gosom: job_id=%s completado — %d resultados en %.1fs",
                    job_id, len(entries), elapsed,
                )
                # Limpiar job para no acumular en cola
                await self.delete_job(job_id)
                return entries

            if status == "failed":
                logger.error("gosom: job_id=%s terminó con status=failed", job_id)
                raise GosomJobFailed(f"Job {job_id} falló")

            # status: "pending" / "working" → seguir esperando

        raise GosomJobTimeout(
            f"Job {job_id} no completó en {self.job_timeout}s"
        )

    async def _download_csv_results(self, job_id: str) -> list[GosomEntry]:
        """
        GET /api/v1/jobs/{id}/download — descarga el CSV y lo parsea a GosomEntry.

        El modo -web almacena resultados como CSV (no JSON inline).
        El CSV tiene cabecera con los nombres de campo del struct Entry de gosom.
        """
        try:
            resp = await self._client.get(f"/api/v1/jobs/{job_id}/download")
        except httpx.ConnectError as exc:
            logger.error("gosom: error descargando CSV job_id=%s: %s", job_id, exc)
            return []

        if resp.status_code == 404:
            logger.warning("gosom: CSV no encontrado para job_id=%s (sin resultados)", job_id)
            return []

        if resp.status_code != 200:
            logger.error("gosom: HTTP %s descargando CSV job_id=%s", resp.status_code, job_id)
            return []

        return _parse_csv_results(resp.content)


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
    y escribe resultados en CSV con -output. Añadir -json para JSON.
    """
    input_file = ""
    output_file = ""
    try:
        with tempfile.NamedTemporaryFile(
            mode="w", suffix=".txt", delete=False, encoding="utf-8"
        ) as f:
            f.write(query + "\n")
            input_file = f.name

        with tempfile.NamedTemporaryFile(
            mode="r", suffix=".csv", delete=False,
        ) as out_f:
            output_file = out_f.name

        cmd = [
            binary_path,
            "-input",  input_file,
            "-output", output_file,
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

        with open(output_file, "rb") as fh:
            return _parse_csv_results(fh.read())

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
        for f in (input_file, output_file):
            if f:
                try:
                    _os.unlink(f)
                except Exception:
                    pass


# ═══════════════════════════════════════════════════════════════════════════════
# Funciones de análisis popular_times
# ═══════════════════════════════════════════════════════════════════════════════

def get_today_hours(open_hours: dict) -> Optional[str]:
    """
    Devuelve el horario de hoy como string, p.ej. '9:00 AM - 9:00 PM'.

    Args:
        open_hours: dict con estructura {"Monday": "9:00 AM - 9:00 PM", ...}
                    o formato gosom {"Monday": ["9:00 AM", "9:00 PM"]}.

    Returns:
        String con el horario de hoy, o None si no está disponible.

    Ejemplo:
        hours = get_today_hours(entry.open_hours)
        # → "9:00 AM – 9:00 PM"  (si hoy es lunes y el local abre a las 9)
    """
    if not open_hours or not isinstance(open_hours, dict):
        return None

    # Mapear día actual al nombre en inglés (gosom usa nombres en inglés)
    day_names = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]
    today_name = day_names[datetime.now().weekday()]

    hours = open_hours.get(today_name)
    if hours is None:
        return None

    if isinstance(hours, str):
        return hours
    if isinstance(hours, list) and len(hours) == 2:
        return f"{hours[0]} – {hours[1]}"
    if isinstance(hours, list) and len(hours) == 1:
        return hours[0]

    return None


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
    Normaliza popular_times del JSON/CSV gosom.

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


def _parse_open_hours(raw: object) -> Optional[dict]:
    """
    Normaliza open_hours del dict gosom.
    Acepta tanto dict de strings como dict de listas.
    """
    if not raw or not isinstance(raw, dict):
        return None
    return raw


def _parse_categories(raw: object) -> list:
    """
    Normaliza el campo categories del dict gosom.
    Puede ser una lista de strings, una string separada por comas, o None.
    """
    if not raw:
        return []
    if isinstance(raw, list):
        return [str(c) for c in raw if c]
    if isinstance(raw, str) and raw.strip():
        return [c.strip() for c in raw.split(",") if c.strip()]
    return []


def _parse_csv_results(content: bytes) -> list[GosomEntry]:
    """
    Parsea el CSV descargado de /api/v1/jobs/{id}/download.

    El CSV de gosom tiene cabecera con los nombres de campo del struct Entry.
    Columnas típicas: title, category, address, open_hours, popular_times,
    website, phone, review_count, review_rating, latitude, longtitude,
    cid, status, description, reviews_link, thumbnail, timezone, price_range,
    data_id, place_id, images, ...
    """
    if not content:
        return []

    try:
        text = content.decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        entries: list[GosomEntry] = []

        for row in reader:
            if not row:
                continue
            try:
                # Campos JSON embebidos en el CSV (popular_times, open_hours son JSON strings)
                import json as _json

                d: dict = dict(row)

                # Deserializar campos JSON embebidos
                for json_field in ("popular_times", "open_hours", "complete_address",
                                   "about", "reviews_per_rating"):
                    val = d.get(json_field, "")
                    if val and isinstance(val, str) and val.strip().startswith("{"):
                        try:
                            d[json_field] = _json.loads(val)
                        except _json.JSONDecodeError:
                            d[json_field] = None

                for json_field_list in ("categories", "images", "user_reviews"):
                    val = d.get(json_field_list, "")
                    if val and isinstance(val, str) and val.strip().startswith("["):
                        try:
                            d[json_field_list] = _json.loads(val)
                        except _json.JSONDecodeError:
                            d[json_field_list] = []

                entry = GosomEntry.from_dict(d)
                if entry.title:
                    entries.append(entry)
            except Exception as exc:
                logger.debug("gosom: error parseando fila CSV: %s — %s", exc, row)

        return entries

    except Exception as exc:
        logger.error("gosom: error parseando CSV: %s", exc)
        return []


def _parse_results(results_raw: object) -> list[GosomEntry]:
    """
    Parsea el array JSON de resultados (usado por el modo -database o CLI con -json).
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
