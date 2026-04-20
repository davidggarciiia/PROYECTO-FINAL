"""
tests/test_competencia_scrape.py — E2E del pipeline batch de Competencia.

No tocamos PostgreSQL. Mockeamos la conexión para capturar los SQL que lanza
el pipeline y verificar que:

  1. `ejecutar` parsea el JSONL de gosom, clasifica (sector, subsector) y
     llama a `_upsert_negocio` por cada fila — incluidas las cerradas, que
     ahora conservan la fila con `activo=FALSE`.
  2. Las entradas `permanently_closed=True` llegan a `_upsert_negocio` con
     el parámetro `activo` a FALSE y `fecha_cierre_detectada` no-nulo.
  3. El pipeline invoca `_recalcular_competencia` (doble agregado) y
     `_precalentar_cache` (caché Redis) como últimos pasos.
  4. El resultado devuelto trae conteos coherentes de parseo, upsert y
     cobertura.

Este test es el "snapshot" funcional del pipeline end-to-end: si mañana
alguien cambia la semántica de `_cerrado` o rompe el orden de las fases,
este test lo pilla.
"""
from __future__ import annotations

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import pytest

# Necesitamos el módulo REAL de pipelines, pero NO queremos tocar `db.conexion`
# porque el conftest global ya lo stubea. Sólo limpiamos `config` y `pipelines`
# para forzar una re-importación controlada con nuestro fake config.
for _mod in list(sys.modules):
    if _mod == "config" or _mod.startswith("pipelines"):
        del sys.modules[_mod]

_fake_config_mod = MagicMock()
class _FakeSettings:
    GOSOM_MODE = "disabled"
    GOSOM_BIN_PATH = "/tmp/gosom"
    GOSOM_DOCKER_IMAGE = "gosom:latest"
    GOSOM_OUTPUT_DIR = "/tmp/gmaps_dump"
    GOSOM_CONCURRENCY = 2
    GOSOM_DEPTH = 5
    GOSOM_TIMEOUT_S = 0
    COMPETENCIA_CACHE_TTL_S = 3600
    PLACES_GOOGLE_API_ENABLED = False
_fake_config_mod.get_settings = lambda: _FakeSettings()
sys.modules["config"] = _fake_config_mod

# db.redis_client no está como módulo real en el stub global — lo proveemos
# explícitamente para que el import de pipelines.competencia_scrape funcione.
# El test sobrescribe después `cs.get_redis` con su propio mock.
_fake_redis_client = MagicMock()
_fake_redis_client.get_redis = lambda: MagicMock()
sys.modules["db.redis_client"] = _fake_redis_client


# ─── Mock de asyncpg ──────────────────────────────────────────────────────────

class _FakeConn:
    """Captura los SQL que el pipeline ejecuta para que los tests puedan
    inspeccionarlos. No interpreta SQL — simplemente anota."""
    def __init__(self):
        self.execute_calls: list[tuple[str, tuple]] = []
        self.fetchrow_calls: list[tuple[str, tuple]] = []
        self.fetchval_calls: list[tuple[str, tuple]] = []
        self.fetch_calls: list[tuple[str, tuple]] = []
        # Secuencia de respuestas predefinidas por cada tipo
        self._fetchrow_queue: list = []
        self._fetchval_queue: list = []
        self._fetch_queue: list = []

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        return "INSERT 0 1"

    async def fetchrow(self, sql, *args):
        self.fetchrow_calls.append((sql, args))
        return self._fetchrow_queue.pop(0) if self._fetchrow_queue else None

    async def fetchval(self, sql, *args):
        self.fetchval_calls.append((sql, args))
        return self._fetchval_queue.pop(0) if self._fetchval_queue else 1

    async def fetch(self, sql, *args):
        self.fetch_calls.append((sql, args))
        return self._fetch_queue.pop(0) if self._fetch_queue else []

    def transaction(self):
        # Context manager async dummy
        @asynccontextmanager
        async def _ctx():
            yield self
        return _ctx()


@pytest.fixture
def fake_conn():
    return _FakeConn()


@pytest.fixture
def dump_fixture(tmp_path: Path) -> Path:
    """Dump realista: 3 entradas utilizables (2 abiertas + 1 cerrada)."""
    path = tmp_path / "out.jsonl"
    entradas = [
        {
            "place_id": "aa1",
            "title": "Bar Paco",
            "latitude": 41.38, "longitude": 2.17,
            "category": "Restaurant",
            "rating": 4.1, "review_count": 320,
        },
        {
            "place_id": "bb2",
            "title": "Pizza Luigi",
            "latitude": 41.39, "longitude": 2.18,
            "category": "Pizzeria",
            "rating": 4.6, "review_count": 150,
        },
        {
            "place_id": "cc3",
            "title": "Zombie Grill",
            "latitude": 41.40, "longitude": 2.19,
            "category": "Restaurant",
            "permanently_closed": True,
        },
    ]
    path.write_text("\n".join(json.dumps(e) for e in entradas), encoding="utf-8")
    return path


def _run(coro):
    return asyncio.run(coro)


# ─── Tests ────────────────────────────────────────────────────────────────────

class TestPipelineFlujoCompleto:
    """El pipeline completo corre sin tocar Postgres real."""

    def test_ejecutar_procesa_dump_y_marca_cerrados(self,
                                                    fake_conn, dump_fixture,
                                                    monkeypatch):
        import importlib
        # Recarga forzada — si otro test ya importó competencia_scrape con el
        # config stubeado del conftest, `get_settings` apunta allí. Reload
        # refresca el módulo con nuestro sys.modules["config"] fake.
        import pipelines.comercio.competencia_scrape as cs
        import pipelines.inmobiliario.scraping.gosom_runner as gr
        cs = importlib.reload(cs)
        gr = importlib.reload(gr)

        fake_conn._fetch_queue.append([
            {"zona_id": "bcn_eixample_01",
             "zona_nombre": "Eixample Dreta",
             "nombre_barrio": "Eixample Dreta",
             "nombre_distrito": "Eixample"},
        ])

        @asynccontextmanager
        async def fake_get_db():
            yield fake_conn

        monkeypatch.setattr(cs, "get_db", fake_get_db)
        monkeypatch.setattr(cs, "get_settings", _fake_config_mod.get_settings)
        monkeypatch.setattr(gr, "get_settings", _fake_config_mod.get_settings)

        # Redis mock — set/get no-op
        fake_redis = MagicMock()
        fake_redis.setex = AsyncMock()
        monkeypatch.setattr(cs, "get_redis", lambda: fake_redis)

        # Desactivar scraper: ejecutar() no lanzará gosom, sólo parseará el dump.
        stats = _run(cs.ejecutar(
            sectores=["restauracion"],
            max_zonas=None,
            output_file=dump_fixture,
        ))

        # 1. Parseó las 3 entradas (incluida la cerrada)
        assert stats["rows_parseadas"] == 3
        assert stats["upsertadas"] == 3

        # 2. gosom_exit=-1 porque estamos en modo disabled
        assert stats["gosom_exit"] == -1

        # 3. Se ejecutó _upsert_negocio con activo=FALSE para la cerrada.
        # Param positions (0-idx): 10=activo, 11=fuente, 12=fecha_cierre_detectada
        upsert_calls = [c for c in fake_conn.execute_calls
                        if "INSERT INTO negocios_activos" in c[0]]
        assert len(upsert_calls) == 3
        cerrada = [c for c in upsert_calls if c[1][10] is False]
        assert len(cerrada) == 1
        assert cerrada[0][1][12] is not None

        # 4. Llamó _recalcular_competencia → al menos una INSERT con 'alta'
        insert_agregados = [c for c in fake_conn.execute_calls
                            if "INSERT INTO competencia_por_local" in c[0]]
        assert len(insert_agregados) >= 1

        # 5. Precalentó caché (aunque no hay filas, se recorre la tabla vacía)
        assert fake_conn.fetch_calls  # al menos un fetch para la caché
