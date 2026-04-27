from __future__ import annotations

import asyncio
import importlib
import sys
from contextlib import asynccontextmanager


def _run(coro):
    return asyncio.run(coro)


for _mod in ("db", "db.zonas"):
    sys.modules.pop(_mod, None)
zonas = importlib.import_module("db.zonas")


class FakeConn:
    def __init__(self):
        self.fetch_queue = [
            [
                {
                    "id": "gs_1",
                    "nombre": "Tattoo Lab",
                    "sector_codigo": "tatuajes",
                    "subsector_codigo": "tatuaje",
                    "lat": 41.4,
                    "lng": 2.16,
                    "rating": 4.8,
                    "num_resenas": 189,
                    "precio_nivel": 3,
                    "distancia_m": 120,
                },
                {
                    "id": "gs_2",
                    "nombre": "Body Art",
                    "sector_codigo": "tatuajes",
                    "subsector_codigo": "tatuaje",
                    "lat": 41.401,
                    "lng": 2.161,
                    "rating": 3.1,
                    "num_resenas": 18,
                    "precio_nivel": 2,
                    "distancia_m": 260,
                },
            ],
            [
                {
                    "negocio_id": "gs_1",
                    "total": 2,
                    "positivas": 1,
                    "negativas": 1,
                    "neutras": 0,
                    "rating_medio": 3.5,
                    "ultima_fecha": "2026-03-01",
                },
            ],
            [
                {
                    "negocio_id": "gs_1",
                    "texto": "Buen resultado, aunque la espera fue larga",
                    "rating": 3.0,
                    "fecha": "2026-03-01",
                    "sentimiento": "neutro",
                    "categoria": "servicio",
                    "fuente": "google_scrape",
                },
            ],
            [
                {"negocio_id": "gs_1", "categoria": "servicio", "n": 2},
            ],
        ]

    async def fetchval(self, *_args):
        return 1

    async def fetchrow(self, *_args):
        return None

    async def fetch(self, *_args):
        return self.fetch_queue.pop(0)


def test_competencia_zona_anexa_resumen_y_extractos(monkeypatch):
    fake_conn = FakeConn()

    @asynccontextmanager
    async def fake_get_db():
        yield fake_conn

    monkeypatch.setattr(zonas, "get_db", fake_get_db)

    result = _run(zonas.get_competencia_zona("zona-1", "tatuajes"))

    assert result is not None
    assert result["amenaza"][0]["id"] == "gs_1"
    assert result["amenaza"][0]["resenas_resumen"]["total"] == 2
    assert result["amenaza"][0]["resenas_resumen"]["temas_top"] == ["servicio"]
    assert result["amenaza"][0]["resenas_destacadas"][0]["fuente"] == "google_scrape"
    assert result["resenas_cobertura"]["negocios_total"] == 2
    assert result["resenas_cobertura"]["negocios_con_resenas"] == 1


class ManyCompetitorsConn:
    def __init__(self):
        self.rows = []
        for i in range(80):
            self.rows.append({
                "id": f"strong_{i}",
                "nombre": f"Strong Tattoo {i}",
                "sector_codigo": "tatuajes",
                "subsector_codigo": "tatuaje",
                "lat": 41.4 + i * 0.00001,
                "lng": 2.16,
                "rating": 4.6,
                "num_resenas": 220,
                "precio_nivel": 3,
                "distancia_m": 40 + i,
            })
        for i in range(40):
            self.rows.append({
                "id": f"weak_{i}",
                "nombre": f"Weak Tattoo {i}",
                "sector_codigo": "tatuajes",
                "subsector_codigo": "tatuaje",
                "lat": 41.402 + i * 0.00001,
                "lng": 2.161,
                "rating": 2.8,
                "num_resenas": 8,
                "precio_nivel": 2,
                "distancia_m": 180 + i,
            })

    async def fetchval(self, *_args):
        return 1

    async def fetchrow(self, *_args):
        return None

    async def fetch(self, *_args):
        return self.rows


def test_competencia_zona_limita_payload_visual(monkeypatch):
    fake_conn = ManyCompetitorsConn()
    captured = {"ids": []}

    @asynccontextmanager
    async def fake_get_db():
        yield fake_conn

    async def fake_get_resenas(negocio_ids):
        captured["ids"] = negocio_ids
        return {nid: zonas._empty_resenas() for nid in negocio_ids}

    monkeypatch.setattr(zonas, "get_db", fake_get_db)
    monkeypatch.setattr(zonas, "_get_resenas_por_negocio", fake_get_resenas)

    result = _run(zonas.get_competencia_zona("zona-1", "tatuajes"))

    assert result is not None
    assert result["num_directos"] > 100
    assert len(result["amenaza"]) == zonas._MAX_AMENAZAS_VISIBLES
    assert len(result["oportunidad"]) == zonas._MAX_OPORTUNIDADES_VISIBLES
    assert result["resenas_cobertura"]["negocios_total"] == (
        zonas._MAX_AMENAZAS_VISIBLES + zonas._MAX_OPORTUNIDADES_VISIBLES
    )
    assert len(captured["ids"]) == result["resenas_cobertura"]["negocios_total"]
