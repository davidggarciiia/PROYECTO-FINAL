from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from pathlib import Path
from types import SimpleNamespace

from pipelines.entorno import resenas_scrape
from pipelines.inmobiliario.scraping.gosom_runner import GosomResult


def _run(coro):
    return asyncio.run(coro)


class FakeConn:
    def __init__(self, pending_rows):
        self.pending_rows = list(pending_rows)
        self.fetch_calls = 0
        self.execute_calls: list[tuple[str, tuple]] = []

    async def fetchval(self, *_args):
        return 77

    async def fetch(self, *_args):
        self.fetch_calls += 1
        if self.fetch_calls == 1:
            return self.pending_rows
        return []

    async def execute(self, sql, *args):
        self.execute_calls.append((sql, args))
        return "INSERT 0 1"


def test_resenas_scrape_inserta_actualiza_estado_y_nlp(monkeypatch, tmp_path: Path):
    negocio = {
        "id": "gs_ChIJ123",
        "nombre": "Tattoo Lab",
        "zona_id": "zona-1",
        "lat": 41.4,
        "lng": 2.16,
        "google_place_id": "ChIJ123",
        "rating": 4.5,
        "total_resenas": 10,
        "review_count": 10,
    }
    fake_conn = FakeConn([negocio])

    @asynccontextmanager
    async def fake_get_db():
        yield fake_conn

    output = tmp_path / "reviews.jsonl"
    output.write_text(
        json.dumps({
            "place_id": "ChIJ123",
            "title": "Tattoo Lab",
            "latitude": 41.4001,
            "longitude": 2.1601,
            "rating": 4.8,
            "review_count": 189,
            "user_reviews_extended": [
                {"text": "Muy buen trato", "rating": 5, "date": "2026-03-01"},
                {"text": "Tuve que esperar", "rating": 2, "date": "2026-02-01"},
            ],
        }),
        encoding="utf-8",
    )

    settings = SimpleNamespace(
        RESENAS_SCRAPE_MAX_NEGOCIOS=0,
        RESENAS_SCRAPE_BATCH_SIZE=50,
        RESENAS_SCRAPE_PAUSA_S=0,
        GOSOM_OUTPUT_DIR=str(tmp_path),
    )

    async def fake_ejecutar(_queries, out, extra_reviews=False):
        assert extra_reviews is True
        assert out == output
        return GosomResult(exit_code=0, stdout_tail="", stderr_tail="", output_path=output)

    async def fake_nlp():
        return 2

    monkeypatch.setattr(resenas_scrape, "get_settings", lambda: settings)
    monkeypatch.setattr(resenas_scrape, "get_db", fake_get_db)
    monkeypatch.setattr(resenas_scrape.gosom_runner, "ejecutar", fake_ejecutar)
    monkeypatch.setattr(resenas_scrape, "_procesar_nlp_pendiente", fake_nlp)

    stats = _run(resenas_scrape.ejecutar(max_negocios=1, batch_size=1, pausa_s=0, output_file=output))

    assert stats["negocios_intentados"] == 1
    assert stats["negocios_con_resenas"] == 1
    assert stats["resenas_insertadas"] == 2
    assert stats["procesadas_nlp"] == 2
    insert_sql = [sql for sql, _ in fake_conn.execute_calls if "INSERT INTO resenas" in sql]
    assert len(insert_sql) == 2
    estado_args = [args for sql, args in fake_conn.execute_calls if "resenas_scrape_estado" in sql]
    assert any("ok" in args for args in estado_args)
