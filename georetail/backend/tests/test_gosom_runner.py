"""
tests/test_gosom_runner.py — Tests del wrapper del scraper gosom.

Cubre:
  - `sector_desde_categoria`: el mapeo de la categoría textual que devuelve
    gosom al `sector_codigo` interno.
  - `_normalizar_row`: descarta entradas sin coordenadas, cerradas y sin
    categoría mapeable; normaliza al shape del resto del pipeline.
  - `_gosom_price_level`: '€' → 1, '€€€' → 3, None / string vacío → None.
  - `generar_queries` + `escribir_queries_file`: genera N×M queries y las
    vuelca al fichero en el formato que el binario espera.
  - `parsear_dump`: itera un fichero JSONL de ejemplo y emite sólo filas
    utilizables.
  - `ejecutar` en modo 'disabled': no intenta arrancar nada y devuelve
    exit_code=-1.
"""
from __future__ import annotations

import asyncio
import json
import sys
from dataclasses import dataclass
from pathlib import Path
from unittest.mock import MagicMock

import pytest

# conftest.py pone un stub global para `config` y `db`. Para este test
# necesitamos los módulos REALES (gosom_runner importa `from config import
# get_settings`). Limpiamos los stubs y los reconstruimos al final del archivo
# si hace falta.
for _mod in list(sys.modules):
    if _mod in ("config",) or _mod.startswith("pipelines"):
        del sys.modules[_mod]

# Dejamos un config mínimo en sys.modules que se pueda monkeypatchear.
_fake_config_mod = MagicMock()
class _FakeSettings:
    GOSOM_MODE = "disabled"
    GOSOM_BIN_PATH = "/tmp/gosom"
    GOSOM_DOCKER_IMAGE = "gosom:latest"
    GOSOM_OUTPUT_DIR = "/tmp/gmaps_dump"
    GOSOM_CONCURRENCY = 2
    GOSOM_DEPTH = 5
    GOSOM_TIMEOUT_S = 0
_fake_config_mod.get_settings = lambda: _FakeSettings()
sys.modules["config"] = _fake_config_mod

from pipelines.inmobiliario.scraping import gosom_runner as gr  # noqa: E402


# ─── Fixtures ─────────────────────────────────────────────────────────────────

def _run(coro):
    return asyncio.run(coro)


@pytest.fixture
def dump_jsonl(tmp_path: Path) -> Path:
    """Dump realista de gosom: mezcla de entradas válidas, cerradas y sin coords."""
    path = tmp_path / "out.jsonl"
    entradas = [
        {  # OK — restaurante
            "place_id": "abc123",
            "title": "Can Solé",
            "latitude": 41.3803, "longitude": 2.1911,
            "category": "Restaurant",
            "rating": 4.5, "review_count": 1200,
            "price": "€€€",
            "hours": {"mon": "13:00-16:00"},
        },
        {  # OK — barbería (mapea a estetica)
            "place_id": "def456",
            "title": "Barber Shop Gràcia",
            "latitude": 41.4050, "longitude": 2.1560,
            "category": "Barber shop",
            "rating": 4.8, "review_count": 320,
        },
        {  # FILTRADO — permanently_closed
            "place_id": "ghi789",
            "title": "Zombie Pizza",
            "latitude": 41.39, "longitude": 2.17,
            "category": "Pizzeria",
            "permanently_closed": True,
        },
        {  # FILTRADO — sin coordenadas
            "place_id": "jkl000",
            "title": "Fantasma",
            "category": "Restaurant",
        },
        {  # FILTRADO — categoría no mapea a ningún sector
            "place_id": "mno111",
            "title": "Taller Mecánico",
            "latitude": 41.40, "longitude": 2.18,
            "category": "Car repair",
        },
        {  # OK — gosom a veces usa `categories` (lista) en vez de `category`
            "place_id": "pqr222",
            "title": "Tatuajes La Ribera",
            "latitude": 41.384, "longitude": 2.183,
            "categories": ["Tattoo shop", "Body piercing"],
            "rating": 4.2,
        },
    ]
    path.write_text("\n".join(json.dumps(e) for e in entradas), encoding="utf-8")
    return path


# ─── Tests de funciones puras ────────────────────────────────────────────────

class TestSectorDesdeCategoria:
    def test_restaurant(self):
        assert gr.sector_desde_categoria("Restaurant") == "restauracion"

    def test_pizzeria_variante(self):
        assert gr.sector_desde_categoria("Italian Pizzeria") == "restauracion"

    def test_barbershop_es_estetica(self):
        assert gr.sector_desde_categoria("Barber shop") == "estetica"

    def test_tattoo(self):
        assert gr.sector_desde_categoria("Tattoo shop") == "tatuajes"

    def test_clothing(self):
        assert gr.sector_desde_categoria("Clothing store") == "moda"

    def test_shisha(self):
        assert gr.sector_desde_categoria("Shisha lounge") == "shisha_lounge"

    def test_desconocida_es_none(self):
        assert gr.sector_desde_categoria("Car repair") is None

    def test_none(self):
        assert gr.sector_desde_categoria(None) is None


class TestGosomPriceLevel:
    def test_euros(self):
        assert gr._gosom_price_level("€€€") == 3
        assert gr._gosom_price_level("€") == 1
        assert gr._gosom_price_level("€€€€") == 4

    def test_dolares(self):
        assert gr._gosom_price_level("$$") == 2

    def test_entero_directo(self):
        assert gr._gosom_price_level(2) == 2

    def test_fuera_de_rango(self):
        assert gr._gosom_price_level(7) is None
        assert gr._gosom_price_level("€€€€€") is None

    def test_none_y_vacio(self):
        assert gr._gosom_price_level(None) is None
        assert gr._gosom_price_level("") is None


class TestNormalizarRow:
    def test_row_valido(self):
        row = {
            "place_id": "xyz", "title": "Sitio",
            "latitude": 41.0, "longitude": 2.0,
            "category": "Restaurant",
            "rating": 4.2, "review_count": 100, "price": "€€",
        }
        out = gr._normalizar_row(row)
        assert out is not None
        assert out["id"] == "gs_xyz"
        assert out["sector_codigo"] == "restauracion"
        # "Restaurant" es terminal genérico → subsector None
        assert out["subsector_codigo"] is None
        assert out["rating"] == 4.2
        assert out["num_resenas"] == 100
        assert out["precio_nivel"] == 2
        assert out["fuente"] == "google_scrape"

    def test_row_con_subsector_especifico(self):
        row = {
            "place_id": "p1", "title": "Pizza Luigi",
            "latitude": 41.38, "longitude": 2.17,
            "category": "Pizzeria", "rating": 4.0,
        }
        out = gr._normalizar_row(row)
        assert out is not None
        assert out["sector_codigo"] == "restauracion"
        assert out["subsector_codigo"] == "pizzeria"

    def test_row_barberia(self):
        out = gr._normalizar_row({
            "place_id": "b1", "title": "Barber",
            "latitude": 41.4, "longitude": 2.15,
            "category": "Barber shop",
        })
        assert out is not None
        assert out["sector_codigo"] == "estetica"
        assert out["subsector_codigo"] == "barberia"

    def test_row_cerrado_se_conserva_con_flag(self):
        """A partir de migración 008, NO descartamos cerrados — los emitimos
        con _cerrado=True para que el ingestor alimente fecha_cierre_detectada
        y el backfill los vuelque a negocios_historico (label del XGBoost)."""
        out = gr._normalizar_row({
            "place_id": "x", "latitude": 1, "longitude": 2,
            "category": "Restaurant", "permanently_closed": True,
        })
        assert out is not None
        assert out["_cerrado"] is True
        assert out["sector_codigo"] == "restauracion"

    def test_row_activo_marca_cerrado_false(self):
        out = gr._normalizar_row({
            "place_id": "y", "latitude": 1, "longitude": 2,
            "category": "Restaurant",
        })
        assert out is not None
        assert out["_cerrado"] is False

    def test_row_sin_place_id_descarta(self):
        # Sin ID estable → duplicaríamos al reejecutar; mejor descartar.
        assert gr._normalizar_row({
            "title": "X", "latitude": 1, "longitude": 2, "category": "Restaurant",
        }) is None

    def test_row_sin_categoria_mapeable_descarta(self):
        assert gr._normalizar_row({
            "place_id": "x", "latitude": 1, "longitude": 2, "category": "Zoo",
        }) is None

    def test_row_categories_lista_con_fallback(self):
        # La primera categoría no mapea, la segunda sí.
        out = gr._normalizar_row({
            "place_id": "x", "latitude": 1, "longitude": 2,
            "categories": ["Unknown category", "Tattoo shop"],
        })
        assert out is not None
        assert out["sector_codigo"] == "tatuajes"
        assert out["subsector_codigo"] == "tatuaje"

    def test_row_principal_y_lista(self):
        # Cuando hay category (principal) y categories (lista), la principal gana.
        out = gr._normalizar_row({
            "place_id": "x", "latitude": 1, "longitude": 2,
            "category": "Coffee shop",
            "categories": ["Pizzeria"],
        })
        assert out is not None
        assert out["subsector_codigo"] == "cafeteria"


# ─── Tests de generación de queries ───────────────────────────────────────────

class TestGenerarQueries:
    def test_una_zona_un_sector(self):
        zonas = [{"zona_id": "z1", "nombre_barrio": "Eixample", "nombre_distrito": "Eixample"}]
        queries = gr.generar_queries(zonas, ["restauracion"])
        # 3 términos para restauracion en _TERMINOS_BUSQUEDA
        assert len(queries) == 3
        assert all(q.zona_id == "z1" for q in queries)
        assert all(q.sector == "restauracion" for q in queries)
        terminos = {q.termino for q in queries}
        assert "restaurante" in terminos

    def test_zona_sin_ubicacion_se_salta(self):
        zonas = [{"zona_id": "z1"}]  # sin barrio ni ubicación
        queries = gr.generar_queries(zonas, ["restauracion"])
        assert queries == []

    def test_escribir_queries_file(self, tmp_path: Path):
        zonas = [{"zona_id": "z1", "nombre_barrio": "Gràcia"}]
        queries = gr.generar_queries(zonas, ["tatuajes"])
        assert len(queries) == 1
        ruta = tmp_path / "queries.txt"
        gr.escribir_queries_file(queries, ruta)
        contenido = ruta.read_text(encoding="utf-8")
        assert "estudio de tatuajes" in contenido
        assert "Gràcia" in contenido


# ─── Tests del parser del dump ────────────────────────────────────────────────

class TestParsearDump:
    def test_filtra_y_normaliza(self, dump_jsonl: Path):
        """De 6 entradas del dump, 4 son utilizables ahora:
          - Can Solé (restauracion, activo)
          - Barber Shop (estetica, activo)
          - Zombie Pizza (restauracion, _cerrado=True — sobrevive para alimentar cierres)
          - Tatuajes La Ribera (tatuajes, activo)

        Se descartan sólo 'Fantasma' (sin coords) y 'Taller Mecánico' (sin sector).
        """
        salida = list(gr.parsear_dump(dump_jsonl))
        assert len(salida) == 4
        nombres = {n["nombre"] for n in salida}
        assert nombres == {"Can Solé", "Barber Shop Gràcia",
                           "Zombie Pizza", "Tatuajes La Ribera"}
        for n in salida:
            assert n["fuente"] == "google_scrape"
            assert n["lat"] is not None and n["lng"] is not None
        cerrados = [n for n in salida if n["_cerrado"]]
        assert len(cerrados) == 1
        assert cerrados[0]["nombre"] == "Zombie Pizza"

    def test_filtra_por_sectores_admitidos(self, dump_jsonl: Path):
        """Sólo restauración — Can Solé (activo) + Zombie Pizza (cerrado)."""
        salida = list(gr.parsear_dump(dump_jsonl, sectores_admitidos=["restauracion"]))
        assert len(salida) == 2
        assert all(n["sector_codigo"] == "restauracion" for n in salida)

    def test_dump_inexistente_no_crashea(self, tmp_path: Path):
        salida = list(gr.parsear_dump(tmp_path / "no_existe.jsonl"))
        assert salida == []

    def test_json_invalido_se_ignora(self, tmp_path: Path):
        path = tmp_path / "malo.jsonl"
        path.write_text(
            '{"place_id": "ok", "title": "T", "latitude": 1, "longitude": 2, "category": "Restaurant"}\n'
            'esto no es JSON\n'
            '{"place_id": "ok2", "title": "T2", "latitude": 3, "longitude": 4, "category": "Restaurant"}\n',
            encoding="utf-8",
        )
        salida = list(gr.parsear_dump(path))
        assert len(salida) == 2


# ─── Tests de ejecutar() ──────────────────────────────────────────────────────

class TestEjecutar:
    def test_modo_disabled_no_lanza_proceso(self, tmp_path: Path):
        # En modo disabled no arranca nada y devuelve exit_code=-1 aunque no
        # exista el fichero de queries. Así el pipeline de ingesta puede correr
        # contra un dump pre-existente en CI/dev sin red.
        result = _run(gr.ejecutar(tmp_path / "queries.txt",
                                   output_file=tmp_path / "out.jsonl"))
        assert result.exit_code == -1
        assert result.output_path == tmp_path / "out.jsonl"
