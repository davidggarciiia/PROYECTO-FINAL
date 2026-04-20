"""
tests/test_places_router.py — Tests del router de Places.

Cubre:
  - `_dedup_negocios` (función pura): colapsa duplicados por (nombre, lat, lng)
    redondeados y prefiere la fuente más informativa.
  - `_normalizar_nombre`: normaliza tildes, mayúsculas y símbolos.
  - `_google_price_level` y `_yelp_price_level`: mapeos a 1-4 con None bien propagado.
  - `CompetenciaResult`: propiedades agregadas (num, rating, score_saturacion) y
    su tolerancia a cobertura='ninguna'.
  - `consultar_competencia`: la política ya NO es "primera que responda" sino
    "combinar hasta tener resultados sólidos". Con Google devolviendo pocos
    resultados se consulta Foursquare/OSM y se deduplican.
  - OSM query NO contiene el typo `skt` (era un bug silencioso de producción).
  - `_buscar_google` excluye businessStatus != OPERATIONAL.

Los proveedores se mockean a nivel de `_llamar_proveedor` para no depender de
httpx/redis reales en CI. Para el test concreto de OSM y businessStatus, se
parchea `httpx.AsyncClient` con un stub que devuelve un JSON controlado.
"""
from __future__ import annotations

import asyncio
import sys
from unittest.mock import AsyncMock, MagicMock

import pytest

# conftest.py stubea `routers` y `db` a nivel global con MagicMocks — útil para
# los tests que sólo tocan funciones puras, pero incompatible con este archivo,
# que necesita importar el módulo REAL `routers.places_router`. Limpiamos los
# stubs problemáticos y proveemos un `db.redis_client` mínimo antes de importar.
for _mod in list(sys.modules):
    if _mod == "routers" or _mod.startswith("routers."):
        del sys.modules[_mod]

# Proveer `db.redis_client` como stub (places_router sólo necesita `get_redis`).
# Nota: el stub real se sustituye por `fake_redis` fixture vía monkeypatch.
_fake_redis_client = MagicMock()
_fake_redis_client.get_redis = lambda: MagicMock()
sys.modules["db.redis_client"] = _fake_redis_client

import routers.places_router as pr  # noqa: E402


# ─── Fakes compartidos ────────────────────────────────────────────────────────

class _FakeRedis:
    """Redis mínimo en memoria: get/setex/incr, sin TTL real."""
    def __init__(self):
        self.store: dict[str, str] = {}

    async def get(self, key):
        return self.store.get(key)

    async def setex(self, key, _ttl, value):
        self.store[key] = str(value)

    async def incr(self, key):
        self.store[key] = str(int(self.store.get(key, "0")) + 1)


@pytest.fixture
def fake_redis(monkeypatch):
    r = _FakeRedis()
    monkeypatch.setattr(pr, "get_redis", lambda: r)
    return r


@pytest.fixture
def google_habilitado(monkeypatch):
    """Activa PLACES_GOOGLE_API_ENABLED para los tests que comprueban la cadena
    completa con Google como primer proveedor."""
    class _S:
        PLACES_GOOGLE_API_ENABLED = True
    fake = MagicMock()
    fake.get_settings = lambda: _S()
    monkeypatch.setitem(sys.modules, "config", fake)


def _n(fuente, nombre, lat, lng, rating=None, num_resenas=None, precio=None):
    """Factory compacto para crear dicts de negocio en los tests."""
    return {
        "id": f"{fuente}_{nombre}",
        "nombre": nombre, "lat": lat, "lng": lng,
        "rating": rating, "num_resenas": num_resenas, "precio_nivel": precio,
        "sector_codigo": "restauracion", "horario": None, "fuente": fuente,
    }


# ─── Tests de funciones puras ─────────────────────────────────────────────────

class TestNormalizarNombre:
    def test_colapsa_tildes_y_mayusculas(self):
        assert pr._normalizar_nombre("Café Niño") == pr._normalizar_nombre("cafe nino")

    def test_ignora_espacios_y_puntuacion(self):
        assert pr._normalizar_nombre("Can Paixano, SL") == pr._normalizar_nombre("canpaixano sl")

    def test_vacio_no_crashea(self):
        assert pr._normalizar_nombre("") == ""


class TestDedup:
    def test_misma_coord_y_nombre_se_colapsan(self):
        a = _n("google", "Café Central", 41.38501, 2.17342, rating=4.0, num_resenas=100)
        b = _n("osm",    "cafe central", 41.38502, 2.17342)
        out = pr._dedup_negocios([a, b])
        assert len(out) == 1
        # Se queda con el de Google (más prioridad + más info)
        assert out[0]["fuente"] == "google"

    def test_coordenadas_distintas_se_mantienen(self):
        a = _n("google", "Café Central", 41.38501, 2.17342)
        b = _n("google", "Café Central", 41.39000, 2.18000)
        out = pr._dedup_negocios([a, b])
        assert len(out) == 2

    def test_sin_coordenadas_se_descarta(self):
        a = _n("google", "Sin Coord", None, None)
        out = pr._dedup_negocios([a])
        assert out == []

    def test_prefiere_fuente_con_mas_resenas_si_misma_prioridad(self):
        a = _n("google", "Bar X", 41.0, 2.0, rating=4.0, num_resenas=10)
        b = _n("google", "Bar X", 41.0, 2.0, rating=4.0, num_resenas=200)
        out = pr._dedup_negocios([a, b])
        assert out[0]["num_resenas"] == 200


class TestPriceLevels:
    def test_google_price_mapping(self):
        assert pr._google_price_level("PRICE_LEVEL_INEXPENSIVE") == 1
        assert pr._google_price_level("PRICE_LEVEL_VERY_EXPENSIVE") == 4
        assert pr._google_price_level(None) is None
        assert pr._google_price_level("UNKNOWN") is None

    def test_yelp_price_mapping(self):
        assert pr._yelp_price_level("$") == 1
        assert pr._yelp_price_level("$$$$") == 4
        # None y vacío no explotan — devuelven None en lugar de mezclar con 0.
        assert pr._yelp_price_level(None) is None
        assert pr._yelp_price_level("") is None


# ─── Tests de CompetenciaResult ───────────────────────────────────────────────

class TestCompetenciaResult:
    def test_num_y_rating_medio(self):
        r = pr.CompetenciaResult(
            negocios=[_n("google", "A", 1.0, 2.0, rating=4.0),
                      _n("google", "B", 1.1, 2.1, rating=5.0)],
            cobertura="alta",
        )
        assert r.num_competidores == 2
        assert r.rating_medio == 4.5

    def test_score_saturacion_solo_si_hay_cobertura(self):
        r = pr.CompetenciaResult(negocios=[], cobertura="ninguna")
        assert r.score_saturacion is None

    def test_score_saturacion_escala_0_100(self):
        negocios = [_n("google", f"N{i}", 1.0 + i * 0.01, 2.0, rating=4.0)
                    for i in range(25)]
        r = pr.CompetenciaResult(negocios=negocios, cobertura="alta")
        assert r.score_saturacion == 100.0  # > 20 competidores → 100


# ─── Tests de consultar_competencia con proveedores mockeados ─────────────────

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro) if False else asyncio.run(coro)


class TestConsultarCompetencia:

    def test_cobertura_alta_con_dos_fuentes(self, fake_redis, google_habilitado, monkeypatch):
        """Google y Foursquare responden → cobertura='alta' y combina ambas."""
        async def fake_llamar(prv, *a, **kw):
            if prv == "google":
                return [_n("google", "Bar A", 41.0, 2.0, rating=4.0)]
            if prv == "foursquare":
                return [_n("foursquare", "Bar B", 41.01, 2.01, rating=3.5)]
            return []
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(41.0, 2.0, "restauracion"))
        assert res.cobertura == "alta"
        assert res.num_competidores == 2
        assert set(res.fuentes_consultadas) == {"google", "foursquare"}

    def test_cobertura_media_con_una_fuente(self, fake_redis, google_habilitado, monkeypatch):
        async def fake_llamar(prv, *a, **kw):
            if prv == "google":
                return [_n("google", "Único", 41.0, 2.0)]
            return []  # Foursquare, Yelp y OSM vacíos
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(41.0, 2.0, "restauracion"))
        assert res.cobertura == "media"
        assert res.num_competidores == 1

    def test_cobertura_ninguna_si_todo_rate_limited(self, fake_redis, google_habilitado, monkeypatch):
        async def fake_llamar(prv, *a, **kw):
            raise pr._RateLimitError()
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(41.0, 2.0, "restauracion"))
        assert res.cobertura == "ninguna"
        assert res.num_competidores == 0
        assert set(res.fuentes_con_rate_limit) == {"google", "foursquare", "yelp", "osm"}

    def test_rate_limit_marca_proveedor_en_redis(self, fake_redis, google_habilitado, monkeypatch):
        async def fake_llamar(prv, *a, **kw):
            if prv == "google":
                raise pr._RateLimitError()
            return []
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        _run(pr.consultar_competencia(41.0, 2.0, "restauracion"))
        assert fake_redis.store.get("places:exhausted:google") == "1"
        # Contador diario se ha incrementado
        assert any(k.startswith("places:rl:google:") for k in fake_redis.store)

    def test_combina_cuando_google_devuelve_pocos(self, fake_redis, google_habilitado, monkeypatch):
        """Bug original: Google devuelve 1 resultado → router para ahí. Ahora combina."""
        async def fake_llamar(prv, *a, **kw):
            if prv == "google":
                return [_n("google", "Uno", 41.0, 2.0)]
            if prv == "foursquare":
                return [_n("foursquare", "Dos", 41.01, 2.01),
                        _n("foursquare", "Tres", 41.02, 2.02)]
            return []
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(41.0, 2.0, "restauracion"))
        assert res.num_competidores == 3
        assert res.cobertura == "alta"

    def test_dedup_entre_fuentes(self, fake_redis, google_habilitado, monkeypatch):
        """Si el mismo negocio aparece en Google y OSM no cuenta dos veces."""
        async def fake_llamar(prv, *a, **kw):
            if prv == "google":
                return [_n("google", "Bar Nova", 41.00001, 2.00001, rating=4.2, num_resenas=80)]
            if prv == "foursquare":
                return [_n("foursquare", "Bar Nova", 41.00000, 2.00000)]
            return []
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(41.0, 2.0, "restauracion"))
        assert res.num_competidores == 1
        # El ganador tiene que ser la entrada más informativa (Google con rating)
        assert res.negocios[0]["fuente"] == "google"


# ─── Tests del query de Overpass (regresión del typo skt/skel) ────────────────

class TestQueryOverpass:

    class _FakeResponse:
        def __init__(self, data): self._data = data; self.status_code = 200
        def raise_for_status(self): pass
        def json(self): return self._data

    class _FakeAsyncClient:
        captured_query: str = ""

        def __init__(self, *a, **kw): pass
        async def __aenter__(self): return self
        async def __aexit__(self, *exc): return False

        async def post(self, url, data=None, json=None, **kw):
            # Capturamos el query del body `data` de Overpass
            if data and "data" in data:
                TestQueryOverpass._FakeAsyncClient.captured_query = data["data"]
            return TestQueryOverpass._FakeResponse({"elements": []})

        async def get(self, *a, **kw):
            return TestQueryOverpass._FakeResponse({})

    def test_query_osm_usa_skel_no_skt(self, monkeypatch):
        """Antes el query llevaba `out skt qt;` (typo). Overpass devolvía 400.
        Este test verifica la regresión: el query ya no contiene 'skt'."""
        TestQueryOverpass._FakeAsyncClient.captured_query = ""
        monkeypatch.setattr(pr.httpx, "AsyncClient", TestQueryOverpass._FakeAsyncClient)

        _run(pr._buscar_osm(41.0, 2.0, "restauracion", 300, 50))

        q = TestQueryOverpass._FakeAsyncClient.captured_query
        assert "skt" not in q, f"El query de Overpass aún contiene el typo 'skt': {q!r}"
        assert "out center" in q or "skel" in q

    def test_query_osm_incluye_relation(self, monkeypatch):
        TestQueryOverpass._FakeAsyncClient.captured_query = ""
        monkeypatch.setattr(pr.httpx, "AsyncClient", TestQueryOverpass._FakeAsyncClient)
        _run(pr._buscar_osm(41.0, 2.0, "restauracion", 300, 50))
        assert "relation" in TestQueryOverpass._FakeAsyncClient.captured_query


# ─── Tests de Google businessStatus filter ────────────────────────────────────

class TestProveedoresActivosFlag:
    """Regresión: la cadena de proveedores cambia según PLACES_GOOGLE_API_ENABLED."""

    def _patch_flag(self, monkeypatch, valor: bool):
        import sys
        from unittest.mock import MagicMock
        class _S:
            PLACES_GOOGLE_API_ENABLED = valor
        fake = MagicMock()
        fake.get_settings = lambda: _S()
        monkeypatch.setitem(sys.modules, "config", fake)

    def test_google_fuera_por_defecto(self, monkeypatch):
        self._patch_flag(monkeypatch, False)
        activos = pr._proveedores_activos()
        assert "google" not in activos
        assert activos[0] == "foursquare"

    def test_google_dentro_con_flag(self, monkeypatch):
        self._patch_flag(monkeypatch, True)
        activos = pr._proveedores_activos()
        assert activos[0] == "google"
        assert "osm" in activos


class TestCacheRedisHit:
    """`consultar_competencia` con `zona_id` consulta la caché primero; si hay
    entrada, no arranca ningún proveedor externo."""

    def test_hit_evita_proveedores(self, fake_redis, monkeypatch):
        import json as _j
        fake_redis.store["comp:zona_X:restauracion:300"] = _j.dumps({
            "num_competidores": 7,
            "rating_medio": 4.1,
            "score_saturacion": 35.0,
            "cobertura": "alta",
        })

        # Si se llamara a un proveedor explota — probamos que ni se intenta.
        async def fake_llamar(*a, **kw):
            raise AssertionError("No debería llamar a proveedores si hay caché")
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(
            41.0, 2.0, "restauracion", radio_m=300, zona_id="zona_X"))
        assert res.cobertura == "alta"
        assert res.num_competidores == 7
        assert res.fuentes_consultadas == ["cache"]

    def test_miss_cae_en_proveedores(self, fake_redis, monkeypatch):
        # No poblamos la caché → miss → va a proveedores.
        async def fake_llamar(prv, *a, **kw):
            if prv == "foursquare":
                return [_n("foursquare", "X", 41.0, 2.0)]
            return []
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(
            41.0, 2.0, "restauracion", radio_m=300, zona_id="zona_desconocida"))
        assert res.num_competidores == 1
        assert "cache" not in res.fuentes_consultadas

    def test_cache_cobertura_ninguna_se_respeta(self, fake_redis, monkeypatch):
        import json as _j
        fake_redis.store["comp:zona_vacia:moda:300"] = _j.dumps({
            "num_competidores": 0,
            "rating_medio": None,
            "score_saturacion": None,
            "cobertura": "ninguna",
        })
        async def fake_llamar(*a, **kw):
            raise AssertionError("tampoco aquí")
        monkeypatch.setattr(pr, "_llamar_proveedor", fake_llamar)

        res = _run(pr.consultar_competencia(
            41.0, 2.0, "moda", radio_m=300, zona_id="zona_vacia"))
        assert res.cobertura == "ninguna"
        assert res.num_competidores == 0


class TestGoogleBusinessStatus:

    class _FakeResponse:
        def __init__(self, data): self._data = data; self.status_code = 200
        def raise_for_status(self): pass
        def json(self): return self._data

    def _fake_response_with_places(self, places):
        resp = self._FakeResponse({"places": places})
        client = MagicMock()
        client.__aenter__ = AsyncMock(return_value=client)
        client.__aexit__ = AsyncMock(return_value=False)
        client.post = AsyncMock(return_value=resp)
        return client

    def test_excluye_cerrados_permanentemente(self, monkeypatch):
        monkeypatch.setenv("GOOGLE_MAPS_API_KEY", "x")
        places = [
            {"id": "1", "displayName": {"text": "Abierto"},
             "location": {"latitude": 41.0, "longitude": 2.0},
             "businessStatus": "OPERATIONAL"},
            {"id": "2", "displayName": {"text": "Cerrado"},
             "location": {"latitude": 41.01, "longitude": 2.01},
             "businessStatus": "CLOSED_PERMANENTLY"},
            {"id": "3", "displayName": {"text": "Cerrado temporal"},
             "location": {"latitude": 41.02, "longitude": 2.02},
             "businessStatus": "CLOSED_TEMPORARILY"},
        ]
        client = self._fake_response_with_places(places)
        monkeypatch.setattr(pr.httpx, "AsyncClient", lambda *a, **kw: client)

        negocios = _run(pr._buscar_google(41.0, 2.0, "restauracion", 300, 20))
        nombres = {n["nombre"] for n in negocios}
        assert "Abierto" in nombres
        assert "Cerrado" not in nombres
        assert "Cerrado temporal" not in nombres
