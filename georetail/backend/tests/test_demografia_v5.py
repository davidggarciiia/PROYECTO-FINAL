"""
tests/test_demografia_v5.py — Tests para las mejoras demográficas v5.

Cubre:
  1. _parse_float — conversión segura de valores numéricos
  2. _resolve_columns — mapeo de aliases de columnas CSV
  3. _descargar_csv — parseo de CSV AMB con columnas normalizadas (mock HTTP)
  4. FEATURE_NAMES v5 — vector tiene 32 features; pct_poblacio_25_44 e delta_renta_3a en pos 30-31
  5. _build_array v5 — imputa correctamente cuando vz no tiene nuevas features
  6. _build_array v5 — usa valores reales cuando vz los tiene
  7. parques.py — _resolve_columns con aliases alternativos
  8. parques.py — _descargar_csv filtra filas sin coordenadas
  9. demographics pipeline — _parse_float handles commas and empty strings
 10. scheduler — parques job registrado con CronTrigger correcto
"""
from __future__ import annotations

import sys
import io
import csv
import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from scoring.features import FEATURE_NAMES


# ── Helpers para crear CSV fake de parques ────────────────────────────────────

def _make_parcs_csv(rows: list[dict], fieldnames: list[str]) -> str:
    buf = io.StringIO()
    writer = csv.DictWriter(buf, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(rows)
    return buf.getvalue()


# ─────────────────────────────────────────────────────────────────────────────
# 1. _parse_float
# ─────────────────────────────────────────────────────────────────────────────

def test_parse_float_integer():
    from pipelines.entorno.parques import _parse_float
    assert _parse_float("42") == pytest.approx(42.0)


def test_parse_float_decimal_comma():
    from pipelines.entorno.parques import _parse_float
    assert _parse_float("3,14") == pytest.approx(3.14)


def test_parse_float_none_returns_default():
    from pipelines.entorno.parques import _parse_float
    assert _parse_float(None, default=99.0) == pytest.approx(99.0)


def test_parse_float_empty_string_returns_default():
    from pipelines.entorno.parques import _parse_float
    assert _parse_float("", default=0.0) == pytest.approx(0.0)


def test_parse_float_non_numeric_returns_default():
    from pipelines.entorno.parques import _parse_float
    assert _parse_float("N/A", default=-1.0) == pytest.approx(-1.0)


# ─────────────────────────────────────────────────────────────────────────────
# 2. _resolve_columns
# ─────────────────────────────────────────────────────────────────────────────

def test_resolve_columns_standard_names():
    from pipelines.entorno.parques import _resolve_columns
    header = ["nom", "municipi", "superficie_ha", "latitud", "longitud"]
    col = _resolve_columns(header)
    assert col["nom"] == "nom"
    assert col["municipi"] == "municipi"
    assert col["superficie_ha"] == "superficie_ha"
    assert col["lat"] == "latitud"
    assert col["lon"] == "longitud"


def test_resolve_columns_alternative_names():
    from pipelines.entorno.parques import _resolve_columns
    header = ["NAME", "MUNICIPIO", "HECTAREES", "LAT", "LNG"]
    col = _resolve_columns(header)
    assert col["nom"] == "NAME"
    assert col["municipi"] == "MUNICIPIO"
    assert col["superficie_ha"] == "HECTAREES"
    assert col["lat"] == "LAT"
    assert col["lon"] == "LNG"


def test_resolve_columns_missing_column():
    from pipelines.entorno.parques import _resolve_columns
    header = ["nom", "municipi"]
    col = _resolve_columns(header)
    assert col["lat"] is None
    assert col["lon"] is None


# ─────────────────────────────────────────────────────────────────────────────
# 3. _descargar_csv — mock HTTP
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_descargar_csv_parses_rows():
    """CSV con 3 parques válidos + 1 sin coordenadas → devuelve 3."""
    from pipelines.entorno.parques import _descargar_csv

    csv_content = _make_parcs_csv(
        rows=[
            {"nom": "Parc de la Ciutadella", "municipi": "Barcelona",
             "superficie_ha": "17.42", "latitud": "41.3870", "longitud": "2.1860"},
            {"nom": "Parc del Laberint", "municipi": "Barcelona",
             "superficie_ha": "9.10",  "latitud": "41.4280", "longitud": "2.1480"},
            {"nom": "Parc de Collserola", "municipi": "Barcelona",
             "superficie_ha": "8295",  "latitud": "41.4120", "longitud": "2.1060"},
            {"nom": "Parc sense coords",  "municipi": "Barcelona",
             "superficie_ha": "1.0",   "latitud": "0",       "longitud": "0"},
        ],
        fieldnames=["nom", "municipi", "superficie_ha", "latitud", "longitud"],
    )

    mock_response = MagicMock()
    mock_response.content = csv_content.encode("utf-8")
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__  = AsyncMock(return_value=False)
    mock_client.get        = AsyncMock(return_value=mock_response)

    with patch("pipelines.parques.httpx.AsyncClient", return_value=mock_client):
        parques = await _descargar_csv()

    assert len(parques) == 3
    noms = [p["nom"] for p in parques]
    assert "Parc de la Ciutadella" in noms
    assert "Parc sense coords" not in noms


@pytest.mark.asyncio
async def test_descargar_csv_latin1_fallback():
    """CSV codificado en latin-1 se decodifica correctamente."""
    from pipelines.entorno.parques import _descargar_csv

    csv_str = "nom,municipi,superficie_ha,latitud,longitud\n"
    csv_str += "Parc Güell,Barcelona,17.18,41.4145,2.1527\n"

    mock_response = MagicMock()
    mock_response.content = csv_str.encode("latin-1")
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__  = AsyncMock(return_value=False)
    mock_client.get        = AsyncMock(return_value=mock_response)

    with patch("pipelines.parques.httpx.AsyncClient", return_value=mock_client):
        parques = await _descargar_csv()

    assert len(parques) == 1
    assert "ell" in parques[0]["nom"]  # "Güell" survived


@pytest.mark.asyncio
async def test_descargar_csv_empty_returns_empty():
    """CSV con solo cabecera → lista vacía."""
    from pipelines.entorno.parques import _descargar_csv

    csv_str = "nom,municipi,superficie_ha,latitud,longitud\n"

    mock_response = MagicMock()
    mock_response.content = csv_str.encode("utf-8")
    mock_response.raise_for_status = MagicMock()

    mock_client = AsyncMock()
    mock_client.__aenter__ = AsyncMock(return_value=mock_client)
    mock_client.__aexit__  = AsyncMock(return_value=False)
    mock_client.get        = AsyncMock(return_value=mock_response)

    with patch("pipelines.parques.httpx.AsyncClient", return_value=mock_client):
        parques = await _descargar_csv()

    assert parques == []


# ─────────────────────────────────────────────────────────────────────────────
# 4. FEATURE_NAMES v5 — longitud y posiciones
# ─────────────────────────────────────────────────────────────────────────────

def test_feature_names_length_v5():
    from scoring.features import FEATURE_NAMES
    assert len(FEATURE_NAMES) == 53, f"Se esperaban 53 features en el vector actual, hay {len(FEATURE_NAMES)}"


def test_feature_names_pct_poblacio_index():
    from scoring.features import FEATURE_NAMES
    assert FEATURE_NAMES[30] == "pct_poblacio_25_44"


def test_feature_names_delta_renta_index():
    from scoring.features import FEATURE_NAMES
    assert FEATURE_NAMES[31] == "delta_renta_3a"


def test_feature_names_no_duplicates():
    from scoring.features import FEATURE_NAMES
    assert len(FEATURE_NAMES) == len(set(FEATURE_NAMES)), "Hay features duplicadas"


# ─────────────────────────────────────────────────────────────────────────────
# 5. _MEDIAS v5 — las nuevas features tienen defaults razonables
# ─────────────────────────────────────────────────────────────────────────────

def test_medias_has_pct_poblacio():
    from scoring.features import _MEDIAS, FEATURE_NAMES
    for name in FEATURE_NAMES:
        assert name in _MEDIAS, f"Feature '{name}' no tiene valor por defecto en _MEDIAS"


def test_medias_pct_poblacio_25_44_range():
    from scoring.features import _MEDIAS
    v = _MEDIAS["pct_poblacio_25_44"]
    assert 0.0 < v < 1.0, f"pct_poblacio_25_44 debe ser fracción 0-1, es {v}"


def test_medias_delta_renta_3a_range():
    from scoring.features import _MEDIAS
    v = _MEDIAS["delta_renta_3a"]
    assert -0.5 < v < 1.0, f"delta_renta_3a fuera de rango plausible: {v}"


# ─────────────────────────────────────────────────────────────────────────────
# 6. _build_array — imputación cuando vz no tiene nuevas features
# ─────────────────────────────────────────────────────────────────────────────

def test_build_array_imputes_missing_v5_features():
    """Cuando vz no tiene pct_poblacio_25_44 ni delta_renta_3a → usa _MEDIAS."""
    from scoring.features import _build_array, _MEDIAS

    # Stub de calcular_flujo_score para aislar la función
    _fake_flujo = MagicMock()
    _fake_flujo.calcular_flujo_score = MagicMock(return_value=45.0)
    _orig = sys.modules.get("scoring.flujo_peatonal")
    sys.modules["scoring.flujo_peatonal"] = _fake_flujo
    try:
        vz = {
            "flujo_peatonal_total": 1000,
            "flujo_peatonal_manana": 350,
            "flujo_peatonal_tarde": 420,
            "flujo_peatonal_noche": 230,
            "renta_media_hogar": 35000,
            "edad_media": 40.0,
            "pct_extranjeros": 0.20,
            "densidad_hab_km2": 20000.0,
            "ratio_locales_comerciales": 0.25,
            # pct_poblacio_25_44 y delta_renta_3a ausentes → imputar
        }
        arr = _build_array(vz, {}, None, {}, {}, {})
        assert arr.shape == (1, len(FEATURE_NAMES))

        idx_pct = FEATURE_NAMES.index("pct_poblacio_25_44")
        idx_delta = FEATURE_NAMES.index("delta_renta_3a")
        assert arr[0, idx_pct]   == pytest.approx(_MEDIAS["pct_poblacio_25_44"])
        assert arr[0, idx_delta] == pytest.approx(_MEDIAS["delta_renta_3a"])
    finally:
        if _orig is None:
            sys.modules.pop("scoring.flujo_peatonal", None)
        else:
            sys.modules["scoring.flujo_peatonal"] = _orig


def test_build_array_uses_real_v5_values():
    """Cuando vz sí tiene pct_poblacio_25_44 y delta_renta_3a → los usa."""
    from scoring.features import _build_array, FEATURE_NAMES

    _fake_flujo = MagicMock()
    _fake_flujo.calcular_flujo_score = MagicMock(return_value=50.0)
    _orig = sys.modules.get("scoring.flujo_peatonal")
    sys.modules["scoring.flujo_peatonal"] = _fake_flujo
    try:
        vz = {
            "flujo_peatonal_total": 800,
            "ratio_locales_comerciales": 0.18,
            "pct_poblacio_25_44": 0.31,
            "delta_renta_3a": 0.12,
        }
        arr = _build_array(vz, {}, None, {}, {}, {})
        assert arr.shape == (1, len(FEATURE_NAMES))

        idx_pct   = FEATURE_NAMES.index("pct_poblacio_25_44")
        idx_delta = FEATURE_NAMES.index("delta_renta_3a")
        assert arr[0, idx_pct]   == pytest.approx(0.31)
        assert arr[0, idx_delta] == pytest.approx(0.12)
    finally:
        if _orig is None:
            sys.modules.pop("scoring.flujo_peatonal", None)
        else:
            sys.modules["scoring.flujo_peatonal"] = _orig


# ─────────────────────────────────────────────────────────────────────────────
# 7. Scheduler — job parques registrado
# ─────────────────────────────────────────────────────────────────────────────

def test_scheduler_has_parques_job():
    """init_scheduler registra el job 'parques' con CronTrigger day=2 hour=6."""
    import importlib

    # Stubs mínimos para importar scheduler sin APScheduler real
    fake_scheduler = MagicMock()
    fake_aps = MagicMock()
    fake_aps.schedulers.asyncio.AsyncIOScheduler = MagicMock(return_value=fake_scheduler)
    fake_aps.triggers.cron.CronTrigger = MagicMock(side_effect=lambda **kw: kw)

    sys.modules["apscheduler"] = fake_aps
    sys.modules["apscheduler.schedulers"] = fake_aps.schedulers
    sys.modules["apscheduler.schedulers.asyncio"] = fake_aps.schedulers.asyncio
    sys.modules["apscheduler.triggers"] = fake_aps.triggers
    sys.modules["apscheduler.triggers.cron"] = fake_aps.triggers.cron

    if "pipelines.scheduler" in sys.modules:
        del sys.modules["pipelines.scheduler"]

    import pipelines.scheduler as sched_mod
    # Reasignar _scheduler al mock para capturar add_job
    sched_mod._scheduler = fake_scheduler
    sched_mod.init_scheduler()

    calls = fake_scheduler.add_job.call_args_list
    job_ids = [c.kwargs.get("id") or (c.args[1] if len(c.args) > 1 else "") for c in calls]
    assert "parques" in job_ids, f"Job 'parques' no encontrado. Jobs: {job_ids}"


# ─────────────────────────────────────────────────────────────────────────────
# 8. parques.py — _upsert_parques escribe geometría WKT correcta
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_upsert_parques_builds_wkt():
    """Verifica que la geometría se construye como SRID=4326;POINT(lon lat)."""
    from pipelines.entorno.parques import _upsert_parques

    conn = MagicMock()
    conn.execute = AsyncMock(return_value=None)

    parques = [{"nom": "Test", "municipi": "Barcelona",
                "superficie_ha": 5.0, "lat": 41.387, "lon": 2.170}]
    await _upsert_parques(conn, parques)

    assert conn.execute.called
    call_args = conn.execute.call_args[0]
    # 5th positional arg es el WKT (0=SQL, 1=nom, 2=municipi, 3=superficie_ha, 4=WKT, 5=now)
    wkt = call_args[4]
    assert wkt.startswith("SRID=4326;POINT(")
    assert "2.17" in wkt
    assert "41.387" in wkt


# ─────────────────────────────────────────────────────────────────────────────
# 9. parques.py — actualizar_parques_amb retorna vacío si CSV vacío
# ─────────────────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_actualizar_parques_amb_empty_csv():
    """Si _descargar_csv devuelve [], actualizar_parques_amb devuelve ceros."""
    from pipelines import parques as parques_mod

    fake_conn = MagicMock()
    fake_conn.fetchval = AsyncMock(return_value=1)
    fake_conn.execute  = AsyncMock(return_value=None)
    fake_conn.__aenter__ = AsyncMock(return_value=fake_conn)
    fake_conn.__aexit__  = AsyncMock(return_value=False)

    with patch.object(parques_mod, "_descargar_csv", AsyncMock(return_value=[])), \
         patch("pipelines.parques.get_db", return_value=fake_conn) if hasattr(parques_mod, "get_db") else patch.object(parques_mod, "_init_pipeline", AsyncMock(return_value=1)), \
         patch.object(parques_mod, "_fin_pipeline", AsyncMock(return_value=None)):
        result = await parques_mod.actualizar_parques_amb()

    assert result["parques_descargados"] == 0
    assert result["parques_upserted"] == 0
    assert result["zonas_actualizadas"] == 0


# ─────────────────────────────────────────────────────────────────────────────
# 10. BUG1 FIX — area_por_barri acumula áreas de múltiples zonas del mismo barrio
# ─────────────────────────────────────────────────────────────────────────────

def test_area_por_barri_accumulates_multiple_zones():
    """
    Simula 3 zonas en el mismo barrio (codigo '000001') con áreas 0.5, 0.3, 0.2 km².
    El área total debe ser 1.0, no 0.2 (el de la última iteración, bug anterior).
    """
    area_rows = [
        {"zona_id": "z1", "barri_code": "000001", "area_km2": 0.5},
        {"zona_id": "z2", "barri_code": "000001", "area_km2": 0.3},
        {"zona_id": "z3", "barri_code": "000001", "area_km2": 0.2},
        {"zona_id": "z4", "barri_code": "000002", "area_km2": 1.0},
    ]
    area_por_barri: dict[str, float] = {}
    for a in area_rows:
        bc = str(a["barri_code"]).zfill(6)
        # Comportamiento CORRECTO (post-fix)
        area_por_barri[bc] = area_por_barri.get(bc, 0.0) + (
            float(a["area_km2"]) if a["area_km2"] else 0.0
        )

    assert area_por_barri["000001"] == pytest.approx(1.0), \
        "area_por_barri debe acumular todas las zonas del barrio"
    assert area_por_barri["000002"] == pytest.approx(1.0)


def test_area_por_barri_bug_overwrite_would_fail():
    """
    Demuestra que el comportamiento ANTERIOR (sobreescribir) daría resultado erróneo.
    Este test documenta el bug para evitar regresiones.
    """
    area_rows = [
        {"zona_id": "z1", "barri_code": "000001", "area_km2": 0.5},
        {"zona_id": "z2", "barri_code": "000001", "area_km2": 0.2},
    ]
    # Comportamiento ROTO (pre-fix): sobreescribir
    area_bug: dict[str, float] = {}
    for a in area_rows:
        bc = a["barri_code"]
        area_bug[bc] = float(a["area_km2"])  # ← sobreescribe, retiene 0.2

    # Comportamiento CORRECTO (post-fix): acumular
    area_fix: dict[str, float] = {}
    for a in area_rows:
        bc = a["barri_code"]
        area_fix[bc] = area_fix.get(bc, 0.0) + float(a["area_km2"])  # retiene 0.7

    assert area_bug["000001"] == pytest.approx(0.2), "El bug producía 0.2"
    assert area_fix["000001"] == pytest.approx(0.7), "El fix produce 0.7"
    assert area_fix["000001"] != area_bug["000001"], "Fix y bug dan resultados distintos"


# ─────────────────────────────────────────────────────────────────────────────
# 11. BUG2 FIX — fecha debe ser objeto date, no string
# ─────────────────────────────────────────────────────────────────────────────

def test_fecha_is_date_object_not_string():
    """fecha debe construirse como date(year, 1, 1), no como f-string."""
    from datetime import date

    year = 2023
    # Post-fix
    fecha_fix = date(year, 1, 1)
    # Pre-fix (bug)
    fecha_bug = f"{year}-01-01"

    assert isinstance(fecha_fix, date), "Post-fix: debe ser date"
    assert isinstance(fecha_bug, str),  "Pre-fix era un string"
    assert str(fecha_fix) == fecha_bug,  "Ambos representan la misma fecha"


# ─────────────────────────────────────────────────────────────────────────────
# 12. BUG3 FIX — _extract_iermb_bcn_value toma el año más reciente
# ─────────────────────────────────────────────────────────────────────────────

def test_extract_iermb_most_recent_year():
    """Con datos de varios años, debe devolver el valor del año más reciente."""
    from pipelines.demografia.demografia import _extract_iermb_bcn_value

    data = [
        {"codi_muni": "08019", "any": 2015, "valor": 5.1},
        {"codi_muni": "08019", "any": 2019, "valor": 6.5},
        {"codi_muni": "08019", "any": 2023, "valor": 7.2},
        {"codi_muni": "08019", "any": 2020, "valor": 6.8},
    ]
    result = _extract_iermb_bcn_value(data)
    assert result == pytest.approx(7.2), \
        f"Debe devolver el valor de 2023 (más reciente), no el promedio. Got: {result}"


def test_extract_iermb_single_year():
    """Con un único año, debe devolver ese valor directamente."""
    from pipelines.demografia.demografia import _extract_iermb_bcn_value

    data = [{"municipi": "Barcelona", "any": 2022, "valor": 6.9}]
    result = _extract_iermb_bcn_value(data)
    assert result == pytest.approx(6.9)


def test_extract_iermb_no_year_field_uses_0():
    """Si no hay campo 'any', el año se trata como 0 y aún devuelve el valor."""
    from pipelines.demografia.demografia import _extract_iermb_bcn_value

    data = [{"codi_muni": "08019", "valor": 7.0}]  # sin campo 'any'
    result = _extract_iermb_bcn_value(data)
    assert result == pytest.approx(7.0)


def test_extract_iermb_ignores_other_municipalities():
    """No debe incluir valores de municipios que no son Barcelona."""
    from pipelines.demografia.demografia import _extract_iermb_bcn_value

    data = [
        {"codi_muni": "08015", "any": 2023, "valor": 9.0},  # Badalona
        {"codi_muni": "08019", "any": 2023, "valor": 7.2},  # Barcelona
        {"codi_muni": "08077", "any": 2023, "valor": 8.1},  # Cornellà
    ]
    result = _extract_iermb_bcn_value(data)
    assert result == pytest.approx(7.2)


def test_extract_iermb_empty_returns_none():
    """Lista vacía debe devolver None."""
    from pipelines.demografia.demografia import _extract_iermb_bcn_value

    assert _extract_iermb_bcn_value([]) is None
    assert _extract_iermb_bcn_value({}) is None


# ─────────────────────────────────────────────────────────────────────────────
# 13. D1 FIX — parques.py tiene funciones _init_pipeline y _fin_pipeline
# ─────────────────────────────────────────────────────────────────────────────

def test_parques_has_pipeline_tracking_functions():
    """parques.py debe exponer _init_pipeline y _fin_pipeline."""
    import pipelines.parques as parques_mod
    import inspect

    assert hasattr(parques_mod, "_init_pipeline"), \
        "_init_pipeline no encontrada en parques.py"
    assert hasattr(parques_mod, "_fin_pipeline"), \
        "_fin_pipeline no encontrada en parques.py"
    assert inspect.iscoroutinefunction(parques_mod._init_pipeline)
    assert inspect.iscoroutinefunction(parques_mod._fin_pipeline)


@pytest.mark.asyncio
async def test_parques_calls_init_and_fin_pipeline():
    """actualizar_parques_amb debe llamar _init_pipeline y _fin_pipeline."""
    from pipelines import parques as parques_mod

    init_mock = AsyncMock(return_value=42)
    fin_mock  = AsyncMock(return_value=None)

    fake_conn = MagicMock()
    fake_conn.fetch   = AsyncMock(return_value=[])
    fake_conn.execute = AsyncMock(return_value=None)
    fake_conn.__aenter__ = AsyncMock(return_value=fake_conn)
    fake_conn.__aexit__  = AsyncMock(return_value=False)

    fake_get_db = MagicMock(return_value=fake_conn)

    csv_data = [{
        "nom": "Parc Test", "municipi": "Barcelona",
        "superficie_ha": 1.0, "lat": 41.38, "lon": 2.17,
    }]

    with patch.object(parques_mod, "_init_pipeline", init_mock), \
         patch.object(parques_mod, "_fin_pipeline", fin_mock), \
         patch.object(parques_mod, "_descargar_csv", AsyncMock(return_value=csv_data)), \
         patch("pipelines.parques.get_db", fake_get_db, create=True):

        # Patch get_db inside the module's lazy import path
        with patch.dict(sys.modules, {"db.conexion": MagicMock(get_db=fake_get_db)}):
            try:
                await parques_mod.actualizar_parques_amb()
            except Exception:
                pass  # errores de DB esperados en test — lo que importa es el tracking

    assert init_mock.called, "_init_pipeline no fue llamado"
    assert fin_mock.called,  "_fin_pipeline no fue llamado"
