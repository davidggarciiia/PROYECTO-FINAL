"""
api/financiero.py — POST /api/financiero

Calculadora financiera 100% automática con proyección a 36 meses.

CAMBIO RESPECTO A LA VERSIÓN ANTERIOR:
  Antes: el frontend enviaba TODOS los parámetros (ticket, clientes, reforma...)
         → el usuario los tenía que rellenar manualmente.
  Ahora: el endpoint los estima automáticamente desde los datos de la BD.
         El usuario puede ajustar cualquier parámetro con un slider (override).
         Si no manda ningún override, la calculadora funciona sin intervención.

Flujo:
  1. Leer parámetros pre-calculados de `v_parametros_financieros_actuales`
     (vista sobre `parametros_financieros_zona`, que actualiza el pipeline semanal).
  2. Si no hay datos pre-calculados, calcular en tiempo real con `estimador.py`.
  3. Aplicar overrides del usuario sobre los valores estimados.
  4. Calcular proyección 36 meses con `financiero/calculadora.py` (Python puro).
  5. Guardar análisis en `analisis_financieros` para exportación PDF.
  6. Devolver resultado con cada parámetro documentado (fuente + confianza + rango).

Debounce:
  El frontend debe aplicar debounce de 300ms antes de llamar a este endpoint.
  Cada movimiento de slider lanza una nueva petición con los overrides actualizados.

Fuentes de datos (ver financiero/estimador.py para el detalle completo):
  alquiler_mensual     → `locales.alquiler_mensual` (Idealista) /
                         `precios_alquiler_zona.precio_m2 × m2` (Open Data BCN)
  ticket_medio         → mediana `negocios_activos.precio_nivel` (Google Places)
  clientes_dia         → `variables_zona.flujo_peatonal_*` (aforadors BCN) ×
                         conversion_rate × 1/(num_competidores+1)
  salarios_mensual     → `ceil(m2/empleados_por_m2)` × convenio × 1.31 (SS empresa)
  coste_mercancia_pct  → 1 - `benchmarks_sector.margen_bruto_tipico` (INE CNAE)
  otros_fijos_mensual  → m2 × suministros/m2 + seguro_rc + gestoría + mantenimiento
  reforma_local        → m2 × media(reforma_m2_min/max benchmarks)
  equipamiento         → media(equipamiento_base_min/max benchmarks)
  deposito_fianza      → alquiler × 2 (Art. 36 LAU)
  otros_iniciales      → licencias (legal.py) + constitución + gestoría apertura
"""

from __future__ import annotations

import logging
from typing import Optional

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel, Field

from schemas.models import ProyeccionMes
from financiero.estimador import (
    estimar_parametros, ParametrosEstimados, PE,
)
from financiero.calculadora import calcular_proyeccion
from db.sesiones import get_sesion
from db.financiero import (
    get_parametros_precalculados,
    guardar_analisis_financiero,
    get_benchmarks_sector,
)

logger = logging.getLogger(__name__)
router = APIRouter(tags=["financiero"])

_UMBRAL_ALQUILER_VENTAS = 0.15

# Campos permitidos en overrides con sus rangos válidos [min, max]
_OVERRIDES_PERMITIDOS: dict[str, tuple[float, float]] = {
    "ticket_medio":              (0.01, 10_000.0),
    "clientes_dia_conservador":  (0.0, 10_000.0),
    "clientes_dia_optimista":    (0.0, 10_000.0),
    "dias_apertura_mes":         (1.0, 31.0),
    "alquiler_mensual":          (0.0, 100_000.0),
    "salarios_mensual":          (0.0, 200_000.0),
    "otros_fijos_mensual":       (0.0, 50_000.0),
    "coste_mercancia_pct":       (0.0, 0.95),
    "reforma_local":             (0.0, 2_000_000.0),
    "equipamiento":              (0.0, 2_000_000.0),
    "deposito_fianza":           (0.0, 200_000.0),
    "otros_iniciales":           (0.0, 200_000.0),
}
_MAX_OVERRIDES = 12


# ─── Request ──────────────────────────────────────────────────────────────────

class FinancieroRequest(BaseModel):
    """
    El único campo obligatorio además de zona_id y session_id son los `overrides`:
    un dict con los valores que el usuario ha ajustado con sliders.

    Si `overrides` está vacío ({}), todo se estima automáticamente.
    El frontend nunca necesita pre-rellenar nada.

    Claves válidas para `overrides`:
      ticket_medio              (€)
      clientes_dia_conservador  (personas/día)
      clientes_dia_optimista    (personas/día)
      dias_apertura_mes         (días)
      alquiler_mensual          (€/mes)
      salarios_mensual          (€/mes)
      otros_fijos_mensual       (€/mes)
      coste_mercancia_pct       (0.00–0.95)
      reforma_local             (€)
      equipamiento              (€)
      deposito_fianza           (€)
      otros_iniciales           (€)
    """
    zona_id:    str
    session_id: str
    overrides:  dict[str, float] = Field(
        default_factory=dict,
        description=(
            "Valores ajustados por el usuario con sliders. "
            "Solo incluir los campos que el usuario ha modificado. "
            "Si está vacío, todo se estima automáticamente."
        ),
    )


# ─── Response ─────────────────────────────────────────────────────────────────

class ParametroResponse(BaseModel):
    """
    Un parámetro financiero con su valor final y metadatos de origen.

    El frontend usa estos campos para renderizar:
      - valor_usado    → valor del slider
      - valor_estimado → mostrar como tooltip 'Valor automático: X'
      - fuente         → badge de origen (ej: 'Google Places API')
      - confianza      → color del badge (verde/amarillo/naranja)
      - rango_min/max  → límites del slider
      - es_override    → si True, mostrar botón 'Auto' para resetear
    """
    valor_estimado: float = Field(..., description="Valor calculado automáticamente")
    valor_usado:    float = Field(..., description="Valor final (= override si el usuario ajustó)")
    es_override:    bool  = Field(..., description="True si el usuario ha movido el slider")
    fuente:         str   = Field(..., description="Descripción de la fuente del dato")
    confianza:      str   = Field(..., description="'alta' | 'media' | 'baja'")
    rango_min:      float = Field(..., description="Límite inferior del slider")
    rango_max:      float = Field(..., description="Límite superior del slider")


class ParametrosResponse(BaseModel):
    """Todos los parámetros estimados con su documentación completa."""
    ticket_medio:               ParametroResponse
    clientes_dia_conservador:   ParametroResponse
    clientes_dia_optimista:     ParametroResponse
    dias_apertura_mes:          ParametroResponse
    alquiler_mensual:           ParametroResponse
    num_empleados:              int
    salarios_mensual:           ParametroResponse
    otros_fijos_mensual:        ParametroResponse
    coste_mercancia_pct:        ParametroResponse
    reforma_local:              ParametroResponse
    equipamiento:               ParametroResponse
    deposito_fianza:            ParametroResponse
    otros_iniciales:            ParametroResponse


class DesgloseInversionResponse(BaseModel):
    reforma_local:   float
    equipamiento:    float
    deposito_fianza: float
    otros_iniciales: float


class FinancieroResponse(BaseModel):
    # ── Parámetros documentados ────────────────────────────────────────────────
    # La parte nueva: cada parámetro viene con su fuente y confianza.
    # El frontend los usa para construir los sliders sin input previo del usuario.
    parametros: ParametrosResponse

    # ── Inversión inicial ──────────────────────────────────────────────────────
    inversion_total:    float
    desglose_inversion: DesgloseInversionResponse

    # ── KPIs anuales ──────────────────────────────────────────────────────────
    ingresos_anuales_conservador: float
    ingresos_anuales_optimista:   float
    margen_bruto_pct: float

    ebitda_anual_conservador: float
    ebitda_anual_optimista:   float

    # ── Rentabilidad ──────────────────────────────────────────────────────────
    roi_3a_conservador: float
    roi_3a_optimista:   float
    payback_meses_conservador: int
    payback_meses_optimista:   int
    breakeven_clientes_dia: int = Field(
        ...,
        description="Clientes/día mínimos para cubrir todos los costes fijos y variables",
    )

    # ── Proyección mensual 36 meses ────────────────────────────────────────────
    proyeccion: list[ProyeccionMes]

    # ── Benchmarks sectoriales ─────────────────────────────────────────────────
    margen_sector_tipico: float = Field(
        ..., description="Margen bruto típico del sector (INE CNAE) para comparar"
    )

    # ── Alertas ───────────────────────────────────────────────────────────────
    alquiler_sobre_ventas_pct: float
    alerta_alquiler: bool = Field(
        ...,
        description=f"True si alquiler/ventas > {_UMBRAL_ALQUILER_VENTAS:.0%} (regla del 15%)",
    )


# ─── Endpoint ────────────────────────────────────────────────────────────────

@router.post(
    "/financiero",
    response_model=FinancieroResponse,
    summary="Análisis financiero 100% automático con proyección a 36 meses",
)
async def financiero(body: FinancieroRequest) -> FinancieroResponse:
    """
    El análisis se genera sin input manual del usuario.

    Flujo típico del frontend:
      1. Usuario pulsa 'Análisis financiero' en el panel de detalle de zona.
      2. Frontend llama POST /api/financiero con overrides={}.
      3. Se muestran los sliders con los valores auto-estimados.
      4. Usuario puede ajustar → frontend relama con overrides={campo: nuevo_valor}.
      5. Usuario pulsa 'Auto' en un slider → frontend relama sin ese override.
    """
    # ── Validar overrides ────────────────────────────────────────────────────
    if len(body.overrides) > _MAX_OVERRIDES:
        raise HTTPException(status_code=400, detail=f"Demasiados overrides (máx {_MAX_OVERRIDES}).")
    for campo, valor in body.overrides.items():
        if campo not in _OVERRIDES_PERMITIDOS:
            raise HTTPException(status_code=400, detail=f"Override no permitido: {campo!r}.")
        vmin, vmax = _OVERRIDES_PERMITIDOS[campo]
        if not (vmin <= valor <= vmax):
            raise HTTPException(
                status_code=400,
                detail=f"Override {campo!r} fuera de rango [{vmin}, {vmax}].",
            )

    # ── Validar sesión ────────────────────────────────────────────────────────
    sesion = await get_sesion(body.session_id)
    if sesion is None:
        raise HTTPException(status_code=404, detail="Sesión no encontrada o expirada.")

    sector = sesion.get("perfil", {}).get("sector", "desconocido")
    perfil = sesion.get("perfil", {})

    # ── Obtener parámetros estimados ──────────────────────────────────────────
    estimados = await _get_o_calcular_estimados(
        zona_id=body.zona_id,
        sector=sector,
        perfil=perfil,
    )

    # ── Aplicar overrides ─────────────────────────────────────────────────────
    params = _aplicar_overrides(estimados, body.overrides)

    # ── Extraer valores para la calculadora ───────────────────────────────────
    # Accedemos por nombre de campo para claridad — la calculadora necesita floats.
    v = {
        "ticket_medio":               params.ticket_medio.valor_usado,
        "clientes_dia_conservador":   params.clientes_dia_conservador.valor_usado,
        "clientes_dia_optimista":     params.clientes_dia_optimista.valor_usado,
        "dias_apertura_mes":          params.dias_apertura_mes.valor_usado,
        "alquiler_mensual":           params.alquiler_mensual.valor_usado,
        "salarios_mensual":           params.salarios_mensual.valor_usado,
        "otros_fijos_mensual":        params.otros_fijos_mensual.valor_usado,
        "coste_mercancia_pct":        params.coste_mercancia_pct.valor_usado,
        "reforma_local":              params.reforma_local.valor_usado,
        "equipamiento":               params.equipamiento.valor_usado,
        "deposito_fianza":            params.deposito_fianza.valor_usado,
        "otros_iniciales":            params.otros_iniciales.valor_usado,
    }

    # ── Benchmarks sectoriales ─────────────────────────────────────────────────
    benchmarks = await get_benchmarks_sector(sector)
    margen_sector = benchmarks.get("margen_bruto_tipico", 0.65)

    # ── Calcular proyección 36 meses ──────────────────────────────────────────
    # financiero/calculadora.py — Python puro, sin llamadas externas.
    # La curva de rampa de arranque se aplica internamente (meses 1-12 ingresos reducidos).
    try:
        resultado = await calcular_proyeccion({**v, "margen_sector_tipico": margen_sector})
    except Exception as exc:
        logger.error("Error calculadora financiera zona=%s: %s", body.zona_id, exc, exc_info=True)
        raise HTTPException(status_code=500, detail="Error calculando la proyección financiera.")

    # ── Alerta alquiler/ventas (regla del 15%) ────────────────────────────────
    # Se calcula sobre ingresos en régimen estable (mes 13+, sin rampa de arranque).
    ingresos_mes_estable = v["ticket_medio"] * v["clientes_dia_conservador"] * v["dias_apertura_mes"]
    ingresos_fallback = v["ticket_medio"] * v.get("clientes_dia_optimista", 0) * v["dias_apertura_mes"]
    ingresos_referencia = ingresos_mes_estable or ingresos_fallback or 1.0
    alquiler_sobre_ventas = v["alquiler_mensual"] / ingresos_referencia

    # ── Guardar en BD ────────────────────────────────────────────────────────
    # Tabla `analisis_financieros` — para el PDF y para analytics.
    # No bloqueamos si falla.
    try:
        await guardar_analisis_financiero(
            session_id=body.session_id,
            zona_id=body.zona_id,
            params={**v, "overrides": body.overrides, "sector": sector},
            resultado=resultado,
        )
    except Exception as exc:
        logger.warning("No se pudo guardar analisis_financiero: %s", exc)

    return FinancieroResponse(
        parametros=params,
        inversion_total=resultado["inversion_total"],
        desglose_inversion=DesgloseInversionResponse(
            reforma_local=v["reforma_local"],
            equipamiento=v["equipamiento"],
            deposito_fianza=v["deposito_fianza"],
            otros_iniciales=v["otros_iniciales"],
        ),
        ingresos_anuales_conservador=resultado["ingresos_anuales_conservador"],
        ingresos_anuales_optimista=resultado["ingresos_anuales_optimista"],
        margen_bruto_pct=round(1 - v["coste_mercancia_pct"], 2),
        ebitda_anual_conservador=resultado["ebitda_anual_conservador"],
        ebitda_anual_optimista=resultado["ebitda_anual_optimista"],
        roi_3a_conservador=round(resultado["roi_3a_conservador"], 2),
        roi_3a_optimista=round(resultado["roi_3a_optimista"], 2),
        payback_meses_conservador=resultado["payback_meses_conservador"],
        payback_meses_optimista=resultado["payback_meses_optimista"],
        breakeven_clientes_dia=resultado["breakeven_clientes_dia"],
        proyeccion=[ProyeccionMes(**m) for m in resultado["proyeccion"]],
        margen_sector_tipico=margen_sector,
        alquiler_sobre_ventas_pct=round(alquiler_sobre_ventas, 3),
        alerta_alquiler=alquiler_sobre_ventas > _UMBRAL_ALQUILER_VENTAS,
    )


# ─── Helpers privados ─────────────────────────────────────────────────────────

async def _get_o_calcular_estimados(
    zona_id: str,
    sector: str,
    perfil: dict,
) -> ParametrosEstimados:
    """
    Lee de `v_parametros_financieros_actuales` (pre-calculado por pipeline semanal).
    Si no hay datos, calcula en tiempo real con `estimador.py`.

    El pipeline semanal (`pipelines/parametros_financieros.py`) corre cada domingo
    e invalida esta caché actualizando `parametros_financieros_zona`.
    """
    precalc = await get_parametros_precalculados(zona_id=zona_id, sector=sector)

    if precalc:
        return _row_to_estimados(precalc)

    logger.info(
        "Calculando parámetros en tiempo real (sin pre-calc) zona=%s sector=%s",
        zona_id, sector,
    )
    return await estimar_parametros(zona_id=zona_id, sector=sector, perfil=perfil)


def _row_to_estimados(p: dict) -> ParametrosEstimados:
    """Convierte una fila de `parametros_financieros_zona` a ParametrosEstimados."""

    def pe(valor, fuente, confianza, rmin, rmax) -> PE:
        return PE(valor=valor, fuente=fuente, confianza=confianza,
                  rango_min=rmin, rango_max=rmax)

    slider_clientes_max = max(100.0, p["clientes_dia_optimista"] * 1.6)

    return ParametrosEstimados(
        ticket_medio=pe(
            p["ticket_medio"], p["ticket_fuente"], p["ticket_confianza"],
            p["ticket_rango_min"], p["ticket_rango_max"],
        ),
        clientes_dia_conservador=pe(
            p["clientes_dia_conservador"], p["clientes_fuente"], p["clientes_confianza"],
            1.0, slider_clientes_max,
        ),
        clientes_dia_optimista=pe(
            p["clientes_dia_optimista"], p["clientes_fuente"], p["clientes_confianza"],
            1.0, slider_clientes_max,
        ),
        dias_apertura_mes=pe(
            p["dias_apertura_mes"],
            "Mediana horario competidores zona (Google Places) / benchmarks sector",
            "alta", 20, 31,
        ),
        alquiler_mensual=pe(
            p["alquiler_mensual"], p["alquiler_fuente"], p["alquiler_confianza"],
            round(p["alquiler_mensual"] * 0.75, 0),
            round(p["alquiler_mensual"] * 1.40, 0),
        ),
        salarios_mensual=pe(
            p["salarios_mensual"], p["salarios_fuente"], "media",
            round(p["salarios_mensual"] * 0.50, 0),
            round(p["salarios_mensual"] * 1.80, 0),
        ),
        otros_fijos_mensual=pe(
            p["otros_fijos_mensual"],
            "Suministros + seguro RC + gestoría + mantenimiento",
            "media",
            round(p["otros_fijos_mensual"] * 0.70, 0),
            round(p["otros_fijos_mensual"] * 1.60, 0),
        ),
        coste_mercancia_pct=pe(
            p["coste_mercancia_pct"],
            "INE CNAE — margen bruto típico del sector",
            "alta",
            max(0.0, p["coste_mercancia_pct"] - 0.10),
            min(0.95, p["coste_mercancia_pct"] + 0.10),
        ),
        reforma_local=pe(
            p["reforma_estimada"],
            "m² × €/m² benchmarks sectoriales",
            "baja",
            p["reforma_rango_min"], p["reforma_rango_max"],
        ),
        equipamiento=pe(
            p["equipamiento_estimado"],
            "Benchmarks sectoriales (rango mín–máx)",
            "baja",
            p["equipamiento_rango_min"], p["equipamiento_rango_max"],
        ),
        deposito_fianza=pe(
            p["deposito_fianza"],
            "Art. 36 LAU — estándar España: 2 meses de alquiler",
            "alta",
            p["alquiler_mensual"],
            p["alquiler_mensual"] * 3,
        ),
        otros_iniciales=pe(
            p["otros_iniciales"],
            "Licencias apertura + constitución SL + gestoría apertura",
            "media",
            round(p["otros_iniciales"] * 0.80, 0),
            round(p["otros_iniciales"] * 1.40, 0),
        ),
        num_empleados=p["num_empleados"],
    )


def _aplicar_overrides(
    estimados: ParametrosEstimados,
    overrides: dict[str, float],
) -> ParametrosResponse:
    """
    Combina los parámetros estimados con los overrides del usuario.

    Para cada parámetro:
      - Si el usuario ha movido el slider → valor_usado = override, es_override = True
      - Si no → valor_usado = valor_estimado, es_override = False

    El valor_estimado siempre se conserva — lo necesita el botón "Auto" del frontend.
    """
    def _build(nombre: str, estimado: PE) -> ParametroResponse:
        if nombre in overrides:
            return ParametroResponse(
                valor_estimado=estimado.valor,
                valor_usado=overrides[nombre],
                es_override=True,
                fuente=estimado.fuente,
                confianza=estimado.confianza,
                rango_min=estimado.rango_min,
                rango_max=estimado.rango_max,
            )
        return ParametroResponse(
            valor_estimado=estimado.valor,
            valor_usado=estimado.valor,
            es_override=False,
            fuente=estimado.fuente,
            confianza=estimado.confianza,
            rango_min=estimado.rango_min,
            rango_max=estimado.rango_max,
        )

    return ParametrosResponse(
        ticket_medio=             _build("ticket_medio",             estimados.ticket_medio),
        clientes_dia_conservador= _build("clientes_dia_conservador", estimados.clientes_dia_conservador),
        clientes_dia_optimista=   _build("clientes_dia_optimista",   estimados.clientes_dia_optimista),
        dias_apertura_mes=        _build("dias_apertura_mes",        estimados.dias_apertura_mes),
        alquiler_mensual=         _build("alquiler_mensual",         estimados.alquiler_mensual),
        num_empleados=            estimados.num_empleados,
        salarios_mensual=         _build("salarios_mensual",         estimados.salarios_mensual),
        otros_fijos_mensual=      _build("otros_fijos_mensual",      estimados.otros_fijos_mensual),
        coste_mercancia_pct=      _build("coste_mercancia_pct",      estimados.coste_mercancia_pct),
        reforma_local=            _build("reforma_local",            estimados.reforma_local),
        equipamiento=             _build("equipamiento",             estimados.equipamiento),
        deposito_fianza=          _build("deposito_fianza",          estimados.deposito_fianza),
        otros_iniciales=          _build("otros_iniciales",          estimados.otros_iniciales),
    )
