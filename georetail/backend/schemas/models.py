"""schemas/models.py — Contratos Pydantic centralizados para toda la API."""
from __future__ import annotations
from typing import Optional, Any
from pydantic import BaseModel, Field


# ── Zona ──────────────────────────────────────────────────────────────────────

class ZonaPreview(BaseModel):
    zona_id: str
    nombre: str
    barrio: str
    distrito: str
    lat: float
    lng: float
    direccion: Optional[str] = None
    m2: Optional[float] = None
    alquiler_mensual: Optional[float] = None
    score_global: Optional[float] = None


class CompetidorCercano(BaseModel):
    nombre: str
    sector: Optional[str] = None
    distancia_m: Optional[float] = None
    rating: Optional[float] = None
    es_competencia_directa: bool = False


class AlertaZona(BaseModel):
    tipo: str   # 'positiva' | 'negativa' | 'neutral'
    texto: str
    fuente: str


class AnalisisIA(BaseModel):
    resumen: str
    puntos_fuertes: list[str] = []
    puntos_debiles: list[str] = []
    oportunidad: str = ""
    riesgos: str = ""
    recomendacion_final: str = "Con reservas"
    razon_recomendacion: str = ""


class ScoresDimensiones(BaseModel):
    flujo_peatonal:    Optional[float] = None
    demografia:        Optional[float] = None
    competencia:       Optional[float] = None
    precio_alquiler:   Optional[float] = None
    transporte:        Optional[float] = None
    seguridad:         Optional[float] = None
    turismo:           Optional[float] = None
    entorno_comercial: Optional[float] = None


class ZonaDetalle(BaseModel):
    zona_id: str
    nombre: str
    barrio: str
    distrito: str
    lat: float
    lng: float
    direccion: Optional[str] = None
    m2: Optional[float] = None
    alquiler_mensual: Optional[float] = None
    disponible: bool = True
    escaparate_ml: Optional[float] = None

    # Scores
    score_global: Optional[float] = None
    scores_dimensiones: Optional[ScoresDimensiones] = None
    probabilidad_supervivencia: Optional[float] = None
    shap_values: Optional[dict] = None
    modelo_version: Optional[str] = None

    # Contexto
    flujo_peatonal_dia: Optional[dict] = None
    renta_media_hogar: Optional[float] = None
    edad_media: Optional[float] = None
    pct_extranjeros: Optional[float] = None
    score_turismo: Optional[float] = None
    num_negocios_activos: Optional[int] = None
    pct_locales_vacios: Optional[float] = None
    num_lineas_transporte: Optional[int] = None
    num_paradas_transporte: Optional[int] = None

    # Enriquecimiento
    competidores_cercanos: list[CompetidorCercano] = []
    alertas: list[AlertaZona] = []
    analisis_ia: Optional[AnalisisIA] = None


# ── Búsqueda ──────────────────────────────────────────────────────────────────

class BuscarRequest(BaseModel):
    descripcion: str = ""
    session_id: Optional[str] = None
    filtros: dict = Field(default_factory=dict)

class BuscarResponse(BaseModel):
    estado: str  # 'ok' | 'cuestionario' | 'scoring' | 'error_tipo_negocio' | 'inviable_legal'
    session_id: str
    # Si estado=ok o scoring:
    zonas: list[ZonaPreview] = []
    total: int = 0
    task_id: Optional[str] = None  # Celery task para polling
    # Si estado=cuestionario:
    pregunta: Optional[str] = None
    progreso_pct: int = 0
    # Si estado=error_tipo_negocio o inviable_legal:
    mensaje: Optional[str] = None


class CuestionarioRequest(BaseModel):
    session_id: str
    respuesta: str

class CuestionarioResponse(BaseModel):
    estado: str  # 'continua' | 'completo'
    pregunta: Optional[str] = None
    progreso_pct: int = 0
    trigger_busqueda: bool = False


# ── Local / Zona ──────────────────────────────────────────────────────────────

class LocalPreviewRequest(BaseModel):
    zona_id: str
    session_id: str

class LocalPreviewResponse(BaseModel):
    zona_id: str
    nombre: str
    calle: Optional[str] = None
    m2: Optional[float] = None
    alquiler_mensual: Optional[float] = None
    score_global: Optional[float] = None
    color: str = "amarillo"  # 'verde' | 'amarillo' | 'rojo'

class LocalDetalleRequest(BaseModel):
    zona_id: str
    session_id: str

class LocalDetalleResponse(BaseModel):
    zona: ZonaDetalle
    financiero_preview: Optional[dict] = None  # estimación rápida sin slider


# ── Financiero ────────────────────────────────────────────────────────────────

class ParametroResponse(BaseModel):
    valor_estimado: float
    valor_usado: float
    es_override: bool
    fuente: str
    confianza: str  # 'alta' | 'media' | 'baja'
    rango_min: float
    rango_max: float

class ParametrosResponse(BaseModel):
    ticket_medio: ParametroResponse
    clientes_dia_conservador: ParametroResponse
    clientes_dia_optimista: ParametroResponse
    dias_apertura_mes: ParametroResponse
    alquiler_mensual: ParametroResponse
    num_empleados: int
    salarios_mensual: ParametroResponse
    otros_fijos_mensual: ParametroResponse
    coste_mercancia_pct: ParametroResponse
    reforma_local: ParametroResponse
    equipamiento: ParametroResponse
    deposito_fianza: ParametroResponse
    otros_iniciales: ParametroResponse

class FinancieroRequest(BaseModel):
    zona_id: str
    session_id: str
    overrides: dict[str, float] = Field(default_factory=dict)

class ProyeccionMes(BaseModel):
    mes: int
    ingresos_conservador: float
    ingresos_optimista: float
    costes_variables: float
    costes_fijos: float
    ebitda_conservador: float
    ebitda_optimista: float
    acumulado_conservador: float
    acumulado_optimista: float
    ramp_factor: float

class FinancieroResponse(BaseModel):
    parametros: ParametrosResponse
    inversion_total: float
    desglose_inversion: dict
    ingresos_anuales_conservador: float
    ingresos_anuales_optimista: float
    margen_bruto_pct: float
    ebitda_anual_conservador: float
    ebitda_anual_optimista: float
    roi_3a_conservador: float
    roi_3a_optimista: float
    payback_meses_conservador: int
    payback_meses_optimista: int
    breakeven_clientes_dia: int
    proyeccion: list[ProyeccionMes]
    margen_sector_tipico: float
    alquiler_sobre_ventas_pct: float
    alerta_alquiler: bool


# ── Locales lista ─────────────────────────────────────────────────────────────

class LocalesResponse(BaseModel):
    total: int
    pagina: int
    por_pagina: int
    zonas: list[ZonaPreview]
    filtros_disponibles: dict


# ── Refinamiento ──────────────────────────────────────────────────────────────

class RefinamientoRequest(BaseModel):
    session_id: str
    texto: str

class RefinamientoResponse(BaseModel):
    zonas: list[ZonaPreview]
    total: int
    mensaje_confirmacion: str


# ── Legal ─────────────────────────────────────────────────────────────────────

class RequisitoLegal(BaseModel):
    nombre: str
    descripcion: str
    obligatorio: bool = True

class LegalResponse(BaseModel):
    sector_codigo: str
    regimen: str
    descripcion_regimen: str
    requisitos: list[RequisitoLegal]
    restricciones_uso: str = ""
    coste_estimado_licencias: float
    tiempo_tramitacion_dias: int
    advertencias_especiales: list[str] = []


# ── Exportar ──────────────────────────────────────────────────────────────────

class ExportarRequest(BaseModel):
    session_id: str
    zona_ids: list[str]
    nombre_empresa: Optional[str] = None
    incluir_financiero: bool = True
    incluir_competencia: bool = True

class ExportarResponse(BaseModel):
    pdf_id: str
    estado: str  # 'generando' | 'ok' | 'error'
    url_descarga: Optional[str] = None
    expires_at: Optional[str] = None


# ── Health ────────────────────────────────────────────────────────────────────

class ServiceStatus(BaseModel):
    status: str  # 'ok' | 'degraded' | 'down'
    latencia_ms: Optional[int] = None
    detalle: Optional[str] = None

class HealthResponse(BaseModel):
    status: str
    version: str
    postgresql: ServiceStatus
    redis: ServiceStatus
    celery: ServiceStatus
    llm_proveedor_activo: Optional[str] = None
    modelo_scoring: Optional[str] = None
