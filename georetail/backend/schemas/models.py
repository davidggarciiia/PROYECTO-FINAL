"""schemas/models.py — Contratos Pydantic centralizados para toda la API."""
from __future__ import annotations
from enum import Enum
from typing import Optional, Any, Literal
from pydantic import BaseModel, Field


# ── Enums ─────────────────────────────────────────────────────────────────────

class ColorZona(str, Enum):
    VERDE    = "verde"
    AMARILLO = "amarillo"
    ROJO     = "rojo"


class EstadoBusqueda(str, Enum):
    OK             = "ok"
    CUESTIONARIO   = "cuestionario"
    ERROR_NEGOCIO  = "error_tipo_negocio"
    INVIABLE_LEGAL = "inviable_legal"


class Recomendacion(str, Enum):
    MUY_RECOMENDADO = "Muy recomendado"
    RECOMENDADO     = "Recomendado"
    NEUTRO          = "Neutro"
    NO_RECOMENDADO  = "No recomendado"


class ViabilidadLegal(str, Enum):
    VIABLE      = "viable"
    RESTRINGIDO = "restringido"
    INVIABLE    = "inviable"


class ModeloLegal(str, Enum):
    CLUB_PRIVADO_FUMADORES = "club_privado_fumadores"
    AUTORIZACION_PREVIA    = "autorizacion_previa"


class AccionRefinamiento(str, Enum):
    RESPUESTA       = "respuesta"
    FILTRO_APLICADO = "filtro_aplicado"
    NUEVA_BUSQUEDA  = "nueva_busqueda"


class EstadoServicio(str, Enum):
    OK       = "ok"
    DEGRADED = "degraded"
    DOWN     = "down"


# ── Zona — resumen (resultado de búsqueda) ────────────────────────────────────

class ZonaResumen(BaseModel):
    """Una zona en el listado de resultados del mapa."""
    zona_id:    str
    nombre:     str
    barrio:     str
    distrito:   str
    score_global: float
    probabilidad_supervivencia_3a: Optional[float] = None
    alquiler_estimado:  Optional[float] = None
    m2_disponibles:     Optional[float] = None
    color:      ColorZona
    lat:        float
    lng:        float
    resumen_ia: Optional[str] = None


class ZonaPreview(BaseModel):
    zona_id:  str
    nombre:   str
    barrio:   str
    distrito: str
    lat:      float
    lng:      float
    direccion:        Optional[str]   = None
    m2:               Optional[float] = None
    alquiler_mensual: Optional[float] = None
    score_global:     Optional[float] = None


# ── Zona — detalle completo ───────────────────────────────────────────────────

class CompetidorCercano(BaseModel):
    nombre:                 str
    sector:                 Optional[str]   = None
    subsector:              Optional[str]   = None  # taxonomía fina (scoring/taxonomia.py)
    lat:                    Optional[float] = None  # para visualización en el mini-mapa
    lng:                    Optional[float] = None
    distancia_m:            Optional[float] = None
    rating:                 Optional[float] = None
    precio_nivel:           Optional[int]   = None
    es_competencia_directa: bool            = False
    num_resenas:            Optional[int]   = None
    es_complementario:      bool            = False
    es_vulnerable:          bool            = False
    es_competencia_directa_subsector: bool  = False  # mismo subsector exacto
    amenaza_score:          Optional[float] = None   # 0-100, gravity model individual


class PrecioSegmento(BaseModel):
    nivel_dominante:    Optional[int]   = None   # 1=€ 2=€€ 3=€€€ 4=€€€€
    etiqueta:           str             = "Sin datos"
    distribucion:       dict            = {}     # {1: count, 2: count, ...}
    tiene_gap:          bool            = False
    gap_nivel:          Optional[int]   = None
    gap_etiqueta:       Optional[str]   = None


class CompetenciaDetalle(BaseModel):
    """Respuesta del endpoint GET /api/competencia/{zona_id}."""
    zona_id:                str
    sector:                 str
    radio_m:                int             = 500
    # Scores v2 (de competencia_detalle_zona si existe, si no calculados al vuelo)
    score_competencia:      float           = 50.0
    score_cluster:          float           = 50.0
    amenaza_incumbentes:    float           = 50.0
    oportunidad_mercado:    float           = 50.0
    score_complementarios:  float           = 50.0
    # Métricas
    num_directos:           int             = 0
    pct_vulnerables:        float           = 0.0
    hhi_index:              float           = 0.0
    ratio_complementarios:  float           = 0.0
    # Análisis de precio
    precio_segmento:        Optional[PrecioSegmento] = None
    # Competidores clasificados en 3 grupos
    amenaza:                list[CompetidorCercano] = []
    oportunidad:            list[CompetidorCercano] = []
    sinergicos:             list[CompetidorCercano] = []
    # Metadatos
    fuente:                 str             = "google_places"
    datos_calculados:       bool            = True   # False si viene de BD precalculada


class AlertaZona(BaseModel):
    tipo:   str
    texto:  str
    fuente: str


class AlertaNLP(BaseModel):
    """Alerta generada por el pipeline NLP de reseñas."""
    tipo:   str
    texto:  str
    fuente: str


class AnalisisIA(BaseModel):
    resumen:             str
    puntos_fuertes:      list[str] = []
    puntos_debiles:      list[str] = []
    oportunidad:         str = ""
    riesgos:             str = ""
    recomendacion_final: str = "Con reservas"
    razon_recomendacion: str = ""


class ImpactoModeloDimension(BaseModel):
    contribucion: float = 0.0
    tendencia: str = "neutral"
    top_features: list[dict[str, Any]] = Field(default_factory=list)


class ExplicacionDimension(BaseModel):
    score: Optional[float] = None
    titular: str = ""
    explicacion_corta: str = ""
    porque_sube: list[str] = Field(default_factory=list)
    porque_baja: list[str] = Field(default_factory=list)
    hechos_clave: list[str] = Field(default_factory=list)
    impacto_modelo: str = ""
    confianza: str = "media"
    fuentes: list[str] = Field(default_factory=list)


class AnalisisIADetallado(AnalisisIA):
    resumen_global: str = ""
    explicaciones_dimensiones: dict[str, ExplicacionDimension] = Field(default_factory=dict)
    impacto_modelo_por_dimension: dict[str, ImpactoModeloDimension] = Field(default_factory=dict)


class ScoreDetalle(BaseModel):
    """Scores desglosados por dimensión."""
    flujo_peatonal:    Optional[float] = None
    demografia:        Optional[float] = None
    competencia:       Optional[float] = None
    precio_alquiler:   Optional[float] = None
    transporte:        Optional[float] = None
    seguridad:         Optional[float] = None
    turismo:           Optional[float] = None


class SeguridadDetalle(BaseModel):
    """Desglose granular de la dimensión de seguridad (v7)."""
    incidencias_por_1000hab: Optional[float] = None
    hurtos_por_1000hab:      Optional[float] = None
    robatoris_por_1000hab:   Optional[float] = None
    danys_por_1000hab:       Optional[float] = None
    incidencias_noche_pct:   Optional[float] = None
    comisarias_1km:          Optional[int]   = None
    dist_comisaria_m:        Optional[float] = None
    seguridad_barri_score:   Optional[float] = None


class EntornoComercialDetalle(BaseModel):
    """Desglose granular de la dimensión de entorno comercial (v8)."""
    pct_locales_vacios:         Optional[float] = None
    tasa_rotacion_anual:        Optional[float] = None
    licencias_nuevas_1a:        Optional[float] = None
    ratio_locales_comerciales:  Optional[float] = None
    nivel_ruido_db:             Optional[float] = None
    score_equipamientos:        Optional[float] = None
    m2_zonas_verdes_cercanas:   Optional[float] = None
    mercados_municipales_1km:   Optional[int]   = None
    eventos_culturales_500m:    Optional[float] = None


class ShapExplicacion(BaseModel):
    """Explicación de un factor SHAP del modelo XGBoost."""
    feature:     str
    valor:       float
    descripcion: Optional[str] = None


class FlujoPeatonalDia(BaseModel):
    manana: int = 0
    tarde:  int = 0
    noche:  int = 0


# ── Legal ──────────────────────────────────────────────────────────────────────

class LicenciaNecesaria(BaseModel):
    nombre:         str
    organismo:      str
    coste_estimado: Optional[str] = None
    tiempo_semanas: Optional[str] = None
    notas:          Optional[str] = None


class RestriccionGeografica(BaseModel):
    """Restricción de densidad o distancia en Planes de Usos de Barcelona."""
    tipo:                  str
    descripcion:           str
    radio_metros:          Optional[int] = None
    obligatorio_verificar: bool = False


class RestriccionZona(BaseModel):
    """Restricción legal activa para una zona concreta."""
    tipo:                  str
    descripcion:           str
    radio_metros:          Optional[int] = None
    obligatorio_verificar: bool = False


class InfoLegal(BaseModel):
    """Resumen legal relevante para una zona + sector."""
    viabilidad:           ViabilidadLegal
    alerta:               Optional[str]            = None
    licencias_necesarias: list[LicenciaNecesaria]  = []
    restriccion_zona:     Optional[RestriccionZona] = None
    requisitos_local:     list[str]                = []


# ── LocalDetalle — panel completo de zona ─────────────────────────────────────

class ScoresDimensiones(BaseModel):
    flujo_peatonal:    Optional[float] = None
    demografia:        Optional[float] = None
    competencia:       Optional[float] = None
    precio_alquiler:   Optional[float] = None
    transporte:        Optional[float] = None
    seguridad:         Optional[float] = None
    turismo:           Optional[float] = None
    dinamismo:         Optional[float] = None


# ── PerfilRefinado (capa de preservación de señal sobre la descripción) ───────
# Es una capa estructurada PERO con vocabulario abierto en `nuances_detected`,
# destinada a mantener el matiz del texto del usuario a lo largo de la pipeline.
# Se genera vía LLM en `agente/refinador.py` y se modula con los pesos del
# sector en `scoring/motor._aplicar_pesos_concepto` para hacer explicables los
# cambios. Si el LLM falla, se devuelven defaults vacíos sin bloquear el flujo.

class PublicoObjetivo(BaseModel):
    edad_rango:              Optional[str]                                            = None  # ej "25-35"
    nivel_socioeconomico:    Optional[Literal["bajo", "medio", "medio-alto", "alto"]] = None
    estilo_vida:             list[str]                                                = Field(default_factory=list)
    horarios_pico:           list[str]                                                = Field(default_factory=list)  # ["mañana","brunch","tarde","noche"]


class PropuestaValor(BaseModel):
    especializacion:         Optional[str]   = None
    diferenciadores:         list[str]       = Field(default_factory=list)
    calidad_percibida_0_5:   Optional[float] = None


class Operacion(BaseModel):
    modelo_servicio:         Optional[Literal["take_away", "mesas", "mixto", "delivery_only"]] = None
    ticket_tier_p1_p5:       Optional[int]                                                     = None  # 1=low, 5=premium
    escala_operativa:        Optional[Literal["solo", "micro", "pequeña", "mediana"]]          = None
    horarios_apertura:       list[str]                                                         = Field(default_factory=list)


class UbicacionIdeal(BaseModel):
    tipo_calle:              Optional[str]                                   = None
    densidad_preferida:      Optional[Literal["baja", "media", "alta"]]      = None
    flujo_tipo:              Optional[str]                                   = None


class PerfilRefinado(BaseModel):
    publico_objetivo:            PublicoObjetivo  = Field(default_factory=PublicoObjetivo)
    propuesta_valor:             PropuestaValor   = Field(default_factory=PropuestaValor)
    operacion:                   Operacion        = Field(default_factory=Operacion)
    ubicacion_ideal:             UbicacionIdeal   = Field(default_factory=UbicacionIdeal)
    nuances_detected:            list[str]        = Field(default_factory=list)
    signal_preservation_score:   int              = 100


class ZonaDetalle(BaseModel):
    zona_id:  str
    nombre:   str
    barrio:   str
    distrito: str
    lat:      float
    lng:      float
    direccion:        Optional[str]   = None
    m2:               Optional[float] = None
    alquiler_mensual: Optional[float] = None
    disponible:       bool            = True
    escaparate_ml:    Optional[float] = None

    score_global:               Optional[float]            = None
    scores_dimensiones:         Optional[ScoresDimensiones] = None
    probabilidad_supervivencia: Optional[float]            = None
    shap_values:                Optional[dict]             = None
    modelo_version:             Optional[str]              = None

    flujo_peatonal_dia:     Optional[dict] = None
    renta_media_hogar:      Optional[float] = None
    edad_media:             Optional[float] = None
    pct_extranjeros:        Optional[float] = None
    score_turismo:          Optional[float] = None
    num_negocios_activos:   Optional[int]   = None
    pct_locales_vacios:     Optional[float] = None
    num_lineas_transporte:  Optional[int]   = None
    num_paradas_transporte: Optional[int]   = None

    seguridad_detalle:     Optional[SeguridadDetalle] = None
    entorno_detalle:       Optional[EntornoComercialDetalle] = None

    competidores_cercanos: list[CompetidorCercano] = []
    alertas:               list[AlertaZona]        = []
    analisis_ia:           Optional[AnalisisIADetallado]    = None
    explicaciones_dimensiones: dict[str, ExplicacionDimension] = Field(default_factory=dict)
    impacto_modelo_por_dimension: dict[str, ImpactoModeloDimension] = Field(default_factory=dict)
    resumen_global_llm: Optional[str] = None

    # Pesos del modelo manual_v2 para el sector clasificado de esta busqueda.
    # Permite al frontend mostrar "esta dimension pesa X% para tu idea".
    sector_codigo:      Optional[str] = None
    pesos_dimensiones:  dict[str, float] = Field(default_factory=dict)

    # Capa de preservación de señal: perfil estructurado rico + pesos modulados
    # por reglas declarativas en función del perfil. Ambos son opcionales y se
    # rellenan cuando el agente refinador ha corrido con éxito.
    perfil_refinado:    Optional[PerfilRefinado] = None
    pesos_modulados:    Optional[dict[str, float]] = None


class LocalDetalle(BaseModel):
    """Datos completos para el panel de detalle de zona."""
    zona_id:    str
    local_id:   Optional[str] = None
    nombre_zona: str
    barrio:     str
    distrito:   str

    # Scoring
    score_global:                   float
    scores_dimension:               ScoreDetalle
    probabilidad_supervivencia_3a:  Optional[float]       = None
    explicaciones_shap:             list[ShapExplicacion] = []

    # Datos físicos
    direccion:        Optional[str]   = None
    m2:               Optional[float] = None
    planta:           Optional[str]   = None
    escaparate_ml:    Optional[float] = None
    alquiler_mensual: Optional[float] = None
    disponible:       Optional[bool]  = None

    # Competidores
    competidores_cercanos: list[CompetidorCercano] = []

    # Alertas NLP
    alertas: list[AlertaNLP] = []

    # Flujo peatonal
    flujo_peatonal_dia: FlujoPeatonalDia = FlujoPeatonalDia()

    # Transporte
    tiempo_transporte_centro_min: Optional[int] = None
    num_lineas_transporte:        Optional[int] = None

    # Análisis IA
    analisis_ia:         str           = ""
    pros:                list[str]     = []
    contras:             list[str]     = []
    recomendacion_final: Recomendacion = Recomendacion.NEUTRO

    # Legal
    info_legal: InfoLegal


# ── Búsqueda ──────────────────────────────────────────────────────────────────

class BuscarRequest(BaseModel):
    descripcion: str = ""
    session_id:  Optional[str] = None
    filtros:     dict = Field(default_factory=dict)


class BuscarResponse(BaseModel):
    estado:       str
    session_id:   str
    zonas:        list[ZonaPreview] = []
    total:        int = 0
    task_id:      Optional[str] = None
    pregunta:     Optional[str] = None
    progreso_pct: int = 0
    mensaje:      Optional[str] = None


class CuestionarioRequest(BaseModel):
    session_id: str
    respuesta:  str


class CuestionarioResponse(BaseModel):
    estado:           str
    pregunta:         Optional[str] = None
    progreso_pct:     int = 0
    trigger_busqueda: bool = False


# ── Local / Zona ──────────────────────────────────────────────────────────────

class LocalPreviewRequest(BaseModel):
    zona_id:    str
    session_id: str


class LocalPreviewResponse(BaseModel):
    zona_id:          str
    nombre:           str
    calle:            Optional[str]   = None
    m2:               Optional[float] = None
    alquiler_mensual: Optional[float] = None
    score_global:     Optional[float] = None
    color:            str = "amarillo"


class LocalDetalleRequest(BaseModel):
    zona_id:    str
    session_id: str


class LocalDetalleResponse(BaseModel):
    zona:               ZonaDetalle
    financiero_preview: Optional[dict] = None
    dev_data:           Optional[dict] = None


# ── Financiero ────────────────────────────────────────────────────────────────

class ParametroResponse(BaseModel):
    valor_estimado: float
    valor_usado:    float
    es_override:    bool
    fuente:         str
    confianza:      str
    rango_min:      float
    rango_max:      float


class ParametrosResponse(BaseModel):
    ticket_medio:             ParametroResponse
    clientes_dia_conservador: ParametroResponse
    clientes_dia_optimista:   ParametroResponse
    dias_apertura_mes:        ParametroResponse
    alquiler_mensual:         ParametroResponse
    num_empleados:            int
    salarios_mensual:         ParametroResponse
    otros_fijos_mensual:      ParametroResponse
    coste_mercancia_pct:      ParametroResponse
    reforma_local:            ParametroResponse
    equipamiento:             ParametroResponse
    deposito_fianza:          ParametroResponse
    otros_iniciales:          ParametroResponse


class FinancieroRequest(BaseModel):
    zona_id:    str
    session_id: str
    overrides:  dict[str, float] = Field(default_factory=dict)


class ProyeccionMes(BaseModel):
    mes:                   int
    ingresos_conservador:  float
    ingresos_optimista:    float
    costes_variables:      float
    costes_fijos:          float
    ebitda_conservador:    float
    ebitda_optimista:      float
    acumulado_conservador: float
    acumulado_optimista:   float
    ramp_factor:           float


class FinancieroResponse(BaseModel):
    parametros:                   ParametrosResponse
    inversion_total:              float
    desglose_inversion:           dict
    ingresos_anuales_conservador: float
    ingresos_anuales_optimista:   float
    margen_bruto_pct:             float
    ebitda_anual_conservador:     float
    ebitda_anual_optimista:       float
    roi_3a_conservador:           float
    roi_3a_optimista:             float
    payback_meses_conservador:    int
    payback_meses_optimista:      int
    breakeven_clientes_dia:       int
    proyeccion:                   list[ProyeccionMes]
    margen_sector_tipico:         float
    alquiler_sobre_ventas_pct:    float
    alerta_alquiler:              bool


# ── Locales lista ─────────────────────────────────────────────────────────────

class LocalListItem(BaseModel):
    zona_id:    str
    nombre:     str
    barrio:     str
    distrito:   str
    score_global: float
    color:        ColorZona
    direccion:                    Optional[str]   = None
    alquiler_mensual:             Optional[float] = None
    m2:                           Optional[float] = None
    disponible:                   Optional[bool]  = None
    probabilidad_supervivencia_3a: float = 0.5
    resumen_ia:                   str   = ""


class FiltrosDisponibles(BaseModel):
    distritos:    list[str] = []
    score_min:    float = 0.0
    score_max:    float = 100.0
    alquiler_min: float = 0.0
    alquiler_max: float = 0.0
    m2_min:       int   = 0
    m2_max:       int   = 0


class LocalesResponse(BaseModel):
    total:               int
    pagina:              int
    por_pagina:          int
    zonas:               list[ZonaPreview]
    filtros_disponibles: dict


# ── Refinamiento ──────────────────────────────────────────────────────────────

class RefinamientoRequest(BaseModel):
    session_id: str
    texto:      str


class RefinamientoResponse(BaseModel):
    zonas:                list[ZonaPreview]
    total:                int
    mensaje_confirmacion: str


# ── Legal ─────────────────────────────────────────────────────────────────────

class RequisitoLegal(BaseModel):
    nombre:      str
    descripcion: str
    obligatorio: bool = True


# ── Exportar ──────────────────────────────────────────────────────────────────

class ExportarRequest(BaseModel):
    session_id:          str
    zona_ids:            list[str]
    nombre_empresa:      Optional[str] = None
    incluir_financiero:  bool = True
    incluir_competencia: bool = True


class ExportarResponse(BaseModel):
    pdf_id:      str
    estado:      str
    url_descarga: Optional[str] = None
    expires_at:  Optional[str] = None


# ── Health ────────────────────────────────────────────────────────────────────

class ServiceStatus(BaseModel):
    name:       str
    status:     str
    latency_ms: Optional[float] = None
    message:    Optional[str] = None


class HealthResponse(BaseModel):
    status:    str
    version:   str
    timestamp: str
    services:  list[ServiceStatus]


# ── Transporte (detalle por zona) ─────────────────────────────────────────────

TipoTransporte = Literal["metro", "bus", "tram", "fgc", "rodalies"]


class ParadaCercana(BaseModel):
    nombre:      str
    distancia_m: float
    tipo:        TipoTransporte


class LineaCercana(BaseModel):
    tipo:              TipoTransporte
    codigo:            str
    nombre:            Optional[str] = None
    color:             str
    dist_min_m:        float
    paradas_cercanas:  list[ParadaCercana]


class TransporteDetalleZona(BaseModel):
    zona_id:       str
    radio_m:       int = 500
    total_lineas:  int
    total_paradas: int
    lineas:        list[LineaCercana]
