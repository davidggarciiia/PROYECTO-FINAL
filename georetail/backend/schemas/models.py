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
    ticket_medio:        ParametroResponse
    clients_per_day:     ParametroResponse  # único slider de clientes (v2)
    max_capacity:        float              # límite físico
    dias_apertura_mes:   ParametroResponse
    alquiler_mensual:    ParametroResponse
    num_empleados:       int
    salarios_mensual:    ParametroResponse
    otros_fijos_mensual: ParametroResponse
    coste_mercancia_pct: ParametroResponse
    reforma_local:       ParametroResponse
    equipamiento:        ParametroResponse
    deposito_fianza:     ParametroResponse
    otros_iniciales:     ParametroResponse


class FinancieroRequest(BaseModel):
    zona_id:    str
    session_id: str
    overrides:  dict[str, float] = Field(default_factory=dict)


class ProyeccionMes(BaseModel):
    mes:                   int
    ingresos_conservador:  float
    ingresos_base:         float = 0.0
    ingresos_optimista:    float
    costes_variables:      float
    costes_fijos:          float
    ebitda_conservador:    float
    ebitda_base:           float = 0.0
    ebitda_optimista:      float
    acumulado_conservador: float
    acumulado_base:        float = 0.0
    acumulado_optimista:   float
    ramp_factor:           float
    # Bandas de volatilidad ±15% para escenario base (v3)
    ingresos_base_low:     int = 0
    ingresos_base_high:    int = 0
    # Escenario de estrés v4 — ingresos ×0.40 con costes fijos intactos
    ingresos_stress:       float = 0.0
    ebitda_stress:         float = 0.0
    acumulado_stress:      float = 0.0


# ── Bloques v2 del panel financiero ───────────────────────────────────────────

class ExplicacionDecision(BaseModel):
    factor_limitante: str              # "alquiler" | "costes" | "demanda" | "payback" | "viable"
    resumen:          str              # frase-decisión en lenguaje humano
    impacto_clave:    str              # subtítulo — el problema concreto en una línea
    razones:          list[str] = []   # bullets de por qué es viable/riesgo/no
    recomendaciones:  list[str] = []   # acciones concretas que puede tomar el usuario


class DecisionBlock(BaseModel):
    recomendacion:     str   # "si" | "riesgo" | "no"
    beneficio_mensual: float
    payback:           int
    capital_necesario: float
    gap_capital:       float
    viability_score:   int                          = 50
    explicacion:       Optional[ExplicacionDecision] = None


class EconomiaBase(BaseModel):
    ingresos_mensuales:      float
    clientes_dia:            float
    ticket_medio:            float
    conversion_pct:          float
    max_potential_customers: float
    ocupacion_efectiva:      float = 0.0


class EstructuraCostes(BaseModel):
    alquiler:         float
    personal:         float
    variable:         float
    otros:            float
    beneficio:        float
    perdida:          float
    ingresos_totales: float


class BreakEvenPunto(BaseModel):
    clientes:       float
    ingresos:       float
    costes_totales: float


class BreakEvenInfo(BaseModel):
    clientes_be:         int
    ingresos_be:         float
    clientes_base:       float
    margen_sobre_be_pct: float
    chart:               list[BreakEvenPunto]


class MetricasClave(BaseModel):
    roi_conservador:   float
    roi_base:          float
    roi_optimista:     float
    roi_stress:        float = -1.0
    margen_bruto_pct:  float
    payback_meses:     int
    payback_stress:    int   = 999
    mes_caja_positiva: int


class SensitividadItem(BaseModel):
    """Impacto en EBITDA año 1 si la variable sube un 10%."""
    variable:       str    # clave interna
    label:          str    # texto legible para el usuario
    impacto_ebitda: float  # positivo = mejora, negativo = empeora


class Riesgo(BaseModel):
    tipo:    str   # "bloqueo" | "warning"
    mensaje: str


class Insight(BaseModel):
    type:       str   # "risk" | "opportunity"
    message:    str
    suggestion: str


class ModeloDemanda(BaseModel):
    flujo_peatonal_dia:      float
    max_potential_customers: float
    capture_rate:            float


class CorreccionAplicada(BaseModel):
    parametro:       str
    valor_original:  float
    valor_corregido: float
    motivo:          str
    capa:            str   = ""   # "gatekeeper" | "pipeline" | "demanda"
    impacto_pct:     float = 0.0  # cambio relativo: (corregido - original) / original


class CapacityModelInfo(BaseModel):
    tipo:                      str
    descripcion:               str
    units:                     int
    sessions_per_unit_per_day: float
    max_clients_day:           float


# ── Validación financiera LLM (v3) ────────────────────────────────────────────

class ProblemaDetectado(BaseModel):
    tipo:        str  # snake_case code del catálogo del prompt
    descripcion: str  # explicación con números reales del input
    impacto:     str  # "alto" | "medio" | "bajo"


class AjusteRecomendado(BaseModel):
    variable:       str
    accion:         str  # "reducir" | "aumentar" | "revisar"
    rango_sugerido: str
    motivo:         str


class ChecksDetallados(BaseModel):
    capacidad: str  # "ok" | "warning" | "error"
    costes:    str
    margenes:  str
    roi:       str
    payback:   str


class ValidacionFinanciera(BaseModel):
    coherencia_global:    str   # "alta" | "media" | "baja"
    veredicto:            str   # "fiable" | "optimista" | "no_creible"
    subsector_usado:      str   = ""  # subsector used for validation (e.g. "tapas_bar")
    problemas_detectados: list[ProblemaDetectado] = Field(default_factory=list)
    ajustes_recomendados: list[AjusteRecomendado] = Field(default_factory=list)
    supuestos_peligrosos: list[str]               = Field(default_factory=list)
    checks_detallados:    ChecksDetallados


class FinancieroResponse(BaseModel):
    parametros:                   ParametrosResponse
    inversion_total:              float
    desglose_inversion:           dict
    ingresos_anuales_conservador: float
    ingresos_anuales_base:        float = 0.0
    ingresos_anuales_optimista:   float
    margen_bruto_pct:             float
    ebitda_anual_conservador:     float
    ebitda_anual_base:            float = 0.0
    ebitda_anual_optimista:       float
    roi_3a_conservador:           float
    roi_3a_base:                  float = 0.0
    roi_3a_optimista:             float
    payback_meses_conservador:    int
    payback_meses_base:           int = 999
    payback_meses_optimista:      int
    breakeven_clientes_dia:       int
    proyeccion:                   list[ProyeccionMes]
    margen_sector_tipico:         float
    alquiler_sobre_ventas_pct:             float
    alquiler_sobre_ventas_pct_conservador: float = 0.0
    alerta_alquiler:                       bool
    # Bloques v2
    decision:          Optional[DecisionBlock]    = None
    economia_base:     Optional[EconomiaBase]     = None
    estructura_costes: Optional[EstructuraCostes] = None
    break_even:        Optional[BreakEvenInfo]    = None
    metricas_clave:    Optional[MetricasClave]    = None
    riesgos:           list[Riesgo]               = Field(default_factory=list)
    insights:          list[Insight]              = Field(default_factory=list)
    modelo_demanda:    Optional[ModeloDemanda]    = None
    # Modelo v3
    business_model_type:    str                              = "retail_walkin"
    correcciones_aplicadas: list[CorreccionAplicada]         = Field(default_factory=list)
    capacity_model:         Optional[CapacityModelInfo]      = None
    tipo_negocio:           str                              = "nuevo"
    validation_flags:       list[str]                        = Field(default_factory=list)
    ocupacion_efectiva:     float                            = 0.0
    validacion_financiera:  Optional[ValidacionFinanciera]   = None
    # Análisis v4: stress + sensibilidad
    ebitda_anual_stress:   float                            = 0.0
    roi_3a_stress:         float                            = -1.0
    payback_meses_stress:  int                              = 999
    sensibilidad:          list[SensitividadItem]           = Field(default_factory=list)


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


# ── Cuestionario estructurado (test alternativo al texto libre) ───────────────
# Contrato del formulario "tipo test". Cuando llega en `BuscarRequest`, el
# endpoint /api/buscar se salta el LLM (validar_negocio + refinar) y construye
# el perfil directamente desde los campos. Solo se invoca refinar() si el
# usuario escribe algo en `matices`.

class FlagsLegales(BaseModel):
    """Flags opcionales que el test envía a /api/legal/roadmap para ajustar
    licencias y requisitos (ej: alcohol → añade llicència de venda de begudes).
    Todos los campos son opcionales; los que no vienen se asumen False."""
    servira_alcohol:          Optional[bool] = None
    con_terraza:              Optional[bool] = None
    cocina_con_humos:         Optional[bool] = None
    aparatologia_sanitaria:   Optional[bool] = None
    tatuajes_invasivos:       Optional[bool] = None
    club_privado_fumadores:   Optional[bool] = None
    emision_musica_alta:      Optional[bool] = None
    venta_menores:            Optional[bool] = None


class OverridesFinancieros(BaseModel):
    """Valores opcionales del test que pre-rellenan /api/financiero overrides.
    El usuario puede afinar los sliders más tarde en el panel de detalle."""
    ticket_medio:       Optional[float] = Field(None, ge=0, le=10_000)
    clientes_dia:       Optional[float] = Field(None, ge=0, le=10_000)
    num_empleados:      Optional[int]   = Field(None, ge=0, le=500)
    m2_objetivo:        Optional[float] = Field(None, ge=0, le=5_000)


class PerfilEstructurado(BaseModel):
    """Payload del cuestionario tipo test. Se mapea internamente a
    `PerfilRefinado` + `sector_detectado` + filtros, sin pasar por LLM (fast
    path). Solo `sector` es obligatorio."""
    sector:           str
    subsector:        Optional[str] = None

    # Mapean 1:1 a PerfilRefinado
    publico_objetivo: PublicoObjetivo = Field(default_factory=PublicoObjetivo)
    operacion:        Operacion       = Field(default_factory=Operacion)
    ubicacion_ideal:  UbicacionIdeal  = Field(default_factory=UbicacionIdeal)

    # Filtros prácticos (mapeable a BuscarRequest.filtros)
    presupuesto_max:  Optional[float]      = Field(None, gt=0, description="Alquiler mensual máximo en €")
    m2_min:           Optional[float]      = Field(None, gt=0)
    m2_max:           Optional[float]      = Field(None, gt=0)
    distritos:        Optional[list[str]]  = None

    # Conexión con /api/legal y /api/financiero
    flags_legales:        Optional[FlagsLegales]        = None
    overrides_financieros: Optional[OverridesFinancieros] = None

    # Texto libre opcional — único campo que dispara refinar() (LLM)
    matices:          Optional[str] = Field(None, max_length=300)


# ── Cuestionario — catálogo de opciones servido al frontend ───────────────────
# El frontend hace fetch a GET /api/cuestionario/opciones al abrir el test para
# poblar los <select> y los chips multiselect desde una sola fuente de verdad
# (la taxonomía del backend, no constantes hardcodeadas).

class SubsectorOpcion(BaseModel):
    codigo: str
    label:  str


class FlagLegalOpcion(BaseModel):
    codigo: str
    label:  str
    descripcion: Optional[str] = None


class OpcionesCuestionarioResponse(BaseModel):
    sectores:                 list[SubsectorOpcion]
    subsectores_por_sector:   dict[str, list[SubsectorOpcion]]
    niveles_socioeconomicos:  list[SubsectorOpcion]
    estilos_vida:             list[SubsectorOpcion]
    horarios_pico:            list[SubsectorOpcion]
    horarios_apertura:        list[SubsectorOpcion]
    modelos_servicio:         list[SubsectorOpcion]
    escalas_operativas:       list[SubsectorOpcion]
    densidades:               list[SubsectorOpcion]
    tipos_calle:              list[SubsectorOpcion]
    tipos_flujo:              list[SubsectorOpcion]
    distritos_bcn:            list[str]
    flags_legales_por_sector: dict[str, list[FlagLegalOpcion]]
