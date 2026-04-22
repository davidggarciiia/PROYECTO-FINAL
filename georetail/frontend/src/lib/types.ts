export type Theme = "dark" | "light";
export type ColorZona = "verde" | "amarillo" | "rojo";

export interface ZonaPreview {
  zona_id: string;
  nombre: string;
  barrio: string;
  distrito: string;
  lat: number;
  lng: number;
  direccion?: string;
  m2?: number;
  alquiler_mensual?: number;
  score_global?: number;
  color?: ColorZona;
}

export interface ZonaResumen {
  zona_id: string;
  nombre: string;
  barrio: string;
  distrito: string;
  score_global: number;
  probabilidad_supervivencia_3a?: number;
  alquiler_estimado?: number;
  m2_disponibles?: number;
  color: ColorZona;
  lat: number;
  lng: number;
  resumen_ia?: string;
}

export interface CompetidorCercano {
  nombre: string;
  sector?: string;
  subsector?: string | null;
  lat?: number;
  lng?: number;
  distancia_m?: number;
  rating?: number;
  precio_nivel?: number;
  es_competencia_directa: boolean;
  es_competencia_directa_subsector?: boolean;
  num_resenas?: number;
  es_complementario?: boolean;
  es_vulnerable?: boolean;
  amenaza_score?: number;
}

export type CompetidorDetalle = CompetidorCercano;

export interface PrecioSegmento {
  nivel_dominante?: number;
  etiqueta: string;
  distribucion: Record<number, number>;
  tiene_gap: boolean;
  gap_nivel?: number;
  gap_etiqueta?: string;
}

export interface CompetenciaDetalle {
  zona_id: string;
  sector: string;
  radio_m?: number;
  score_competencia: number;
  score_cluster: number;
  amenaza_incumbentes: number;
  oportunidad_mercado: number;
  score_complementarios: number;
  num_directos?: number;
  pct_vulnerables?: number;
  hhi_index: number;
  ratio_complementarios?: number;
  precio_segmento?: PrecioSegmento | null;
  amenaza: CompetidorDetalle[];
  oportunidad: CompetidorDetalle[];
  sinergicos: CompetidorDetalle[];
  fuente?: string;
  datos_calculados?: boolean;
}

export interface AlertaZona {
  tipo: string;
  texto: string;
  fuente: string;
}

export interface AnalisisIA {
  resumen: string;
  puntos_fuertes: string[];
  puntos_debiles: string[];
  oportunidad: string;
  riesgos: string;
  recomendacion_final: string;
  razon_recomendacion: string;
}

export interface ImpactoModeloFeature {
  feature?: string;
  valor?: number;
  descripcion?: string;
}

export interface ImpactoModeloDimension {
  contribucion: number;
  tendencia: string;
  top_features: ImpactoModeloFeature[];
}

export interface ExplicacionDimension {
  score?: number | null;
  titular: string;
  explicacion_corta: string;
  porque_sube: string[];
  porque_baja: string[];
  hechos_clave: string[];
  impacto_modelo: string;
  confianza: string;
  fuentes: string[];
}

export interface AnalisisIADetallado extends AnalisisIA {
  resumen_global?: string;
  explicaciones_dimensiones?: Record<string, ExplicacionDimension>;
  impacto_modelo_por_dimension?: Record<string, ImpactoModeloDimension>;
}

export interface ScoresDimensiones {
  flujo_peatonal?: number;
  demografia?: number;
  competencia?: number;
  precio_alquiler?: number;
  transporte?: number;
  seguridad?: number;
  turismo?: number;
  dinamismo?: number;
}

export interface SeguridadDetalle {
  incidencias_por_1000hab?: number;
  hurtos_por_1000hab?: number;
  robatoris_por_1000hab?: number;
  danys_por_1000hab?: number;
  incidencias_noche_pct?: number;
  comisarias_1km?: number;
  dist_comisaria_m?: number;
  seguridad_barri_score?: number;
}

export interface EntornoComercialDetalle {
  pct_locales_vacios?: number;
  tasa_rotacion_anual?: number;
  licencias_nuevas_1a?: number;
  ratio_locales_comerciales?: number;
  nivel_ruido_db?: number;
  score_equipamientos?: number;
  m2_zonas_verdes_cercanas?: number;
  mercados_municipales_1km?: number;
  eventos_culturales_500m?: number;
}

// ── Perfil refinado (Fase 1-2 backend) ───────────────────────────────────
// Estructura rica generada por el agente refinador (LLM) que complementa
// al perfil numerico clasico (8 dims). Se expone al frontend para que el
// usuario vea que ha entendido el motor y pueda corregirlo.
export interface PublicoObjetivo {
  edad_rango?: string | null;
  nivel_socioeconomico?: "bajo" | "medio" | "medio-alto" | "alto" | null;
  estilo_vida: string[];
  horarios_pico: string[];
}

export interface PropuestaValor {
  especializacion?: string | null;
  diferenciadores: string[];
  calidad_percibida_0_5?: number | null;
}

export interface Operacion {
  modelo_servicio?: "take_away" | "mesas" | "mixto" | "delivery_only" | null;
  ticket_tier_p1_p5?: number | null;
  escala_operativa?: "solo" | "micro" | "pequeña" | "mediana" | null;
  horarios_apertura: string[];
}

export interface UbicacionIdeal {
  tipo_calle?: string | null;
  densidad_preferida?: "baja" | "media" | "alta" | null;
  flujo_tipo?: string | null;
}

export interface PerfilRefinado {
  publico_objetivo: PublicoObjetivo;
  propuesta_valor: PropuestaValor;
  operacion: Operacion;
  ubicacion_ideal: UbicacionIdeal;
  nuances_detected: string[];
  signal_preservation_score: number;
}

export interface ZonaDetalle {
  zona_id: string;
  nombre: string;
  barrio: string;
  distrito: string;
  lat: number;
  lng: number;
  direccion?: string;
  m2?: number;
  alquiler_mensual?: number;
  disponible: boolean;
  escaparate_ml?: number;
  score_global?: number;
  scores_dimensiones?: ScoresDimensiones;
  probabilidad_supervivencia?: number;
  shap_values?: Record<string, number> | null;
  modelo_version?: string | null;

  // Pesos del modelo manual_v2 para el sector clasificado de esta busqueda.
  // El frontend los muestra por dimension ("peso 23%") + explica por que.
  sector_codigo?: string;
  subsector_codigo?: string | null;
  pesos_dimensiones?: Record<string, number>;
  // Perfil rico + pesos modulados (Fase 1-2 backend). Pueden venir null
  // si el LLM fallback no genero perfil refinado — el frontend degrada.
  perfil_refinado?: PerfilRefinado | null;
  pesos_modulados?: Record<string, number> | null;
  flujo_peatonal_dia?: { manana: number; tarde: number; noche: number };
  renta_media_hogar?: number;
  edad_media?: number;
  pct_extranjeros?: number;
  score_turismo?: number;
  num_negocios_activos?: number;
  pct_locales_vacios?: number;
  num_lineas_transporte?: number;
  num_paradas_transporte?: number;
  seguridad_detalle?: SeguridadDetalle | null;
  entorno_detalle?: EntornoComercialDetalle | null;
  competidores_cercanos: CompetidorCercano[];
  alertas: AlertaZona[];
  analisis_ia?: AnalisisIADetallado | null;
  explicaciones_dimensiones?: Record<string, ExplicacionDimension>;
  impacto_modelo_por_dimension?: Record<string, ImpactoModeloDimension>;
  resumen_global_llm?: string | null;
}

/** Detalle narrativo de la dimensión Turismo (endpoint lazy, drawer). */
export interface DimensionTurismoDetalle {
  zona_id: string;
  zona_nombre: string;
  radio_m: number;
  score_turismo: number | null;
  resumen: string;
  hechos: {
    landmarks_count: number;
    hoteles_count: number;
    venues_count: number;
    airbnb_density_500m: number | null;
    booking_hoteles_500m: number | null;
    dist_playa_m: number | null;
    dist_landmark_top3_m: number | null;
    score_turismo_hut: number | null;
  };
  landmarks: Array<{
    nombre: string;
    distancia_m: number;
    wikidata_id: string | null;
    peso: number | null;
  }>;
  hoteles: Array<{
    nombre: string;
    tipo: string;
    estrellas: number | null;
    rating: number | null;
    distancia_m: number;
  }>;
  venues: Array<{
    nombre: string;
    tipo: string;
    fuente: string | null;
    distancia_m: number;
  }>;
}

// ── Cuestionario estructurado (alternativa al texto libre) ───────────────
// Contrato espejo de `schemas.models.PerfilEstructurado` del backend.
// Si viene en BuscarRequest, el backend salta el LLM (validar+refinar) y
// construye el perfil directamente desde el form. Solo `matices` pasa por LLM.
export interface FlagsLegales {
  servira_alcohol?: boolean;
  con_terraza?: boolean;
  cocina_con_humos?: boolean;
  aparatologia_sanitaria?: boolean;
  tatuajes_invasivos?: boolean;
  club_privado_fumadores?: boolean;
  emision_musica_alta?: boolean;
  venta_menores?: boolean;
}

export interface OverridesFinancieros {
  ticket_medio?: number;
  clientes_dia?: number;
  num_empleados?: number;
  m2_objetivo?: number;
}

export interface PerfilEstructurado {
  sector: string;
  subsector?: string;
  publico_objetivo?: PublicoObjetivo;
  operacion?: Operacion;
  ubicacion_ideal?: UbicacionIdeal;
  presupuesto_max?: number;
  m2_min?: number;
  m2_max?: number;
  distritos?: string[];
  flags_legales?: FlagsLegales;
  overrides_financieros?: OverridesFinancieros;
  matices?: string;
}

export interface SubsectorOpcion {
  codigo: string;
  label: string;
}

export interface FlagLegalOpcion {
  codigo: string;
  label: string;
  descripcion?: string;
}

export interface OpcionesCuestionarioResponse {
  sectores: SubsectorOpcion[];
  subsectores_por_sector: Record<string, SubsectorOpcion[]>;
  niveles_socioeconomicos: SubsectorOpcion[];
  estilos_vida: SubsectorOpcion[];
  horarios_pico: SubsectorOpcion[];
  horarios_apertura: SubsectorOpcion[];
  modelos_servicio: SubsectorOpcion[];
  escalas_operativas: SubsectorOpcion[];
  densidades: SubsectorOpcion[];
  tipos_calle: SubsectorOpcion[];
  tipos_flujo: SubsectorOpcion[];
  distritos_bcn: string[];
  flags_legales_por_sector: Record<string, FlagLegalOpcion[]>;
}

export interface BuscarRequest {
  descripcion: string;
  session_id?: string;
  filtros?: Record<string, unknown>;
  presupuesto_max?: number;
  m2_min?: number;
  m2_max?: number;
  distritos?: string[];
  perfil_estructurado?: PerfilEstructurado;
}

export interface BuscarResponse {
  estado: "ok" | "cuestionario" | "error_tipo_negocio" | "inviable_legal";
  session_id: string;
  zonas?: ZonaResumen[] | null;
  total_zonas_analizadas?: number;
  pregunta?: string;
  progreso_cuestionario?: number;
  motivo?: string;
}

export interface CuestionarioRequest {
  session_id: string;
  respuesta: string;
}

export interface CuestionarioResponse {
  estado: string;
  pregunta?: string;
  progreso_pct: number;
  trigger_busqueda: boolean;
}

export interface ProyeccionMes {
  mes: number;
  ingresos_conservador: number;
  ingresos_base?: number;
  ingresos_optimista: number;
  costes_variables: number;
  costes_fijos: number;
  ebitda_conservador: number;
  ebitda_base?: number;
  ebitda_optimista: number;
  acumulado_conservador: number;
  acumulado_base?: number;
  acumulado_optimista: number;
  ramp_factor: number;
  ingresos_base_low?: number;
  ingresos_base_high?: number;
}

export interface ParametroResponse {
  valor_estimado: number;
  valor_usado: number;
  es_override: boolean;
  fuente: string;
  confianza: string;
  rango_min: number;
  rango_max: number;
}

// Alias legacy usado por paneles antiguos
export type ParametroFinanciero = ParametroResponse;

// ── Bloques v2 del panel financiero ────────────────────────────────────────
export interface DecisionBlock {
  recomendacion: "si" | "riesgo" | "no";
  beneficio_mensual: number;
  payback: number;
  capital_necesario: number;
  gap_capital: number;
}

export interface EconomiaBase {
  ingresos_mensuales: number;
  clientes_dia: number;
  ticket_medio: number;
  conversion_pct: number;
  max_potential_customers: number;
  ocupacion_efectiva?: number;
}

export interface EstructuraCostes {
  alquiler: number;
  personal: number;
  variable: number;
  otros: number;
  beneficio: number;
  perdida: number;
  ingresos_totales: number;
}

export interface BreakEvenPunto {
  clientes: number;
  ingresos: number;
  costes_totales: number;
}

export interface BreakEvenInfo {
  clientes_be: number;
  ingresos_be: number;
  clientes_base: number;
  margen_sobre_be_pct: number;
  chart: BreakEvenPunto[];
}

export interface MetricasClave {
  roi_conservador: number;
  roi_base: number;
  roi_optimista: number;
  margen_bruto_pct: number;
  payback_meses: number;
  mes_caja_positiva: number;
}

export interface Riesgo {
  tipo: "bloqueo" | "warning";
  mensaje: string;
}

export interface Insight {
  type: "risk" | "opportunity";
  message: string;
  suggestion: string;
}

export interface ModeloDemanda {
  flujo_peatonal_dia: number;
  max_potential_customers: number;
  capture_rate: number;
}

export interface CorreccionAplicada {
  parametro: string;
  valor_original: number;
  valor_corregido: number;
  motivo: string;
}

export interface CapacityModelInfo {
  tipo: string;
  descripcion: string;
  units: number;
  sessions_per_unit_per_day: number;
  max_clients_day: number;
}

export interface BusinessContext {
  tipo: "nuevo" | "traspaso";
  capital_inicial?: number;
  capacidad_operativa?: number;
}

// ── Validación LLM (v3) ────────────────────────────────────────────────────
export interface ProblemaDetectado {
  tipo: string;
  descripcion: string;
  impacto: "alto" | "medio" | "bajo";
}

export interface AjusteRecomendado {
  variable: string;
  accion: "reducir" | "aumentar" | "revisar";
  rango_sugerido: string;
  motivo: string;
}

export interface ChecksDetallados {
  capacidad: string;
  costes: string;
  margenes: string;
  roi: string;
  payback: string;
}

export interface ValidacionFinanciera {
  coherencia_global: "alta" | "media" | "baja";
  veredicto: "fiable" | "optimista" | "no_creible";
  problemas_detectados: ProblemaDetectado[];
  ajustes_recomendados: AjusteRecomendado[];
  supuestos_peligrosos: string[];
  checks_detallados: ChecksDetallados;
}

export interface FinancieroResponse {
  parametros: Record<string, ParametroResponse>;
  inversion_total: number;
  desglose_inversion: Record<string, number>;
  ingresos_anuales_conservador: number;
  ingresos_anuales_base?: number;
  ingresos_anuales_optimista: number;
  margen_bruto_pct: number;
  ebitda_anual_conservador: number;
  ebitda_anual_base?: number;
  ebitda_anual_optimista: number;
  roi_3a_conservador: number;
  roi_3a_base?: number;
  roi_3a_optimista: number;
  payback_meses_conservador: number;
  payback_meses_base?: number;
  payback_meses_optimista: number;
  breakeven_clientes_dia: number;
  proyeccion: ProyeccionMes[];
  margen_sector_tipico: number;
  alquiler_sobre_ventas_pct: number;
  alerta_alquiler: boolean;
  // Bloques v2
  decision?: DecisionBlock;
  economia_base?: EconomiaBase;
  estructura_costes?: EstructuraCostes;
  break_even?: BreakEvenInfo;
  metricas_clave?: MetricasClave;
  riesgos?: Riesgo[];
  insights?: Insight[];
  modelo_demanda?: ModeloDemanda;
  // Contexto v3
  business_model_type?: string;
  correcciones_aplicadas?: CorreccionAplicada[];
  capacity_model?: CapacityModelInfo;
  tipo_negocio?: "nuevo" | "traspaso";
  validation_flags?: string[];
  ocupacion_efectiva?: number;
  validacion_financiera?: ValidacionFinanciera;
}

export interface LocalDetalleResponse {
  zona: ZonaDetalle;
  financiero_preview?: Partial<FinancieroResponse>;
}

export interface ProfesionalExterno {
  nombre: string;
  descripcion: string;
  coste_aprox: string;
}

export interface TramiteLegal {
  numero: number;
  titulo: string;
  nombre_oficial?: string;
  que_es: string;
  donde: string;
  documentos?: string[];
  tiempo_estimado?: string;
  coste_estimado?: string;
  enlace?: string;
  alerta?: string;
}

export interface FaseRoadmap {
  id: string;
  numero: number;
  titulo: string;
  descripcion?: string;
  tramites: TramiteLegal[];
}

export interface CosteRoadmap {
  concepto: string;
  coste: string;
}

export interface LegalRoadmapResponse {
  tipo_negocio: string;
  distrito: string;
  zona_restringida: boolean;
  equipo_externo: ProfesionalExterno[];
  fases: FaseRoadmap[];
  costes_resumen: CosteRoadmap[];
  proximos_pasos: string[];
}

// ── Transporte — detalle de líneas cercanas a la zona ─────────────────────
export type TransporteTipo = "metro" | "bus" | "tram" | "fgc" | "rodalies";

export interface ParadaCercana {
  nombre: string;
  distancia_m: number;
  tipo: TransporteTipo | string;
}

export interface LineaCercana {
  tipo: TransporteTipo | string;
  codigo: string;
  nombre: string;
  color?: string | null;
  dist_min_m: number;
  paradas_cercanas: ParadaCercana[];
}

export interface TransporteDetalleZona {
  zona_id: string;
  radio_m: number;
  total_lineas: number;
  total_paradas: number;
  lineas: LineaCercana[];
}

export interface DevData {
  flujo_fuentes?: Record<string, unknown>;
  transporte?: Record<string, unknown>;
  competencia_raw?: {
    competidores?: CompetidorDetalle[];
    [key: string]: unknown;
  };
  demografia?: Record<string, unknown>;
  seguridad?: Record<string, unknown>;
  entorno?: Record<string, unknown>;
  turismo?: Record<string, unknown>;
  ml?: {
    modelo_version?: string;
    probabilidad_supervivencia?: number;
    shap_values?: Record<string, number>;
    [key: string]: unknown;
  };
}
