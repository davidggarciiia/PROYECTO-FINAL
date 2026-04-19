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

export interface BuscarRequest {
  descripcion: string;
  session_id?: string;
  filtros?: Record<string, unknown>;
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
  ingresos_optimista: number;
  costes_variables: number;
  costes_fijos: number;
  ebitda_conservador: number;
  ebitda_optimista: number;
  acumulado_conservador: number;
  acumulado_optimista: number;
  ramp_factor: number;
}

export interface ParametroFinanciero {
  valor_usado: number;
  fuente: string;
  rango_min: number;
  rango_max: number;
}

export interface FinancieroResponse {
  parametros: Record<string, ParametroFinanciero>;
  inversion_total: number;
  desglose_inversion: Record<string, number>;
  ingresos_anuales_conservador: number;
  ingresos_anuales_optimista: number;
  margen_bruto_pct: number;
  ebitda_anual_conservador: number;
  ebitda_anual_optimista: number;
  roi_3a_conservador: number;
  roi_3a_optimista: number;
  payback_meses_conservador: number;
  payback_meses_optimista: number;
  breakeven_clientes_dia: number;
  proyeccion: ProyeccionMes[];
  margen_sector_tipico: number;
  alquiler_sobre_ventas_pct: number;
  alerta_alquiler: boolean;
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
