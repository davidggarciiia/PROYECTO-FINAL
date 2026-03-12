// Tipos TypeScript sincronizados con schemas/models.py del backend

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
  distancia_m?: number;
  rating?: number;
  precio_nivel?: number;
  es_competencia_directa: boolean;
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

export interface ScoresDimensiones {
  flujo_peatonal?: number;
  demografia?: number;
  competencia?: number;
  precio_alquiler?: number;
  transporte?: number;
  seguridad?: number;
  turismo?: number;
  entorno_comercial?: number;
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
  score_global?: number;
  scores_dimensiones?: ScoresDimensiones;
  probabilidad_supervivencia?: number;
  flujo_peatonal_dia?: { manana: number; tarde: number; noche: number };
  renta_media_hogar?: number;
  edad_media?: number;
  pct_extranjeros?: number;
  num_negocios_activos?: number;
  pct_locales_vacios?: number;
  num_lineas_transporte?: number;
  num_paradas_transporte?: number;
  competidores_cercanos: CompetidorCercano[];
  alertas: AlertaZona[];
  analisis_ia?: AnalisisIA;
}

export interface LocalDetalle {
  zona_id: string;
  local_id?: string;
  nombre_zona: string;
  barrio: string;
  distrito: string;
  score_global: number;
  scores_dimension: ScoresDimensiones;
  probabilidad_supervivencia_3a?: number;
  explicaciones_shap: Array<{ feature: string; valor: number; descripcion?: string }>;
  direccion?: string;
  m2?: number;
  planta?: string;
  alquiler_mensual?: number;
  disponible?: boolean;
  competidores_cercanos: CompetidorCercano[];
  alertas: AlertaZona[];
  flujo_peatonal_dia: { manana: number; tarde: number; noche: number };
  tiempo_transporte_centro_min?: number;
  num_lineas_transporte?: number;
  analisis_ia: string;
  pros: string[];
  contras: string[];
  recomendacion_final: string;
  info_legal: {
    viabilidad: "viable" | "restringido" | "inviable";
    alerta?: string;
    licencias_necesarias: Array<{
      nombre: string;
      organismo: string;
      coste_estimado?: string;
      tiempo_semanas?: string;
      notas?: string;
    }>;
    restriccion_zona?: { tipo: string; descripcion: string };
    requisitos_local: string[];
  };
}

// Búsqueda
export interface BuscarRequest {
  descripcion: string;
  session_id?: string;
  filtros?: Record<string, unknown>;
}

export interface BuscarResponse {
  estado: "ok" | "cuestionario" | "error_tipo_negocio" | "inviable_legal";
  session_id: string;
  zonas: ZonaPreview[];
  total: number;
  task_id?: string;
  pregunta?: string;
  progreso_pct: number;
  mensaje?: string;
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

// Financiero
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

export interface FinancieroResponse {
  parametros: Record<string, { valor_usado: number; fuente: string; rango_min: number; rango_max: number }>;
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
