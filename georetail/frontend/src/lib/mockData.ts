import type {
  ZonaPreview,
  LocalDetalleResponse,
  FinancieroResponse,
  BuscarResponse,
} from "./types";

export const MOCK_ZONAS: ZonaPreview[] = [
  {
    zona_id: "mock-eixample-01",
    nombre: "Eixample Esquerra – Calle Muntaner",
    barrio: "Eixample Esquerra",
    distrito: "Eixample",
    lat: 41.3871,
    lng: 2.1541,
    direccion: "Carrer de Muntaner, 112",
    m2: 85,
    alquiler_mensual: 2400,
    score_global: 82,
  },
  {
    zona_id: "mock-gracia-01",
    nombre: "Vila de Gràcia – Travessera de Gràcia",
    barrio: "Vila de Gràcia",
    distrito: "Gràcia",
    lat: 41.4034,
    lng: 2.1569,
    direccion: "Travessera de Gràcia, 76",
    m2: 72,
    alquiler_mensual: 1850,
    score_global: 76,
  },
  {
    zona_id: "mock-born-01",
    nombre: "El Born – Carrer del Comerç",
    barrio: "Sant Pere, Santa Caterina i la Ribera",
    distrito: "Ciutat Vella",
    lat: 41.3853,
    lng: 2.1826,
    direccion: "Carrer del Comerç, 24",
    m2: 60,
    alquiler_mensual: 2100,
    score_global: 71,
  },
  {
    zona_id: "mock-poblenou-01",
    nombre: "Poblenou – Rambla del Poblenou",
    barrio: "Poblenou",
    distrito: "Sant Martí",
    lat: 41.3988,
    lng: 2.1978,
    direccion: "Rambla del Poblenou, 44",
    m2: 95,
    alquiler_mensual: 1600,
    score_global: 68,
  },
  {
    zona_id: "mock-sarria-01",
    nombre: "Sarrià – Carrer Major de Sarrià",
    barrio: "Sarrià",
    distrito: "Sarrià-Sant Gervasi",
    lat: 41.3978,
    lng: 2.1215,
    direccion: "Carrer Major de Sarrià, 33",
    m2: 110,
    alquiler_mensual: 2200,
    score_global: 74,
  },
  {
    zona_id: "mock-poblesec-01",
    nombre: "Poble Sec – Carrer de Blai",
    barrio: "Poble Sec",
    distrito: "Sants-Montjuïc",
    lat: 41.3742,
    lng: 2.1601,
    direccion: "Carrer de Blai, 18",
    m2: 55,
    alquiler_mensual: 1350,
    score_global: 63,
  },
];

const MOCK_DETALLE_BASE = (zona: ZonaPreview): LocalDetalleResponse => ({
  zona: {
    zona_id: zona.zona_id,
    nombre: zona.nombre,
    barrio: zona.barrio,
    distrito: zona.distrito,
    lat: zona.lat,
    lng: zona.lng,
    direccion: zona.direccion,
    m2: zona.m2,
    alquiler_mensual: zona.alquiler_mensual,
    disponible: true,
    score_global: zona.score_global,
    scores_dimensiones: {
      flujo_peatonal: Math.round((zona.score_global ?? 70) * 0.9 + Math.random() * 15),
      demografia: Math.round((zona.score_global ?? 70) * 0.85 + Math.random() * 15),
      competencia: Math.round((zona.score_global ?? 70) * 1.05 + Math.random() * 10 - 5),
      precio_alquiler: Math.round((zona.score_global ?? 70) * 0.95 + Math.random() * 10),
      transporte: Math.round((zona.score_global ?? 70) * 1.1 + Math.random() * 10 - 5),
      seguridad: Math.round((zona.score_global ?? 70) * 0.92 + Math.random() * 12),
      turismo: Math.round((zona.score_global ?? 70) * 0.8 + Math.random() * 20),
      entorno_comercial: Math.round((zona.score_global ?? 70) + Math.random() * 15 - 7),
    },
    probabilidad_supervivencia: (zona.score_global ?? 70) / 100 * 0.85 + 0.1,
    flujo_peatonal_dia: { manana: 3200, tarde: 5800, noche: 2100 },
    renta_media_hogar: 38000,
    edad_media: 34,
    pct_extranjeros: 0.18,
    num_negocios_activos: 47,
    pct_locales_vacios: 0.08,
    num_lineas_transporte: 4,
    num_paradas_transporte: 6,
    competidores_cercanos: [
      { nombre: "Bar El Rincón", sector: "Hostelería", distancia_m: 120, rating: 4.2, precio_nivel: 2, es_competencia_directa: true },
      { nombre: "Cafetería Bona Sort", sector: "Hostelería", distancia_m: 210, rating: 3.8, precio_nivel: 1, es_competencia_directa: true },
      { nombre: "Supermercado Bon Preu", sector: "Alimentación", distancia_m: 350, rating: 4.0, precio_nivel: 2, es_competencia_directa: false },
    ],
    alertas: [
      { tipo: "info", texto: "Zona con alta densidad peatonal en horario de tarde.", fuente: "Ajuntament de Barcelona" },
    ],
    analisis_ia: {
      resumen: `${zona.nombre} es una ubicación con alto potencial para negocios de hostelería y retail de proximidad. La combinación de flujo peatonal constante y una demografía joven con poder adquisitivo medio-alto la hace especialmente atractiva.`,
      puntos_fuertes: [
        "Flujo peatonal elevado en franja vespertina",
        "Buena conectividad con transporte público",
        "Demografía favorable: edad media 34 años",
        "Bajo porcentaje de locales vacíos (8%)",
      ],
      puntos_debiles: [
        "Competencia directa a menos de 200m",
        "Alquiler ligeramente por encima de la media del distrito",
      ],
      oportunidad: "Mercado en crecimiento con demanda insatisfecha en el segmento premium-accesible.",
      riesgos: "Posible saturación del segmento si se abren más establecimientos similares en los próximos 12 meses.",
      recomendacion_final: "RECOMENDADA",
      razon_recomendacion: "La ubicación presenta una combinación óptima de factores para un negocio de restauración de ticket medio.",
    },
  },
  financiero_preview: {
    inversion_total: 85000,
    ingresos_anuales_conservador: 180000,
    ingresos_anuales_optimista: 240000,
    payback_meses_conservador: 22,
    roi_3a_conservador: 0.38,
  },
});

const MOCK_FINANCIERO: FinancieroResponse = {
  parametros: {
    ticket_medio: { valor_usado: 14, fuente: "Benchmark sectorial", rango_min: 10, rango_max: 20 },
    clientes_dia: { valor_usado: 85, fuente: "Estimación flujo peatonal", rango_min: 60, rango_max: 120 },
    dias_operacion_mes: { valor_usado: 26, fuente: "Estándar sector", rango_min: 24, rango_max: 28 },
  },
  inversion_total: 85000,
  desglose_inversion: {
    "Reforma y adecuación": 35000,
    "Equipamiento cocina": 22000,
    "Mobiliario y decoración": 12000,
    "Fianza y gastos apertura": 9000,
    "Stock inicial": 4000,
    "Otros": 3000,
  },
  ingresos_anuales_conservador: 180240,
  ingresos_anuales_optimista: 240320,
  margen_bruto_pct: 0.68,
  ebitda_anual_conservador: 28800,
  ebitda_anual_optimista: 52000,
  roi_3a_conservador: 0.38,
  roi_3a_optimista: 0.71,
  payback_meses_conservador: 22,
  payback_meses_optimista: 15,
  breakeven_clientes_dia: 42,
  alerta_alquiler: false,
  alquiler_sobre_ventas_pct: 0.14,
  margen_sector_tipico: 0.65,
  proyeccion: Array.from({ length: 12 }, (_, i) => ({
    mes: i + 1,
    ingresos_conservador: Math.round(15020 * (0.5 + (i / 12) * 0.5)),
    ingresos_optimista: Math.round(20027 * (0.55 + (i / 12) * 0.45)),
    costes_variables: Math.round(4800 * (0.5 + (i / 12) * 0.5)),
    costes_fijos: 8200,
    ebitda_conservador: Math.round(2400 * (0.5 + (i / 12) * 0.5) - 2400 * (i === 0 ? 1 : 0)),
    ebitda_optimista: Math.round(4333 * (0.55 + (i / 12) * 0.45)),
    acumulado_conservador: Math.round(2400 * (i + 1) * 0.7 - 85000),
    acumulado_optimista: Math.round(4333 * (i + 1) * 0.8 - 85000),
    ramp_factor: Math.min(1, 0.5 + (i / 11) * 0.5),
  })),
};

// ────────────────────────────────────────────────
// API mock functions
// ────────────────────────────────────────────────

let mockSessionId = "demo-session-001";

export function mockBuscar(descripcion: string): BuscarResponse {
  return {
    estado: "ok",
    session_id: mockSessionId,
    zonas: MOCK_ZONAS,
    total: MOCK_ZONAS.length,
    progreso_pct: 100,
    mensaje: `Demo: mostrando ${MOCK_ZONAS.length} zonas de ejemplo en Barcelona para "${descripcion}"`,
  };
}

export function mockLocalDetalle(zona_id: string): LocalDetalleResponse {
  const zona = MOCK_ZONAS.find(z => z.zona_id === zona_id) ?? MOCK_ZONAS[0];
  return MOCK_DETALLE_BASE(zona);
}

export function mockFinanciero(): FinancieroResponse {
  return MOCK_FINANCIERO;
}
