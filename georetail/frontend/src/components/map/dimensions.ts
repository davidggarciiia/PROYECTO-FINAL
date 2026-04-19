/**
 * Dimensiones KnowPoints — mapping a los 8 scorers reales del backend.
 * Mantiene los nombres en español y añade una abreviatura de 5 caracteres
 * para los mini-bars del ActiveDock.
 */

export type DimensionKey =
  | "flujo_peatonal"
  | "demografia"
  | "competencia"
  | "transporte"
  | "seguridad"
  | "turismo"
  | "dinamismo"
  | "precio_alquiler";

export interface DimensionBreakdownRow {
  label: string;
  meta: string;
  /** Offset applied to the dimension score to generate a visually realistic
   *  placeholder value per sub-row. Will be clamped to [0, 100]. */
  offset: number;
}

export interface DimensionMeta {
  key: DimensionKey;
  name: string;
  short: string;
  hint: string;
  subtitle: string;
  what: string;
  sources: string[];
  /** TODO: reemplazar por desglose real cuando el backend lo devuelva. */
  breakdown: DimensionBreakdownRow[];
}

export const DIMENSIONS: DimensionMeta[] = [
  {
    key: "flujo_peatonal",
    name: "Flujo peatonal",
    short: "FLUJO",
    hint: "Aforadors BCN",
    subtitle: "Volumen y calidad del tráfico peatonal en el entorno inmediato del local.",
    what: "Mide cuántas personas pasan por delante del local y a qué ritmo — no es lo mismo una calle de paseo que una de tránsito rápido. Combinamos aforadors municipales, conteo por visión artificial sobre cámaras públicas y triangulación anónima de datos de operadoras móviles en franjas horarias clave.",
    sources: ["Aforadors BCN", "VCity BSC", "Vianants BCN", "Ratio comercial"],
    breakdown: [
      { label: "Paseantes hora punta", meta: "Media 18:00–20:30 · días laborables", offset: 6 },
      { label: "Flujo fin de semana", meta: "Sábado 12:00–14:00 vs laborable", offset: -8 },
      { label: "Ratio paseo / tránsito", meta: "Velocidad media < 3 km/h", offset: -4 },
      { label: "Estacionalidad", meta: "Estabilidad 12 meses", offset: 2 },
    ],
  },
  {
    key: "demografia",
    name: "Demografía",
    short: "DEMO",
    hint: "INE · Idescat",
    subtitle: "Perfil demográfico y de renta del residente y trabajador habitual de la zona.",
    what: "Cruzamos datos del INE, Idescat y secciones censales para medir edad media, nivel formativo, renta por hogar y densidad de teletrabajo. La idea: saber si el público del barrio encaja con el ticket medio de tu negocio antes de firmar.",
    sources: ["INE", "Idescat", "Padró BCN", "Renda BCN"],
    breakdown: [
      { label: "Renta media del hogar", meta: "Índice · BCN = 100", offset: 4 },
      { label: "Franja 25–44 años", meta: "% sobre población", offset: -4 },
      { label: "Estudios superiores", meta: "% población activa", offset: 3 },
      { label: "Densidad teletrabajo", meta: "Clave para flujo diurno", offset: -6 },
    ],
  },
  {
    key: "competencia",
    name: "Competencia",
    short: "COMP",
    hint: "200 m radio",
    subtitle: "Densidad y calidad de la competencia directa en 200 m a la redonda.",
    what: "Analizamos cada local con actividad equivalente en 200 metros: horario, precio medio, puntuación de reseñas, hueco de mercado y solape de propuesta. Una competencia excelente te quita clientes; una competencia mediocre y abundante te da espacio.",
    sources: ["Cens locals BCN", "Google Places", "Foursquare", "NLP reseñas"],
    breakdown: [
      { label: "Locales directos en 200 m", meta: "Densidad vs. media del distrito", offset: 6 },
      { label: "Reseña media competencia", meta: "Escala Google · 100 = 5 ★", offset: -6 },
      { label: "Hueco de ticket medio", meta: "Gap vs propuesta similar", offset: 8 },
      { label: "Nuevos entrantes 12 m", meta: "Aperturas recientes / cierres", offset: -4 },
    ],
  },
  {
    key: "transporte",
    name: "Transporte",
    short: "TRANS",
    hint: "TMB · Bicing",
    subtitle: "Conectividad de transporte público y movilidad blanda en 5 minutos a pie.",
    what: "Suma de metro, bus, Bicing, aparcamiento rotatorio y proximidad a estaciones de cercanías, ponderado por frecuencia real. Una buena conectividad amplía el radio de captación más allá del barrio.",
    sources: ["TMB GTFS", "Bicing GBFS", "Rodalies", "Ajuntament · movilidad"],
    breakdown: [
      { label: "Metro en 5 min", meta: "Líneas · frecuencia pico", offset: 6 },
      { label: "Paradas de bus", meta: "En 150 m · líneas distintas", offset: -4 },
      { label: "Estaciones Bicing", meta: "En 300 m · disponibilidad", offset: -6 },
      { label: "Conexión cercanías", meta: "Rodalies / FGC", offset: -12 },
    ],
  },
  {
    key: "seguridad",
    name: "Seguridad",
    short: "SEG",
    hint: "Mossos · GUB",
    subtitle: "Nivel de seguridad percibida y datos objetivos de incidencias en el entorno.",
    what: "Combinamos datos abiertos de Mossos d'Esquadra y Guardia Urbana con encuestas de percepción ciudadana. Medimos incidencias contra la propiedad, altercados en vía pública y percepción nocturna — más relevante cuanto más dure tu servicio hacia la tarde-noche.",
    sources: ["Mossos", "Guardia Urbana", "Encuesta victimización", "Iluminación viaria"],
    breakdown: [
      { label: "Incidencias contra propiedad", meta: "Tasa por 1.000 hab · 12 meses", offset: 6 },
      { label: "Percepción seguridad nocturna", meta: "Encuesta ciudadana", offset: -4 },
      { label: "Iluminación del tramo", meta: "Lux en calzada y acera", offset: 4 },
      { label: "Presencia policial efectiva", meta: "Tiempo medio de respuesta", offset: -2 },
    ],
  },
  {
    key: "turismo",
    name: "Turismo",
    short: "TURIS",
    hint: "BCN Turisme",
    subtitle: "Intensidad turística: visitantes internacionales, estacionalidad y dependencia.",
    what: "Cuantificamos el peso del turismo sobre el gasto local: pernoctaciones en 500 m, paso por puntos icónicos, estacionalidad del flujo y gasto medio por visitante. Más turismo puede subir el ticket — pero también hace el negocio estacional.",
    sources: ["BCN Turisme", "INE EOH", "Airbnb", "HUT Generalitat"],
    breakdown: [
      { label: "Pernoctaciones en 500 m", meta: "Hoteles + HUT + Airbnb", offset: 6 },
      { label: "Paso por hitos icónicos", meta: "Distancia a top 10 atractivos", offset: -4 },
      { label: "Estacionalidad", meta: "Pico verano vs. invierno", offset: 4 },
      { label: "Gasto medio visitante", meta: "Ticket turista vs. residente", offset: -6 },
    ],
  },
  {
    key: "dinamismo",
    name: "Dinamismo",
    short: "DINAM",
    hint: "Aperturas / cierres",
    subtitle: "Dinamismo comercial: ritmo de aperturas, cierres y llegada de operadores referente.",
    what: "Medimos cuántos locales abren y cierran al mes, qué operadores referentes han llegado al barrio en los últimos 24 meses y la evolución del precio medio por m² en alquiler comercial. Un barrio dinámico atrae inversión y público.",
    sources: ["Llicències BCN", "Idealista comercial", "Cens comercial", "Observatori"],
    breakdown: [
      { label: "Aperturas / cierres · 12 m", meta: "Ratio > 1 = crecimiento", offset: 6 },
      { label: "Operadores referente llegados", meta: "Últimos 24 meses", offset: -6 },
      { label: "Evolución €/m² alquiler", meta: "Tendencia 24 m", offset: 4 },
      { label: "Inversión pública prevista", meta: "Planes urbanismo 36 m", offset: -10 },
    ],
  },
  {
    key: "precio_alquiler",
    name: "Precio alquiler",
    short: "PRECIO",
    hint: "Idealista · BCN",
    subtitle: "Nivel de alquiler comercial: asequibilidad respecto a la media de la ciudad.",
    what: "Comparamos el €/m² mensual de alquiler comercial en la zona con el resto de Barcelona. Un score alto significa que el precio es bajo o razonable para el tipo de negocio; uno bajo significa que la zona es cara y exigirá un ticket o facturación acordes.",
    sources: ["Idealista", "Fotocasa", "Habitaclia", "Open Data BCN"],
    breakdown: [
      { label: "€/m² comercial zona", meta: "Mediana anuncios activos", offset: 4 },
      { label: "Evolución 12 m", meta: "Tendencia precio reciente", offset: -4 },
      { label: "Asequibilidad vs BCN", meta: "Índice BCN = 100", offset: 6 },
      { label: "Locales disponibles", meta: "Oferta activa · 200 m", offset: -6 },
    ],
  },
];

export const DIM_BY_KEY: Record<DimensionKey, DimensionMeta> = DIMENSIONS.reduce(
  (acc, d) => ({ ...acc, [d.key]: d }),
  {} as Record<DimensionKey, DimensionMeta>,
);
