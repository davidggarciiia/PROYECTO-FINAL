/**
 * Dimensiones del editorial — 7 reales del scoring, sin precio_alquiler.
 *
 * El alquiler vive ahora dentro de la sección Financiero (€/m² + alerta
 * sobre ventas). Mantenemos sólo las dimensiones que tienen un scorer
 * detrás en backend/scoring/dimensiones/*.
 */

export type DimensionKey =
  | "flujo_peatonal"
  | "demografia"
  | "competencia"
  | "transporte"
  | "seguridad"
  | "turismo"
  | "dinamismo";

export interface DimensionMeta {
  key: DimensionKey;
  /** Nombre completo, p. ej. "Flujo peatonal". */
  name: string;
  /** Forma corta para badges (5–6 chars). */
  short: string;
  /** Hint de fuente que ya usa el dossier viejo. */
  hint: string;
  /** Lead 1 línea, lo que el usuario lee en el TOC y en el Ledger. */
  subtitle: string;
  /** Texto largo sobre cómo se calcula. */
  what: string;
  /** Fuentes que se muestran como chips si la explicación no trae las suyas. */
  sources: string[];
}

export const DIMENSIONS: DimensionMeta[] = [
  {
    key: "flujo_peatonal",
    name: "Flujo peatonal",
    short: "FLUJO",
    hint: "VCity BSC",
    subtitle:
      "Volumen y reparto del tráfico peatonal en el entorno inmediato del local.",
    what:
      "Cruzamos la malla VCity del Barcelona Supercomputing Center (triangulación anónima de telefonía + visión artificial) con los picos de Google Popular Times. La estimación cubre toda la ciudad de forma homogénea, sin depender de aforadors físicos.",
    sources: ["VCity BSC", "Google Popular Times"],
  },
  {
    key: "demografia",
    name: "Demografía",
    short: "DEMO",
    hint: "INE · Idescat",
    subtitle:
      "Perfil del residente y trabajador habitual de la zona — renta, edad y nivel formativo.",
    what:
      "Cruzamos INE, Idescat y secciones censales del Padró Municipal. Buscamos saber si el público del barrio encaja con el ticket medio de tu negocio antes de firmar el alquiler.",
    sources: ["INE", "Idescat", "Padró BCN", "Renda BCN"],
  },
  {
    key: "competencia",
    name: "Competencia",
    short: "COMP",
    hint: "200 m radio",
    subtitle:
      "Densidad y calidad de la competencia directa, indirecta y complementaria a 200 m.",
    what:
      "Analizamos cada local con actividad equivalente en 500 m: horario, precio medio, reseñas y solape de propuesta. Una competencia excelente te quita clientes; una competencia mediocre y abundante te da espacio.",
    sources: ["Cens locals BCN", "Google Places", "Foursquare", "NLP reseñas"],
  },
  {
    key: "transporte",
    name: "Transporte",
    short: "TRANS",
    hint: "TMB · Bicing",
    subtitle:
      "Conectividad multimodal en 5 minutos a pie — metro, bus, Bicing y cercanías.",
    what:
      "Suma de metro, bus, Bicing, aparcamiento rotatorio y cercanías, ponderada por frecuencia real. Una buena conectividad amplía el radio de captación más allá del barrio.",
    sources: ["TMB GTFS", "Bicing GBFS", "Rodalies", "Ajuntament · movilidad"],
  },
  {
    key: "seguridad",
    name: "Seguridad",
    short: "SEG",
    hint: "Mossos · GUB",
    subtitle:
      "Incidencias contra la propiedad, percepción y reparto día/noche del entorno.",
    what:
      "Datos abiertos de Mossos d'Esquadra y Guardia Urbana cruzados con la encuesta de victimización municipal. Hurtos, robos, daños y altercados por 1.000 habitantes — más relevante cuanto más se alargue el servicio hacia la noche.",
    sources: ["Mossos", "Guardia Urbana", "Encuesta victimización"],
  },
  {
    key: "turismo",
    name: "Turismo",
    short: "TURIS",
    hint: "BCN Turisme",
    subtitle:
      "Intensidad turística — alojamiento, landmarks y dependencia estacional.",
    what:
      "Pesamos pernoctaciones a 500 m, paso por puntos icónicos, estacionalidad y gasto medio por visitante. Más turismo puede subir el ticket — pero también hace el negocio más estacional.",
    sources: ["BCN Turisme", "INE EOH", "Airbnb", "HUT Generalitat"],
  },
  {
    key: "dinamismo",
    name: "Dinamismo",
    short: "DINAM",
    hint: "Aperturas / cierres",
    subtitle:
      "Ritmo de aperturas, cierres y llegada de operadores referente en los últimos 24 meses.",
    what:
      "Contamos cuántos locales abren y cierran al mes, qué operadores referentes han llegado al barrio y cómo evoluciona el alquiler comercial. Un barrio dinámico atrae inversión y público.",
    sources: ["Llicències BCN", "Idealista comercial", "Cens comercial", "Observatori"],
  },
];

export const DIM_BY_KEY: Record<DimensionKey, DimensionMeta> = DIMENSIONS.reduce(
  (acc, d) => ({ ...acc, [d.key]: d }),
  {} as Record<DimensionKey, DimensionMeta>,
);

/* ── Banding semántico ──────────────────────────────────────────────────── */

export type Band = "hi" | "mid" | "lo" | "na";

export function scoreBand(score?: number | null): Band {
  if (score == null || Number.isNaN(score)) return "na";
  if (score >= 75) return "hi";
  if (score >= 55) return "mid";
  return "lo";
}

export function bandLabel(band: Band): string {
  if (band === "hi") return "Favorable";
  if (band === "mid") return "Aceptable";
  if (band === "lo") return "Desfavorable";
  return "Sin datos";
}
