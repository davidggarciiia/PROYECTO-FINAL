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

export interface DimensionMeta {
  key: DimensionKey;
  name: string;
  short: string;
  hint: string;
  subtitle: string;
  what: string;
  sources: string[];
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
  },
  {
    key: "demografia",
    name: "Demografía",
    short: "DEMO",
    hint: "INE · Idescat",
    subtitle: "Perfil demográfico y de renta del residente y trabajador habitual de la zona.",
    what: "Cruzamos datos del INE, Idescat y secciones censales para medir edad media, nivel formativo, renta por hogar y densidad de teletrabajo. La idea: saber si el público del barrio encaja con el ticket medio de tu negocio antes de firmar.",
    sources: ["INE", "Idescat", "Padró BCN", "Renda BCN"],
  },
  {
    key: "competencia",
    name: "Competencia",
    short: "COMP",
    hint: "200 m radio",
    subtitle: "Densidad y calidad de la competencia directa en 200 m a la redonda.",
    what: "Analizamos cada local con actividad equivalente en 200 metros: horario, precio medio, puntuación de reseñas, hueco de mercado y solape de propuesta. Una competencia excelente te quita clientes; una competencia mediocre y abundante te da espacio.",
    sources: ["Cens locals BCN", "Google Places", "Foursquare", "NLP reseñas"],
  },
  {
    key: "transporte",
    name: "Transporte",
    short: "TRANS",
    hint: "TMB · Bicing",
    subtitle: "Conectividad de transporte público y movilidad blanda en 5 minutos a pie.",
    what: "Suma de metro, bus, Bicing, aparcamiento rotatorio y proximidad a estaciones de cercanías, ponderado por frecuencia real. Una buena conectividad amplía el radio de captación más allá del barrio.",
    sources: ["TMB GTFS", "Bicing GBFS", "Rodalies", "Ajuntament · movilidad"],
  },
  {
    key: "seguridad",
    name: "Seguridad",
    short: "SEG",
    hint: "Mossos · GUB",
    subtitle: "Nivel de seguridad percibida y datos objetivos de incidencias en el entorno.",
    what: "Combinamos datos abiertos de Mossos d'Esquadra y Guardia Urbana con encuestas de percepción ciudadana. Medimos incidencias contra la propiedad, altercados en vía pública y percepción nocturna — más relevante cuanto más dure tu servicio hacia la tarde-noche.",
    sources: ["Mossos", "Guardia Urbana", "Encuesta victimización", "Iluminación viaria"],
  },
  {
    key: "turismo",
    name: "Turismo",
    short: "TURIS",
    hint: "BCN Turisme",
    subtitle: "Intensidad turística: visitantes internacionales, estacionalidad y dependencia.",
    what: "Cuantificamos el peso del turismo sobre el gasto local: pernoctaciones en 500 m, paso por puntos icónicos, estacionalidad del flujo y gasto medio por visitante. Más turismo puede subir el ticket — pero también hace el negocio estacional.",
    sources: ["BCN Turisme", "INE EOH", "Airbnb", "HUT Generalitat"],
  },
  {
    key: "dinamismo",
    name: "Dinamismo",
    short: "DINAM",
    hint: "Aperturas / cierres",
    subtitle: "Dinamismo comercial: ritmo de aperturas, cierres y llegada de operadores referente.",
    what: "Medimos cuántos locales abren y cierran al mes, qué operadores referentes han llegado al barrio en los últimos 24 meses y la evolución del precio medio por m² en alquiler comercial. Un barrio dinámico atrae inversión y público.",
    sources: ["Llicències BCN", "Idealista comercial", "Cens comercial", "Observatori"],
  },
  {
    key: "precio_alquiler",
    name: "Precio alquiler",
    short: "PRECIO",
    hint: "Idealista · BCN",
    subtitle: "Nivel de alquiler comercial: asequibilidad respecto a la media de la ciudad.",
    what: "Comparamos el €/m² mensual de alquiler comercial en la zona con el resto de Barcelona. Un score alto significa que el precio es bajo o razonable para el tipo de negocio; uno bajo significa que la zona es cara y exigirá un ticket o facturación acordes.",
    sources: ["Idealista", "Fotocasa", "Habitaclia", "Open Data BCN"],
  },
];

export const DIM_BY_KEY: Record<DimensionKey, DimensionMeta> = DIMENSIONS.reduce(
  (acc, d) => ({ ...acc, [d.key]: d }),
  {} as Record<DimensionKey, DimensionMeta>,
);
