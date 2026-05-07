import type { DimensionKey } from "./dimensions";

/**
 * Implicaciones operativas por dimensión × sector.
 *
 * Es texto editorial — no viene del backend. Lo que sí viene del backend
 * (explicaciones_dimensiones.impacto_modelo, alertas_zona, recomendacion_final
 * del análisis_ia) lo mostramos donde corresponde. Esto rellena el bloque
 * "Implicaciones operativas" del informe con frases que un humano escribiría
 * después de leer los datos.
 *
 * Si el sector no está mapeado, devuelve la lista por defecto.
 */

type Implicaciones = Partial<Record<string, string[]>>;

const POR_SECTOR: Partial<Record<DimensionKey, Implicaciones>> = {
  flujo_peatonal: {
    restauracion: [
      "Si el pico es tarde/noche, refuerza plantilla esas franjas y sirve carta express.",
      "Si el flujo cae en domingo, valora cierre o brunch reducido.",
      "Diseña la barra para servir 80–100 cubiertos/h en hora punta sin colas.",
    ],
    moda: [
      "Concentra rebajas y novedades en los días de pico medido.",
      "Aprovecha el flujo de paso con escaparate dinámico — rota cada 10 días.",
      "Si el flujo es residente, fideliza con club de cliente.",
    ],
    estetica: [
      "Si predomina el residente, agenda reservada con 48h de margen.",
      "El flujo de paso ayuda al walk-in para servicios cortos (manicura, depilación).",
      "Adapta el horario al pico real medido, no al horario del barrio.",
    ],
    inmobiliario: [
      "El flujo importa menos que la visibilidad — prioriza esquina o local con escaparate corrido.",
    ],
  },
  demografia: {
    restauracion: [
      "El ticket medio debe encajar con la renta hogar de la zona.",
      "Si hay alta presencia universitaria, valora menú del día y after-work.",
      "Comunicación bilingüe ES/EN si la zona tiene >15% extranjeros.",
    ],
    moda: [
      "Calibra precio y producto a la franja de edad dominante.",
      "Renta alta sostiene producto premium y exclusividad.",
      "Si la zona es joven, refuerza presencia en redes sociales locales.",
    ],
    estetica: [
      "El público objetivo del servicio debe coincidir con la edad mediana del barrio.",
      "Renta alta soporta tratamientos premium y aparatología.",
    ],
  },
  competencia: {
    restauracion: [
      "Diferénciate por concepto, no por precio — los locales con peor reseña son los más vulnerables.",
      "Si el HHI es fragmentado, hay hueco para una propuesta clara.",
      "Vigila los 2-3 competidores con > 4,5 estrellas — son la referencia que tendrás que superar.",
    ],
    moda: [
      "El concept store o nicho funciona mejor donde hay competencia genérica.",
      "Si hay cluster de moda, el flujo cruzado entre tiendas te beneficia.",
    ],
    estetica: [
      "Mismo subsector saturado pero distinto público (hombre/mujer, premium/básico) deja hueco.",
      "Posiciónate por especialidad, no por gama completa.",
    ],
  },
  transporte: {
    restauracion: [
      "Comunica accesibilidad: «a 4 min de la parada X» en Google y RRSS.",
      "Si hay metro a < 5 min, dimensiona para clientela de fuera del barrio.",
      "Aliarse con hoteles cercanos para captar cliente de paso.",
    ],
    moda: [
      "Buena conectividad amplía la zona de captación a toda la ciudad — invierte en marketing online.",
    ],
    inmobiliario: [
      "El metro a < 5 min sube el precio del local en alquiler.",
    ],
  },
  seguridad: {
    restauracion: [
      "Cierre antes de las 23h reduce drásticamente el riesgo nocturno.",
      "Si hay actividad nocturna alta, contrata seguro RC ampliado y caja con doble llave.",
      "Coordina cierre con la ronda municipal si la franja 22-2h es complicada.",
    ],
    moda: [
      "Persiana metálica reforzada y alarma 24/7 son obligadas si las incidencias superan la media.",
    ],
    estetica: [
      "Negocio diurno → riesgo nocturno irrelevante.",
    ],
  },
  turismo: {
    restauracion: [
      "Alta dependencia turística → presupuesta caja para cubrir el bajón de invierno.",
      "Carta bilingüe y QR en mesa son básicos en zonas con > 20% turista.",
      "Si la zona es local, fideliza al residente con menú semanal.",
    ],
    moda: [
      "El turista suele buscar souvenir o producto local — diseña una cápsula específica.",
      "Producto premium funciona mejor en zonas hoteleras de 4-5★.",
    ],
    estetica: [
      "El turista no es tu público — el score turístico no debe penalizar la decisión.",
    ],
  },
  dinamismo: {
    restauracion: [
      "Barrio en alza → posibilidad de revisión al alza del alquiler en próximo contrato.",
      "Llegada de operadores referente reduce el riesgo de abrir en solitario.",
      "Tasa de supervivencia 3a por encima de la media baja la prima de riesgo.",
    ],
    moda: [
      "Aprovecha la llegada de marcas referente para co-marketing y ventas cruzadas.",
    ],
    estetica: [
      "Aperturas crecientes pueden saturar el subsector — adelanta tu apertura si la idea está clara.",
    ],
  },
};

const POR_DEFECTO: Partial<Record<DimensionKey, string[]>> = {
  flujo_peatonal: [
    "Adapta horarios y plantilla al pico real de la zona.",
    "Si el flujo es residente, prioriza fidelización; si es de paso, escaparate y conversión.",
  ],
  demografia: [
    "Asegúrate de que el ticket medio encaja con la renta de la zona.",
    "Comunica en el idioma dominante del barrio.",
  ],
  competencia: [
    "Diferenciación clara obligatoria si hay > 3 competidores directos.",
    "Si el mercado está fragmentado, aprovecha el hueco con propuesta nítida.",
  ],
  transporte: [
    "Comunica los accesos en tu Google Business Profile.",
    "La buena conectividad amplía tu zona de captación.",
  ],
  seguridad: [
    "Adapta el cierre al pico nocturno de incidencias.",
    "Considera seguro RC si la zona supera la media de incidencias.",
  ],
  turismo: [
    "Si dependes del turista, presupuesta caja para temporada baja.",
    "Si la clientela es local, fideliza con producto recurrente.",
  ],
  dinamismo: [
    "Un barrio en alza puede traducirse en revisión al alza del alquiler.",
    "La supervivencia a 3 años de la zona reduce el riesgo del proyecto.",
  ],
};

/**
 * Devuelve 2-3 implicaciones operativas para una dimensión y un sector dado.
 * Si no hay match exacto sector × dim, cae al texto por defecto de la dim.
 */
export function getImplicaciones(
  dim: DimensionKey,
  sector: string | null | undefined,
): string[] {
  if (sector) {
    const porSector = POR_SECTOR[dim];
    if (porSector && porSector[sector]) {
      return porSector[sector] as string[];
    }
  }
  return POR_DEFECTO[dim] ?? [];
}
