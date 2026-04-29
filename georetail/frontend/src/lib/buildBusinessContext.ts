/**
 * buildBusinessContext.ts — Construye un contexto de negocio estructurado y enriquecido.
 *
 * Combina sector + subsector + descripción libre del usuario en un objeto
 * que puede alimentar el cuestionario adaptativo, el refinador y el modelo financiero.
 *
 * El campo `matices_enriquecidos` se envía como `PerfilEstructurado.matices` al backend,
 * donde el LLM lo usa para personalizar preguntas y análisis.
 */

import { getSectorDef, inferStreams, type StreamType } from "@/lib/sectorMap";

export interface BusinessContext {
  label:                string;        // "Restaurante japonés premium"
  sector_label:         string;        // "Hostelería"
  subsector_label:      string;        // "Restaurante"
  stream_types:         StreamType[];  // ["space", "traffic"] — modelo económico inferido
  matices_enriquecidos: string;        // string estructurado → PerfilEstructurado.matices
}

/**
 * Genera el contexto de negocio a partir de los datos del Onboarding.
 *
 * @param sectorCodigo      - Código interno del sector UI (ej: "hosteleria")
 * @param subsectorLabel    - Etiqueta del subsector seleccionado (ej: "Restaurante")
 * @param descripcionUsuario - Texto libre introducido por el usuario (puede estar vacío)
 */
export function buildBusinessContext(
  sectorCodigo: string,
  subsectorLabel: string,
  descripcionUsuario: string,
): BusinessContext {
  const sectorDef   = getSectorDef(sectorCodigo);
  const sectorLabel = sectorDef?.label ?? sectorCodigo;
  const streams     = inferStreams(sectorCodigo);
  const detalle     = descripcionUsuario.trim();

  // Partes del string estructurado para el backend
  const parts: string[] = [];
  parts.push(`Sector: ${sectorLabel}`);
  if (subsectorLabel) parts.push(`Subsector: ${subsectorLabel}`);
  if (detalle)        parts.push(`Detalle: ${detalle}`);

  // Label legible para mostrar en UI y en el wizard
  const label =
    [subsectorLabel, detalle].filter(Boolean).join(" — ") || sectorLabel;

  return {
    label,
    sector_label:         sectorLabel,
    subsector_label:      subsectorLabel,
    stream_types:         streams,
    matices_enriquecidos: parts.join(". "),
  };
}
