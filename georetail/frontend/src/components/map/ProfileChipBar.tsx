"use client";

import type { PerfilRefinado } from "@/lib/types";
import styles from "./ProfileChipBar.module.css";

interface Props {
  perfil: PerfilRefinado;
  sector?: string | null;
  subsector?: string | null;
}

/**
 * Chip bar — tira horizontal de chips que resume como el motor ha
 * entendido la idea del usuario. Se monta en el header del Dossier
 * inmediatamente despues de la linea barrio/distrito.
 *
 * Contenido (ocultos si vienen null/vacios):
 *  - sector / subsector (code style, mono)
 *  - top-3 nuances_detected (neutral)
 *  - ticket_tier como "€" x tier
 *  - publico_objetivo.edad_rango
 *  - horarios_pico unidos por "·"
 */
export default function ProfileChipBar({ perfil, sector, subsector }: Props) {
  const nuances = (perfil.nuances_detected ?? []).slice(0, 3);
  const ticketTier = perfil.operacion?.ticket_tier_p1_p5 ?? null;
  const edadRango = perfil.publico_objetivo?.edad_rango ?? null;
  const horariosPico = (perfil.publico_objetivo?.horarios_pico ?? []).filter(
    (h): h is string => typeof h === "string" && h.length > 0,
  );

  const hasAny =
    Boolean(sector) ||
    Boolean(subsector) ||
    nuances.length > 0 ||
    (ticketTier != null && ticketTier >= 1) ||
    Boolean(edadRango) ||
    horariosPico.length > 0;

  if (!hasAny) return null;

  const ticketStr =
    ticketTier != null && ticketTier >= 1 && ticketTier <= 5
      ? "€".repeat(Math.round(ticketTier))
      : null;

  return (
    <div className={styles.bar} role="list" aria-label="Perfil detectado">
      {sector && (
        <span
          role="listitem"
          className={`${styles.chip} ${styles.chipCode}`}
          title={`Sector detectado: ${sector}`}
        >
          {sector}
        </span>
      )}
      {subsector && (
        <span
          role="listitem"
          className={`${styles.chip} ${styles.chipCode}`}
          title={`Subsector detectado: ${subsector}`}
        >
          {subsector}
        </span>
      )}
      {nuances.map((n) => (
        <span
          key={`n-${n}`}
          role="listitem"
          className={styles.chip}
          title={`Matiz detectado: ${n}`}
        >
          {n}
        </span>
      ))}
      {ticketStr && (
        <span
          role="listitem"
          className={`${styles.chip} ${styles.chipTicket}`}
          title={`Ticket tier ${ticketTier} de 5`}
        >
          {ticketStr}
        </span>
      )}
      {edadRango && (
        <span
          role="listitem"
          className={styles.chip}
          title={`Publico objetivo: ${edadRango}`}
        >
          {edadRango}
        </span>
      )}
      {horariosPico.length > 0 && (
        <span
          role="listitem"
          className={styles.chip}
          title={`Horarios pico: ${horariosPico.join(", ")}`}
        >
          {horariosPico.join(" · ")}
        </span>
      )}
    </div>
  );
}
