"use client";

import type React from "react";
import styles from "./PrecioAlquilerPanel.module.css";

export interface PrecioAlquilerPanelProps {
  /** Price per m² (commercial lease). */
  precio_m2?: number | null;
  /** BCN-wide average price per m² for reference bar. */
  precio_m2_bcn_media?: number | null;
  /** Minimum price per m² observed in the dataset for the range bar. */
  precio_m2_min?: number | null;
  /** Maximum price per m² observed in the dataset for the range bar. */
  precio_m2_max?: number | null;
  /** Monthly lease of the specific local (used as alternative hero when m² not available). */
  alquiler_mensual?: number | null;
  /** Dimension score 0-100. */
  score?: number | null;
  /** Narrative signal pills from the backend. */
  explicacion_bullets?: string[];
}

function formatEuroM2(valor: number): string {
  return (
    valor.toLocaleString("es-ES", { maximumFractionDigits: 0 }) + " €/m²"
  );
}

function formatEuroMes(valor: number): string {
  return valor.toLocaleString("es-ES", { maximumFractionDigits: 0 }) + " €/mes";
}

/** Higher price = worse affordability for the tenant. */
function affordabilityChip(
  precio_m2: number,
  bcn_media: number,
): { label: string; variant: "green" | "amber" | "red" } {
  const ratio = precio_m2 / bcn_media;
  if (ratio <= 0.85) return { label: "Precio asequible", variant: "green" };
  if (ratio <= 1.15) return { label: "Precio moderado", variant: "amber" };
  return { label: "Precio elevado", variant: "red" };
}

function Chip({
  label,
  variant,
}: {
  label: string;
  variant: "green" | "amber" | "red" | "neutral";
}) {
  const cls =
    variant === "green"
      ? styles.chipGreen
      : variant === "amber"
      ? styles.chipAmber
      : variant === "red"
      ? styles.chipRed
      : styles.chipNeutral;
  return <span className={`${styles.chip} ${cls}`}>{label}</span>;
}

export default function PrecioAlquilerPanel({
  precio_m2,
  precio_m2_bcn_media,
  precio_m2_min,
  precio_m2_max,
  alquiler_mensual,
  score,
  explicacion_bullets,
}: PrecioAlquilerPanelProps) {
  const hasPrecioM2 = precio_m2 != null;
  const hasAlquiler = alquiler_mensual != null;
  const hasScore = score != null;
  const hasBullets = Array.isArray(explicacion_bullets) && explicacion_bullets.length > 0;

  if (!hasPrecioM2 && !hasAlquiler && !hasScore && !hasBullets) {
    return (
      <div className={styles.panel}>
        <div className={styles.emptyState}>
          Sin datos de precio de alquiler disponibles para esta zona.
        </div>
      </div>
    );
  }

  // BCN defaults when not provided explicitly.
  const bcnMedia = precio_m2_bcn_media ?? 18;
  const bcnMin = precio_m2_min ?? 10;
  const bcnMax = precio_m2_max ?? 30;

  // Hero bar: position of current price on a scale from min to max.
  let heroPct = 50;
  let rangePosPct = 50;
  if (hasPrecioM2 && precio_m2 != null) {
    const range = bcnMax - bcnMin || 1;
    heroPct = Math.min(100, Math.max(0, Math.round(((precio_m2 - bcnMin) / range) * 100)));
    rangePosPct = heroPct;
  }
  const bcnMediaPosPct = Math.min(
    100,
    Math.max(0, Math.round(((bcnMedia - bcnMin) / (bcnMax - bcnMin || 1)) * 100)),
  );

  const chip =
    hasPrecioM2 && precio_m2 != null
      ? affordabilityChip(precio_m2, bcnMedia)
      : null;

  // Score gauge: conic-gradient percentage (0-100).
  const scorePct = hasScore && score != null ? Math.min(100, Math.max(0, score)) : 0;

  return (
    <div className={styles.panel}>

      {/* ── Price hero ── */}
      {(hasPrecioM2 || hasAlquiler) && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>
            {hasPrecioM2 ? "Precio de alquiler comercial" : "Alquiler mensual estimado"}
          </p>

          <div className={styles.heroNum}>
            {hasPrecioM2 && precio_m2 != null
              ? formatEuroM2(precio_m2)
              : alquiler_mensual != null
              ? formatEuroMes(alquiler_mensual)
              : "—"}
          </div>

          {/* Gradient range bar: red (low=cheap) → green (high=expensive) reversed
              so low price shows green fill, high price shows red fill. */}
          {hasPrecioM2 && precio_m2 != null && (
            <div className={styles.rangeWrap}>
              {/* Gradient track */}
              <div className={styles.rangeTrack}>
                <div className={styles.rangeGradient} />
                {/* BCN media marker */}
                <div
                  className={styles.rangeMediaMarker}
                  style={{ left: `${bcnMediaPosPct}%` }}
                />
                {/* Current price dot */}
                <div
                  className={styles.rangeDot}
                  style={{ left: `${rangePosPct}%` }}
                />
              </div>
              <div className={styles.rangeLabels}>
                <span className={styles.rangeLabelLeft}>{formatEuroM2(bcnMin)} mín</span>
                <span
                  className={styles.rangeLabelCenter}
                  style={{ left: `${bcnMediaPosPct}%` }}
                >
                  BCN media
                </span>
                <span className={styles.rangeLabelRight}>{formatEuroM2(bcnMax)} máx</span>
              </div>
            </div>
          )}

          {chip && <Chip label={chip.label} variant={chip.variant} />}
        </section>
      )}

      {/* ── Score gauge + affordability chip row ── */}
      {hasScore && score != null && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>Score de la dimensión</p>
          <div className={styles.gaugeRow}>
            <div className={styles.gaugeWrap}>
              <div
                className={styles.gaugeRing}
                style={{ "--pct": scorePct } as React.CSSProperties}
              >
                <div className={styles.gaugeInner}>
                  <span className={styles.gaugeValue}>{Math.round(score)}</span>
                  <span className={styles.gaugeMax}>/100</span>
                </div>
              </div>
            </div>
            <div className={styles.gaugeContext}>
              <p className={styles.gaugeDesc}>
                {scorePct >= 70
                  ? "El precio del alquiler es competitivo para este mercado — buen punto de partida financiero."
                  : scorePct >= 45
                  ? "Precio en rango moderado — el punto muerto es alcanzable con tráfico sostenido."
                  : "Precio elevado para la zona — el alquiler compromete el margen. Analiza bien el P&L."}
              </p>
              <div className={styles.gaugeChips}>
                <Chip
                  label={
                    scorePct >= 70
                      ? "Favorable"
                      : scorePct >= 45
                      ? "Moderado"
                      : "Restrictivo"
                  }
                  variant={scorePct >= 70 ? "green" : scorePct >= 45 ? "amber" : "red"}
                />
              </div>
            </div>
          </div>
        </section>
      )}

      {/* ── Signal pills ── */}
      {hasBullets && explicacion_bullets && (
        <section className={styles.section}>
          <p className={styles.sectionTitle}>Señales detectadas</p>
          <div className={styles.signalTags}>
            {explicacion_bullets.map((bullet, i) => (
              <span key={i} className={styles.signalTag}>
                {bullet}
              </span>
            ))}
          </div>
        </section>
      )}
    </div>
  );
}
